"""Profile loading, JSON Schema validation and merge order (docs/03 §3; docs/11 WP3).

Two layers of validation, deliberately separate: `profiles/_schema.json` checks *structure*
(types, required keys, enums) via `jsonschema`; the `_resolve_*` helpers below check *content*
against the generated `sysap.codes` tables and the codec registry (a name that is well-formed but
does not exist, e.g. a typo'd pairing). Both are profile-authoring trust boundaries (CLAUDE.md
rule 5): a bad profile fails loudly here, at load time, never silently at compile or run time.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator, Mapping, Sequence
from dataclasses import dataclass
from functools import lru_cache
from itertools import chain
from pathlib import Path
from typing import Any, Literal

import jsonschema
import yaml

from freeathome2mqtt.model.codecs import KNOWN_CODECS
from freeathome2mqtt.model.entity import AttributeSpec, AttrKind, CommandSpec
from freeathome2mqtt.sysap.codes import Function, Pairing, Parameter

logger = logging.getLogger(__name__)

_SCHEMA_PATH = Path(__file__).resolve().parent.parent / "profiles" / "_schema.json"
_ATTR_KIND_BY_NAME: Mapping[str, AttrKind] = {"state": AttrKind.STATE, "event": AttrKind.EVENT}
_ATTRIBUTE_FIXED_KEYS = frozenset(
    {"pairing", "codec", "kind", "unit", "values", "default", "entity_category", "precision"}
)
_COMMAND_FIXED_KEYS = frozenset(
    {"pairing", "codec", "values", "range", "continuous", "optimistic", "confirm"}
)


class ProfileError(Exception):
    """A profile file is missing, malformed, fails schema validation, or references an unknown
    code (docs/03 §3): a profile-authoring bug, caught at load time (CLAUDE.md rule 5).
    """


@dataclass(frozen=True, slots=True)
class Profile:
    """One channel type, loaded and validated (docs/03 §3)."""

    id: str
    description: str | None
    functions: tuple[Function, ...]
    requires_outputs: tuple[Pairing, ...]
    requires_inputs: tuple[Pairing, ...]
    attributes: Mapping[str, AttributeSpec]
    commands: Mapping[str, CommandSpec]
    parameters: Mapping[str, Parameter]
    homeassistant: Mapping[str, Any]
    transform: str | None
    availability: Literal["device", "none"]


class ProfileRegistry:
    """All loaded, validated profiles, indexed for compile-time lookup (docs/02 §1)."""

    def __init__(self, profiles: Mapping[str, Profile]) -> None:
        self._profiles = dict(profiles)
        by_function: dict[Function, list[Profile]] = {}
        for profile in sorted(self._profiles.values(), key=lambda p: p.id):
            for function in profile.functions:
                by_function.setdefault(function, []).append(profile)
        self._by_function = {function: tuple(ps) for function, ps in by_function.items()}

    def __getitem__(self, profile_id: str) -> Profile:
        return self._profiles[profile_id]

    def __iter__(self) -> Iterator[Profile]:
        return iter(self._profiles.values())

    def __len__(self) -> int:
        return len(self._profiles)

    def for_function(self, function: Function) -> tuple[Profile, ...]:
        """Every profile claiming `function`, in deterministic `id` order (docs/03 §3.4)."""
        return self._by_function.get(function, ())


def _resolve_function(name: str, *, source: str) -> Function:
    try:
        return Function[name]
    except KeyError as exc:
        raise ProfileError(f"{source}: unknown function {name!r}") from exc


def _resolve_pairing(name: str, *, source: str) -> Pairing:
    try:
        return Pairing[name]
    except KeyError as exc:
        raise ProfileError(f"{source}: unknown pairing {name!r}") from exc


def _resolve_parameter(name: str, *, source: str) -> Parameter:
    try:
        return Parameter[name]
    except KeyError as exc:
        raise ProfileError(f"{source}: unknown parameter {name!r}") from exc


def _resolve_codec(name: str, *, source: str) -> str:
    if name not in KNOWN_CODECS:
        raise ProfileError(f"{source}: unknown codec {name!r}")
    return name


def _resolve_kind(raw: str, *, source: str) -> AttrKind:
    try:
        return _ATTR_KIND_BY_NAME[raw]
    except KeyError as exc:
        raise ProfileError(f"{source}: unknown attribute kind {raw!r}") from exc


def _build_attribute_spec(name: str, raw: Mapping[str, Any], *, source: str) -> AttributeSpec:
    codec_params = {k: v for k, v in raw.items() if k not in _ATTRIBUTE_FIXED_KEYS}
    return AttributeSpec(
        name=name,
        pairing=_resolve_pairing(raw["pairing"], source=source),
        codec=_resolve_codec(raw["codec"], source=source),
        kind=_resolve_kind(raw.get("kind", "state"), source=source),
        unit=raw.get("unit"),
        values=raw.get("values"),
        default=raw.get("default"),
        entity_category=raw.get("entity_category"),
        precision=raw.get("precision"),
        codec_params=codec_params,
    )


def _build_command_spec(name: str, raw: Mapping[str, Any], *, source: str) -> CommandSpec:
    codec_params = {k: v for k, v in raw.items() if k not in _COMMAND_FIXED_KEYS}
    range_raw = raw.get("range")
    return CommandSpec(
        name=name,
        pairing=_resolve_pairing(raw["pairing"], source=source),
        codec=_resolve_codec(raw["codec"], source=source),
        values=raw.get("values"),
        range=(float(range_raw[0]), float(range_raw[1])) if range_raw is not None else None,
        continuous=raw.get("continuous", False),
        optimistic=raw.get("optimistic"),
        confirm=raw.get("confirm", True),
        codec_params=codec_params,
    )


@lru_cache(maxsize=1)
def _load_validator() -> jsonschema.protocols.Validator:
    # The schema is fixed, packaged content -- cached so a full profile directory (many files,
    # many profiles) parses it once rather than once per profile object.
    schema = json.loads(_SCHEMA_PATH.read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator.check_schema(schema)
    return jsonschema.Draft202012Validator(schema)


def parse_profile(raw: Mapping[str, Any], *, source: str) -> Profile:
    """Validate one already-parsed profile object and convert it to a `Profile`. Pure -- no I/O."""
    validator = _load_validator()
    errors = sorted(validator.iter_errors(raw), key=lambda e: [str(p) for p in e.path])
    if errors:
        profile_id = raw.get("id", "?")
        raise ProfileError(
            f"{source}: profile {profile_id!r} failed schema validation: {errors[0].message}"
        )

    functions = tuple(_resolve_function(name, source=source) for name in raw["functions"])
    requires = raw.get("requires", {})
    requires_outputs = tuple(
        _resolve_pairing(name, source=source) for name in requires.get("outputs", [])
    )
    requires_inputs = tuple(
        _resolve_pairing(name, source=source) for name in requires.get("inputs", [])
    )
    attributes = {
        name: _build_attribute_spec(name, attr_raw, source=source)
        for name, attr_raw in raw["attributes"].items()
    }
    commands = {
        name: _build_command_spec(name, cmd_raw, source=source)
        for name, cmd_raw in raw.get("commands", {}).items()
    }
    parameters = {
        name: _resolve_parameter(param_name, source=source)
        for name, param_name in raw.get("parameters", {}).items()
    }

    return Profile(
        id=raw["id"],
        description=raw.get("description"),
        functions=functions,
        requires_outputs=requires_outputs,
        requires_inputs=requires_inputs,
        attributes=attributes,
        commands=commands,
        parameters=parameters,
        homeassistant=raw.get("homeassistant", {}),
        transform=raw.get("transform"),
        availability=raw.get("availability", "device"),
    )


def load_profile_file(path: Path) -> list[Profile]:
    """Read and parse one YAML file of profiles (I/O), in file order."""
    text = path.read_text(encoding="utf-8")
    try:
        raw_items = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ProfileError(f"{path}: invalid YAML: {exc}") from exc
    if not isinstance(raw_items, list):
        got = type(raw_items).__name__
        raise ProfileError(f"{path}: expected a YAML list of profiles, got {got}")
    return [parse_profile(item, source=str(path)) for item in raw_items]


def load_profile_directory(directory: Path) -> list[Profile]:
    """Load every `*.yaml`/`*.yml` file in `directory` (I/O; non-recursive), sorted by filename
    so the result never depends on directory-listing order (P-54's determinism concern applies
    just as much to profile load order as to entity naming).
    """
    paths = sorted(chain(directory.glob("*.yaml"), directory.glob("*.yml")))
    profiles: list[Profile] = []
    for path in paths:
        profiles.extend(load_profile_file(path))
    return profiles


def build_registry(
    built_in: Sequence[Profile], overrides: Sequence[Profile] = ()
) -> ProfileRegistry:
    """Merge `overrides` over `built_in`, keyed by `id` (docs/03 §3). A same-tier duplicate `id`
    is a `ProfileError` -- almost certainly a copy-paste bug, not an intentional override.
    """
    merged: dict[str, Profile] = {}
    for profile in built_in:
        if profile.id in merged:
            raise ProfileError(f"duplicate built-in profile id: {profile.id!r}")
        merged[profile.id] = profile

    seen_override_ids: set[str] = set()
    for profile in overrides:
        if profile.id in seen_override_ids:
            raise ProfileError(f"duplicate user profile id: {profile.id!r}")
        seen_override_ids.add(profile.id)
        if profile.id in merged:
            logger.info("profile %r overridden by a user profile", profile.id)
        merged[profile.id] = profile

    return ProfileRegistry(merged)


def load_profile_registry(built_in_dir: Path, user_dir: Path | None = None) -> ProfileRegistry:
    """The full loader (ADR-011's 'out-of-tree profiles'): built-ins from `built_in_dir`, merged
    with any user profiles from `user_dir`. A missing `built_in_dir` is a packaging bug and raises;
    a missing `user_dir` just means no overrides -- it is optional by design.
    """
    if not built_in_dir.is_dir():
        raise ProfileError(f"built-in profile directory not found: {built_in_dir}")
    built_in = load_profile_directory(built_in_dir)
    overrides = (
        load_profile_directory(user_dir) if user_dir is not None and user_dir.is_dir() else []
    )
    return build_registry(built_in, overrides)
