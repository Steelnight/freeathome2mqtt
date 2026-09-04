"""The pure `compile()`: config JSON + profiles -> flat, compiled tables (docs/03 §4; docs/11 WP3).

No I/O, no network, no clock (docs/02 §2) -- `compile()` takes an already-fetched, already-unwrapped
`Configuration` (the SysAP-UUID unwrap happens once in `sysap.rest.RestClient`, not here) and an
already-loaded `ProfileRegistry`, and produces a `Model` deterministically. Same input ->
byte-identical output, including ordering: devices and channels are walked in sorted serial/id
order regardless of the input dict's own iteration order, so a restart never reshuffles topics
(P-39, P-54).
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from itertools import chain
from typing import Any, Literal

from freeathome2mqtt.model.codecs import Codec, build_codec
from freeathome2mqtt.model.entity import (
    AttributeSpec,
    Binding,
    CommandSpec,
    EgressBinding,
    Entity,
)
from freeathome2mqtt.model.naming import SlugCandidate, resolve_slugs, slugify
from freeathome2mqtt.model.profiles import Profile, ProfileRegistry
from freeathome2mqtt.mqtt.topics import (
    entity_availability_topic,
    entity_get_topic,
    entity_set_topic,
    entity_state_topic,
)
from freeathome2mqtt.sysap.codes import Function, Pairing, Parameter
from freeathome2mqtt.sysap.schema import Channel, Configuration, Device, InOutPut, parse_function_id

logger = logging.getLogger(__name__)

_VIRTUAL_DEVICE_SERIAL_PREFIX = "6000"
_CIRCLED_ALPHANUMERIC_MIN = 0x2460
_CIRCLED_ALPHANUMERIC_MAX = 0x24FF
_PRECISION_AWARE_CODECS = frozenset({"float", "scaled"})
# docs/03 §5, P-09: color_temp_pct is bound per entity from the CHANNEL's own physical bounds, not
# the profile's codec_params -- a profile using it must name these two entries in `parameters:`.
_COLOR_TEMP_WARMEST_PARAM_NAME = "color_temp_warmest"
_COLOR_TEMP_COOLEST_PARAM_NAME = "color_temp_coolest"


class CompileError(Exception):
    """A profile is internally inconsistent in a way only visible once compiled against real
    channel data (docs/03 §4): a P-01/P-02 input/output miswiring, or an internal invariant a
    correctly-matched profile should never violate. A profile-authoring bug, not a
    per-installation quirk -- it is not channel-specific, so it fails the same way every time.
    """


@dataclass(frozen=True, slots=True)
class CompileOptions:
    """Everything about compilation that is a *choice* rather than a fact from the config
    (docs/03 §1.1, §4; docs/01 §4.2, §4.4).
    """

    topic_prefix: str = "freeathome2mqtt"
    topic_style: Literal["slug", "id"] = "slug"
    include_orphan_channels: bool = False
    include_virtual_devices: bool = False
    excluded_interfaces: frozenset[str] = frozenset({"hue", "sonos"})
    aliases: Mapping[str, str] = field(default_factory=dict)
    # Populated from `EntitiesStore` options (docs/07 §4.1), symmetric with `aliases` above --
    # `bridge/request/entity/remove` and `entity/options {"enabled": false}` (docs/04 §5) both
    # persist here rather than mutating the model directly, so the entity simply doesn't exist
    # in the next `Model` and the existing removed-entity retraction path (P-35) does the rest.
    excluded_entity_ids: frozenset[str] = frozenset()


@dataclass(frozen=True, slots=True)
class CompileStats:
    """Counts of what compilation excluded and why (docs/03 §3.4, §3.5; P-18) -- surfaced in
    `bridge/info` so exclusions are visible rather than mysterious.
    """

    devices_total: int
    devices_excluded_by_interface: int
    channels_total: int
    channels_orphaned: int
    channels_unknown_function: int
    channels_unsupported: int
    channels_ambiguous_profile: int
    entities_created: int
    channels_excluded_by_option: int = 0


@dataclass(frozen=True, slots=True)
class Model:
    """The compiled installation: flat lookup tables only, nothing that needs a dict-of-dicts
    walk on the hot path (docs/03 §2, §4 step 9).
    """

    entities: tuple[Entity, ...]
    ingress: Mapping[str, Binding]
    egress: Mapping[tuple[int, str], EgressBinding]
    by_id: Mapping[str, int]
    by_topic: Mapping[str, int]
    discovery: tuple[tuple[str, bytes], ...]
    initial_values: tuple[tuple[Any, ...], ...]
    stats: CompileStats


@dataclass(frozen=True, slots=True)
class _CompiledChannel:
    """One channel that survived filtering and profile matching -- not yet an `Entity` because
    naming/slug collisions (docs/03 §1.1) must be resolved globally, across every channel, before
    any one of them can be assigned a final topic segment.
    """

    entity_id: str
    device_serial: str
    channel_id: str
    channel: Channel
    device: Device
    profile: Profile
    name: str
    area: str | None


@dataclass(slots=True)
class _Tally:
    devices_total: int = 0
    devices_excluded_by_interface: int = 0
    channels_total: int = 0
    channels_orphaned: int = 0
    channels_unknown_function: int = 0
    channels_unsupported: int = 0
    channels_ambiguous_profile: int = 0
    entities_created: int = 0
    channels_excluded_by_option: int = 0


@dataclass(slots=True)
class _CompileContext:
    """Everything constant across one `compile()` call, bundled to keep helper signatures small.
    Scoped to a single call -- never a module-level global (CLAUDE.md rule 6).
    """

    floorplan: Mapping[str, Mapping[str, str]]
    registry: ProfileRegistry
    options: CompileOptions
    tally: _Tally
    seen_unknown_function_ids: set[str | None]


@dataclass(slots=True)
class _AttributeBuildResult:
    names: tuple[str, ...]
    kinds: tuple[int, ...]
    bindings: dict[str, Binding]
    initial_values: tuple[Any, ...]


def _identity(value: Any) -> Any:
    return value


# --------------------------------------------------------------------------- floorplan (§4.4, P-14)


def _resolve_floorplan(config: Configuration) -> dict[str, dict[str, str]]:
    floors = config.get("floorplan", {}).get("floors", {})
    result: dict[str, dict[str, str]] = {}
    for floor_id, floor in floors.items():
        rooms = floor.get("rooms") or {}  # P-14: rooms may be null, not just absent
        result[floor_id] = {room_id: room.get("name", "") for room_id, room in rooms.items()}
    return result


def _resolve_area(
    floorplan: Mapping[str, Mapping[str, str]], floor_id: str, room_id: str
) -> str | None:
    return floorplan.get(floor_id, {}).get(room_id)


def _channel_floor_room(channel: Channel, device: Device) -> tuple[str | None, str | None]:
    floor_id = channel.get("floor") or device.get("floor")
    room_id = channel.get("room") or device.get("room")
    return floor_id, room_id


# -------------------------------------------------------------------- device filtering (§4.2, §4.5)


def _is_virtual_device(serial: str, device: Device) -> bool:
    return serial.startswith(_VIRTUAL_DEVICE_SERIAL_PREFIX) or device.get("interface") == "VD"


def _device_included(serial: str, device: Device, options: CompileOptions) -> bool:
    if _is_virtual_device(serial, device) and not options.include_virtual_devices:
        return False
    interface = device.get("interface")
    return interface is None or interface not in options.excluded_interfaces


# ----------------------------------------------------------------------- channel names (§6.5, P-15)


def _is_placeholder_name(name: str) -> bool:
    stripped = name.strip()
    if not stripped:
        return True
    return all(_CIRCLED_ALPHANUMERIC_MIN <= ord(ch) <= _CIRCLED_ALPHANUMERIC_MAX for ch in stripped)


def _resolve_channel_name(channel: Channel, channel_id: str, device: Device) -> str:
    raw = channel.get("displayName")
    if raw and not _is_placeholder_name(raw):
        return raw
    device_name = device.get("displayName")
    if device_name:
        return device_name
    return f"Channel {channel_id}"


# ------------------------------------------------------------------------- function ID (§4.3, P-06)


def _resolve_channel_function(channel: Channel) -> Function | None:
    parsed = parse_function_id(channel.get("functionID"))
    if parsed is None:
        return None
    try:
        return Function(parsed)
    except ValueError:
        return None


def _log_unknown_function_once(
    raw: str | None, device_serial: str, device: Device, ctx: _CompileContext
) -> None:
    if raw in ctx.seen_unknown_function_ids:
        return
    ctx.seen_unknown_function_ids.add(raw)
    logger.info(
        "unsupported function ID %r on device %s (article %s)",
        raw,
        device_serial,
        device.get("articleNumber", "?"),
    )


# -------------------------------------------------------------------------- profile matching (§3.4)


def _find_datapoint_key(datapoints: Mapping[str, InOutPut], pairing: Pairing) -> str | None:
    for key in sorted(datapoints):
        if datapoints[key].get("pairingID") == pairing.value:
            return key
    return None


def _find_parameter_value(parameters: Mapping[str, str], parameter: Parameter) -> str | None:
    # Keys are "parNNNN" (docs/01 §4.3), but matched by parsed hex value rather than an exact
    # "par" + zero-padded-hex string, so this tolerates whatever case/width a real SysAP sends.
    for key in sorted(parameters):
        suffix = key.removeprefix("par").removeprefix("PAR")
        try:
            parsed = int(suffix, 16)
        except ValueError:
            continue
        if parsed == parameter.value:
            return parameters[key]
    return None


def _has_all_pairings(datapoints: Mapping[str, InOutPut], pairings: tuple[Pairing, ...]) -> bool:
    present = {dp.get("pairingID") for dp in datapoints.values()}
    return all(pairing.value in present for pairing in pairings)


def _profile_matches(channel: Channel, profile: Profile) -> bool:
    return _has_all_pairings(
        channel.get("outputs", {}), profile.requires_outputs
    ) and _has_all_pairings(channel.get("inputs", {}), profile.requires_inputs)


def _specificity(profile: Profile) -> int:
    return len(profile.requires_outputs) + len(profile.requires_inputs)


def _match_profile(channel: Channel, function: Function, ctx: _CompileContext) -> Profile | None:
    candidates = [p for p in ctx.registry.for_function(function) if _profile_matches(channel, p)]
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]

    candidates.sort(key=lambda p: (-_specificity(p), p.id))
    if _specificity(candidates[0]) == _specificity(candidates[1]):
        ctx.tally.channels_ambiguous_profile += 1
        logger.warning(
            "ambiguous profile match for function %s: %s (using %s)",
            function.name,
            [p.id for p in candidates],
            candidates[0].id,
        )
    return candidates[0]


# ------------------------------------------------------------------------- pass 1: collect channels


def _collect_one_channel(
    *, device_serial: str, channel_id: str, channel: Channel, device: Device, ctx: _CompileContext
) -> _CompiledChannel | None:
    ctx.tally.channels_total += 1

    entity_id = f"{device_serial}_{channel_id}"
    if entity_id in ctx.options.excluded_entity_ids:
        ctx.tally.channels_excluded_by_option += 1
        return None

    floor_id, room_id = _channel_floor_room(channel, device)
    if floor_id is None or room_id is None:
        if not ctx.options.include_orphan_channels:
            ctx.tally.channels_orphaned += 1
            return None
        area = None
    else:
        area = _resolve_area(ctx.floorplan, floor_id, room_id)

    function = _resolve_channel_function(channel)
    if function is None:
        ctx.tally.channels_unknown_function += 1
        _log_unknown_function_once(channel.get("functionID"), device_serial, device, ctx)
        return None

    profile = _match_profile(channel, function, ctx)
    if profile is None:
        ctx.tally.channels_unsupported += 1
        return None

    return _CompiledChannel(
        entity_id=entity_id,
        device_serial=device_serial,
        channel_id=channel_id,
        channel=channel,
        device=device,
        profile=profile,
        name=_resolve_channel_name(channel, channel_id, device),
        area=area,
    )


def _collect_device_channels(
    device_serial: str, device: Device, ctx: _CompileContext
) -> list[_CompiledChannel]:
    channels = device.get("channels", {})
    result: list[_CompiledChannel] = []
    for channel_id in sorted(channels):
        compiled = _collect_one_channel(
            device_serial=device_serial,
            channel_id=channel_id,
            channel=channels[channel_id],
            device=device,
            ctx=ctx,
        )
        if compiled is not None:
            result.append(compiled)
    return result


def _collect_channels(config: Configuration, ctx: _CompileContext) -> list[_CompiledChannel]:
    devices = config.get("devices", {})
    compiled: list[_CompiledChannel] = []
    for device_serial in sorted(devices):
        device = devices[device_serial]
        ctx.tally.devices_total += 1
        if not _device_included(device_serial, device, ctx.options):
            ctx.tally.devices_excluded_by_interface += 1
            continue
        compiled.extend(_collect_device_channels(device_serial, device, ctx))
    return compiled


# ------------------------------------------------------------------------ naming (§1.1, P-39, P-40)


def _resolve_names_and_slugs(
    compiled: Sequence[_CompiledChannel], aliases: Mapping[str, str]
) -> dict[str, str]:
    candidates = [
        SlugCandidate(
            entity_id=c.entity_id,
            name=aliases.get(c.entity_id, c.name),
            area=c.area,
            channel_id=c.channel_id,
        )
        for c in compiled
    ]
    return resolve_slugs(candidates)


# ---------------------------------------------------------------------------- pass 2: codecs (§5)


def _resolve_color_temp_bounds(compiled: _CompiledChannel) -> tuple[float, float] | None:
    """Read the channel's own physical warmest/coolest bounds for `color_temp_pct` (P-09) --
    never hardcode 2700-6500K. A profile that forgot to name the two parameter entries is a static
    authoring bug (every channel using it would fail identically) and raises; a specific channel
    that just doesn't have the values set is not -- `None` lets the caller drop that one attribute
    or command, the same policy as any other absent, non-required datapoint.
    """
    warmest_param = compiled.profile.parameters.get(_COLOR_TEMP_WARMEST_PARAM_NAME)
    coolest_param = compiled.profile.parameters.get(_COLOR_TEMP_COOLEST_PARAM_NAME)
    if warmest_param is None or coolest_param is None:
        raise CompileError(
            f"{compiled.profile.id}: codec color_temp_pct needs '{_COLOR_TEMP_WARMEST_PARAM_NAME}' "
            f"and '{_COLOR_TEMP_COOLEST_PARAM_NAME}' entries in the profile's parameters: section"
        )
    parameters = compiled.channel.get("parameters", {})
    warmest_raw = _find_parameter_value(parameters, warmest_param)
    coolest_raw = _find_parameter_value(parameters, coolest_param)
    if warmest_raw is None or coolest_raw is None:
        return None
    return float(warmest_raw), float(coolest_raw)


def _build_attribute_codec(spec: AttributeSpec, compiled: _CompiledChannel) -> Codec | None:
    if spec.codec == "enum":
        return build_codec(
            "enum", decode_values=spec.values or {}, encode_values={}, default=spec.default
        )
    if spec.codec == "color_temp_pct":
        bounds = _resolve_color_temp_bounds(compiled)
        if bounds is None:
            return None
        return build_codec("color_temp_pct", warmest_kelvin=bounds[0], coolest_kelvin=bounds[1])
    params = dict(spec.codec_params)
    if spec.precision is not None and spec.codec in _PRECISION_AWARE_CODECS:
        params["precision"] = spec.precision
    return build_codec(spec.codec, **params)


def _build_command_codec(spec: CommandSpec, compiled: _CompiledChannel) -> Codec | None:
    if spec.codec == "enum":
        return build_codec("enum", decode_values={}, encode_values=spec.values or {})
    if spec.codec == "color_temp_pct":
        bounds = _resolve_color_temp_bounds(compiled)
        if bounds is None:
            return None
        return build_codec("color_temp_pct", warmest_kelvin=bounds[0], coolest_kelvin=bounds[1])
    return build_codec(spec.codec, **spec.codec_params)


def _make_range_validator(value_range: tuple[float, float] | None) -> Callable[[Any], Any]:
    if value_range is None:
        return _identity
    low, high = value_range

    def validate(value: Any) -> Any:
        return max(low, min(high, value))

    return validate


# ------------------------------------------------------- pass 2: attributes/commands (§4 steps 5-7)


def _check_not_required(
    compiled: _CompiledChannel, pairing: Pairing, required: tuple[Pairing, ...], name: str
) -> None:
    if pairing in required:
        raise CompileError(
            f"{compiled.profile.id}: {name!r} pairing {pairing.name} is required but absent "
            f"on {compiled.entity_id} (internal invariant violated -- the profile should not "
            "have matched)"
        )


def _resolve_attribute_datapoint(
    channel: Channel, spec: AttributeSpec, *, profile_id: str
) -> str | None:
    output_key = _find_datapoint_key(channel.get("outputs", {}), spec.pairing)
    if output_key is not None:
        return output_key
    if _find_datapoint_key(channel.get("inputs", {}), spec.pairing) is not None:
        raise CompileError(
            f"{profile_id}: attribute pairing {spec.pairing.name} is bound to an input datapoint, "
            "not an output (P-01) -- attributes read from outputs only"
        )
    return None


def _resolve_command_datapoint(
    channel: Channel, spec: CommandSpec, *, profile_id: str
) -> str | None:
    input_key = _find_datapoint_key(channel.get("inputs", {}), spec.pairing)
    if input_key is not None:
        return input_key
    if _find_datapoint_key(channel.get("outputs", {}), spec.pairing) is not None:
        raise CompileError(
            f"{profile_id}: command pairing {spec.pairing.name} is bound to an output datapoint, "
            "not an input (P-02) -- commands write to inputs only"
        )
    return None


def _build_attributes(compiled: _CompiledChannel, entity_idx: int) -> _AttributeBuildResult:
    names: list[str] = []
    kinds: list[int] = []
    bindings: dict[str, Binding] = {}
    initial_values: list[Any] = []
    outputs = compiled.channel.get("outputs", {})

    for attr_name, spec in compiled.profile.attributes.items():
        key = _resolve_attribute_datapoint(compiled.channel, spec, profile_id=compiled.profile.id)
        if key is None:
            _check_not_required(
                compiled, spec.pairing, compiled.profile.requires_outputs, attr_name
            )
            continue

        codec = _build_attribute_codec(spec, compiled)
        if codec is None:
            logger.warning(
                "%s: attribute %r (%s) dropped -- codec parameters unavailable on this channel",
                compiled.entity_id,
                attr_name,
                spec.codec,
            )
            continue

        attr_idx = len(names)
        names.append(attr_name)
        kinds.append(int(spec.kind))
        bindings[f"{compiled.device_serial}/{compiled.channel_id}/{key}"] = Binding(
            entity_idx=entity_idx,
            attr_idx=attr_idx,
            decode=codec.decode,
            kind=int(spec.kind),
            attr_bit=1 << attr_idx,
        )
        initial_values.append(codec.decode(outputs[key].get("value", "")))

    return _AttributeBuildResult(tuple(names), tuple(kinds), bindings, tuple(initial_values))


def _build_commands(
    compiled: _CompiledChannel, entity_idx: int, attr_index_by_name: Mapping[str, int]
) -> dict[tuple[int, str], EgressBinding]:
    egress: dict[tuple[int, str], EgressBinding] = {}

    for cmd_name, spec in compiled.profile.commands.items():
        key = _resolve_command_datapoint(compiled.channel, spec, profile_id=compiled.profile.id)
        if key is None:
            _check_not_required(compiled, spec.pairing, compiled.profile.requires_inputs, cmd_name)
            continue

        codec = _build_command_codec(spec, compiled)
        if codec is None:
            logger.warning(
                "%s: command %r (%s) dropped -- codec parameters unavailable on this channel",
                compiled.entity_id,
                cmd_name,
                spec.codec,
            )
            continue

        # A typo'd `optimistic:` is a minor, non-critical loss (no instant UI feedback for that
        # command) rather than a P-01/P-02-grade correctness bug, so it degrades to None instead
        # of raising.
        optimistic_attr = attr_index_by_name.get(spec.optimistic) if spec.optimistic else None
        egress[(entity_idx, cmd_name)] = EgressBinding(
            entity_idx=entity_idx,
            rest_path=f"{compiled.device_serial}.{compiled.channel_id}.{key}",
            encode=codec.encode,
            continuous=spec.continuous,
            optimistic_attr=optimistic_attr,
            validate=_make_range_validator(spec.range),
            confirm=spec.confirm,
        )

    return egress


def _build_entity(
    idx: int, compiled: _CompiledChannel, slug: str, options: CompileOptions
) -> tuple[Entity, dict[str, Binding], dict[tuple[int, str], EgressBinding], tuple[Any, ...]]:
    attrs = _build_attributes(compiled, idx)
    attr_index_by_name = {name: i for i, name in enumerate(attrs.names)}
    egress = _build_commands(compiled, idx, attr_index_by_name)

    topic_slug = slugify(compiled.entity_id) if options.topic_style == "id" else slug
    state_topic = entity_state_topic(options.topic_prefix, topic_slug)
    has_optimistic_command = any(
        cmd.optimistic is not None for cmd in compiled.profile.commands.values()
    )
    # An attribute's `entity_category` (docs/03 §3.2) is only unambiguously the whole entity's
    # HA `entity_category` when the entity has exactly one attribute -- HA's entity_category is
    # per-discovery-payload, and this codebase publishes one payload per entity (ADR-009), not
    # per attribute. A multi-attribute entity (e.g. room_temperature_controller, dimming_actuator)
    # may mark some of its attributes diagnostic without that making the whole entity diagnostic.
    entity_category = (
        next(iter(compiled.profile.attributes.values())).entity_category
        if len(compiled.profile.attributes) == 1
        else None
    )

    entity = Entity(
        idx=idx,
        id=compiled.entity_id,
        profile=compiled.profile.id,
        name=compiled.name,
        area=compiled.area,
        device_serial=compiled.device_serial,
        channel_id=compiled.channel_id,
        attr_names=attrs.names,
        attr_kinds=attrs.kinds,
        state_topic=state_topic,
        set_topic=entity_set_topic(state_topic),
        get_topic=entity_get_topic(state_topic),
        availability_topic=entity_availability_topic(state_topic)
        if compiled.profile.availability == "device"
        else None,
        optimistic=has_optimistic_command,
        discovery=(),  # WP10 renders real Home Assistant discovery payloads; empty until then.
        transform=compiled.profile.transform,
        entity_category=entity_category,
    )
    return entity, attrs.bindings, egress, attrs.initial_values


# ------------------------------------------------------------------------------- compile()


def compile(config: Configuration, profiles: ProfileRegistry, options: CompileOptions) -> Model:
    """Pure: config JSON + a loaded `ProfileRegistry` -> a flat, compiled `Model` (docs/03 §4)."""
    ctx = _CompileContext(
        floorplan=_resolve_floorplan(config),
        registry=profiles,
        options=options,
        tally=_Tally(),
        seen_unknown_function_ids=set(),
    )
    compiled_channels = _collect_channels(config, ctx)
    slugs = _resolve_names_and_slugs(compiled_channels, options.aliases)

    entities: list[Entity] = []
    ingress: dict[str, Binding] = {}
    egress: dict[tuple[int, str], EgressBinding] = {}
    by_id: dict[str, int] = {}
    by_topic: dict[str, int] = {}
    initial_values: list[tuple[Any, ...]] = []

    for idx, compiled in enumerate(compiled_channels):
        slug = slugs[compiled.entity_id]
        entity, entity_ingress, entity_egress, entity_values = _build_entity(
            idx, compiled, slug, options
        )
        entities.append(entity)
        ingress.update(entity_ingress)
        egress.update(entity_egress)
        by_id[entity.id] = idx
        by_topic[slug] = idx
        initial_values.append(entity_values)
        ctx.tally.entities_created += 1

    discovery = tuple(chain.from_iterable(entity.discovery for entity in entities))
    stats = CompileStats(
        devices_total=ctx.tally.devices_total,
        devices_excluded_by_interface=ctx.tally.devices_excluded_by_interface,
        channels_total=ctx.tally.channels_total,
        channels_orphaned=ctx.tally.channels_orphaned,
        channels_unknown_function=ctx.tally.channels_unknown_function,
        channels_unsupported=ctx.tally.channels_unsupported,
        channels_ambiguous_profile=ctx.tally.channels_ambiguous_profile,
        entities_created=ctx.tally.entities_created,
        channels_excluded_by_option=ctx.tally.channels_excluded_by_option,
    )
    return Model(
        entities=tuple(entities),
        ingress=ingress,
        egress=egress,
        by_id=by_id,
        by_topic=by_topic,
        discovery=discovery,
        initial_values=tuple(initial_values),
        stats=stats,
    )
