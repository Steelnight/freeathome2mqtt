"""The named transform escape hatch for cross-datapoint channel logic (ADR-003; docs/03 §7).

The `transform:` registry is the one sanctioned dynamic-dispatch mechanism in this codebase
(CLAUDE.md rule 8): a fixed, reviewed, named set of classes registered at import time via
`@transform(name)`, never an open lookup from a profile- or channel-supplied string beyond that
fixed set. Each transform is a pure function of an entity's raw attribute values -- no I/O, no
clock, no globals -- so it stays testable in isolation and runs off the hot path, after change
detection, only for entities that actually changed.

`values` in both methods below is positional, parallel to the owning profile's own `attributes:`
declaration order -- the same convention `Entity.attr_names`/`StateStore.values` already use. Each
transform's docstring states exactly what its indices mean; that positional contract is the
tradeoff of an escape hatch, not something to abstract away.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Sequence
from typing import Any

from freeathome2mqtt.model.codecs import CommandError

_COVER_FULLY_OPEN = 100


class Transform(ABC):
    """Base for the named escape-hatch transforms (docs/03 §7)."""

    @abstractmethod
    def derive(self, values: Sequence[Any]) -> dict[str, Any]:
        """Derive composite attributes from raw ones after any change."""

    @abstractmethod
    def command(self, name: str, value: Any, values: Sequence[Any]) -> list[tuple[str, Any]]:
        """Return (sub_command_name, symbolic_value) writes for one logical command.

        `sub_command_name` names one of the *profile's own* commands, and `symbolic_value` is that
        command's own pre-encode value -- the caller resolves the name to a compiled
        `EgressBinding` and calls its `encode()`, exactly as it would for a non-transformed
        command. A transform has no compile-time knowledge of REST paths or raw wire encodings;
        both belong to the profile's codec, not here.
        """


_REGISTRY: dict[str, type[Transform]] = {}


def transform(name: str) -> Callable[[type[Transform]], type[Transform]]:
    """Register a `Transform` subclass under `name` (docs/03 §7). The registry is fixed at import
    time; there is no runtime registration from profile- or channel-supplied input.
    """

    def decorator(cls: type[Transform]) -> type[Transform]:
        if name in _REGISTRY:
            raise ValueError(f"duplicate transform name: {name!r}")
        _REGISTRY[name] = cls
        return cls

    return decorator


def get_transform(name: str) -> Transform:
    """Instantiate the named transform (docs/03 §7). Raises `KeyError` for an unknown name -- a
    profile-authoring bug, the same trust boundary as an unknown codec (CLAUDE.md rule 5).
    """
    return _REGISTRY[name]()


@transform("room_temperature_controller")
class RoomTemperatureControllerTransform(Transform):
    """HVAC mode is derived from on/off + eco + the raw heating/cooling mode; setting a mode
    writes 2-3 datapoints (docs/03 §7).

    `values`: ``[0]`` on_off (bool), ``[1]`` eco (bool), ``[2]`` mode (``"heating"``/``"cooling"``,
    the `mode` attribute's own symbolic enum values -- their raw wire encoding is the codec's
    concern, declared on the profile, not this transform's).
    """

    def derive(self, values: Sequence[Any]) -> dict[str, Any]:
        on_off, eco, mode = values[0], values[1], values[2]
        if not on_off:
            return {"hvac_mode": "off"}
        if eco:
            return {"hvac_mode": "eco"}
        return {"hvac_mode": mode}

    def command(self, name: str, value: Any, values: Sequence[Any]) -> list[tuple[str, Any]]:
        if name != "hvac_mode":
            raise CommandError(f"{type(self).__name__} has no command {name!r}")
        if value == "off":
            return [("on_off", False)]
        if value == "eco":
            return [("on_off", True), ("eco", True)]
        if value in ("heating", "cooling"):
            return [("on_off", True), ("eco", False), ("mode", value)]
        raise CommandError(f"unknown hvac_mode: {value!r}")


@transform("cover_with_slats")
class CoverWithSlatsTransform(Transform):
    """Position and slat angle interact: a fully open cover has nothing to tilt, so its slat
    position is reported as unknown rather than a stale raw reading (docs/03 §7).

    `values`: ``[0]`` position (``0..100``, ``100`` = open), ``[1]`` slat_position (``0..100``).

    ⚠ `AL_STOP_STEP_UP_DOWN` is dual-purpose on real hardware -- it stops a moving cover, or steps
    the slats by a small increment when already stationary. Which applies is decided by the device
    from its own motion state, not by this bridge, so `command("stop", ...)` always writes the
    same value; confirm against real hardware (docs/11's guidance for the implementing agent, #4).
    """

    def derive(self, values: Sequence[Any]) -> dict[str, Any]:
        position, slat_position = values[0], values[1]
        return {"slat_position": None if position == _COVER_FULLY_OPEN else slat_position}

    def command(self, name: str, value: Any, values: Sequence[Any]) -> list[tuple[str, Any]]:
        if name == "position":
            # Opening fully also resets the slats to flat (0): a fully retracted cover has
            # nothing to tilt, so leaving a stale slat command behind would be misleading.
            writes: list[tuple[str, Any]] = [("position", value)]
            if value == _COVER_FULLY_OPEN:
                writes.append(("slat_position", 0))
            return writes
        if name == "stop":
            return [("stop", True)]
        raise CommandError(f"{type(self).__name__} has no command {name!r}")
