"""Decode/encode primitives and the codec registry (docs/03 §5; docs/11 WP3).

Every decoder maps ``""`` to ``None`` (P-04) -- except `trigger`, which is event-only and treats
*any* payload, including ``""``, as the edge signal itself (docs/03 §5). No decoder raises on
malformed input; it returns ``None`` instead, so one misbehaving sensor never kills ingress
(the caller counts ``codec_errors``). Codecs are pure and bound once at compile time
(`build_codec`) so nothing on the hot path does name lookup or dispatch (ADR-004).
"""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

_PERCENT_MIN = 0
_PERCENT_MAX = 100


class CommandError(Exception):
    """A command value could not be encoded (docs/03 §6): reject, never clamp silently."""


@dataclass(frozen=True, slots=True)
class Codec:
    """A bound ``(decode, encode)`` pair, parameters already baked in (docs/03 §5)."""

    name: str
    decode: Callable[[str], Any]
    encode: Callable[[Any], str]


def _parse_finite_float(raw: str) -> float | None:
    """Parse `raw` as a float, rejecting non-finite results.

    ``float()`` happily parses ``"inf"``/``"-inf"``/``"nan"`` without raising, and passing those
    on to ``round()``/``int()`` raises ``OverflowError``/``ValueError`` respectively -- a decoder
    totality gap that hypothesis (docs/10 §5) found. Centralised so every numeric codec is safe.
    """
    try:
        value = float(raw)
    except ValueError:
        return None
    return value if math.isfinite(value) else None


def _clamp_percent(value: float) -> int:
    return max(_PERCENT_MIN, min(_PERCENT_MAX, round(value)))


def _decode_bool01(raw: str) -> bool | None:
    if raw == "1":
        return True
    if raw == "0":
        return False
    return None


def _encode_bool01(value: Any) -> str:
    return "1" if value else "0"


def _make_bool01() -> Codec:
    return Codec("bool01", _decode_bool01, _encode_bool01)


def _decode_int(raw: str) -> int | None:
    # P-05: values arrive as "42.0"; int(v) alone would raise, so parse as float first.
    value = _parse_finite_float(raw)
    return None if value is None else int(value)


def _encode_int(value: Any) -> str:
    return str(int(value))


def _make_int() -> Codec:
    return Codec("int", _decode_int, _encode_int)


def _make_float(*, precision: int = 2) -> Codec:
    def decode(raw: str) -> float | None:
        value = _parse_finite_float(raw)
        return None if value is None else round(value, precision)

    def encode(value: Any) -> str:
        return str(round(float(value), precision))

    return Codec("float", decode, encode)


def _decode_percent_int(raw: str) -> int | None:
    value = _parse_finite_float(raw)
    return None if value is None else _clamp_percent(value)


def _encode_percent_int(value: Any) -> str:
    return str(_clamp_percent(float(value)))


def _make_percent_int() -> Codec:
    return Codec("percent_int", _decode_percent_int, _encode_percent_int)


def _decode_percent_inverted(raw: str) -> int | None:
    value = _parse_finite_float(raw)
    return None if value is None else _PERCENT_MAX - _clamp_percent(value)


def _encode_percent_inverted(value: Any) -> str:
    return str(_PERCENT_MAX - _clamp_percent(float(value)))


def _make_percent_inverted() -> Codec:
    return Codec("percent_inverted", _decode_percent_inverted, _encode_percent_inverted)


def _make_enum(
    *, decode_values: Mapping[str, Any], encode_values: Mapping[Any, str], default: Any = None
) -> Codec:
    """`decode_values`: raw -> symbolic (attribute direction, docs/03 §3.2).

    `encode_values`: symbolic -> raw (command direction, docs/03 §3.3) -- independent maps
    (P-03): never derive one from the other.
    """

    def decode(raw: str) -> Any:
        if raw == "":
            return default
        return decode_values.get(raw, default)

    def encode(value: Any) -> str:
        try:
            return encode_values[value]
        except KeyError as exc:
            raise CommandError(f"unknown enum value: {value!r}") from exc

    return Codec("enum", decode, encode)


def _make_scaled(*, factor: float, offset: float = 0.0, precision: int = 2) -> Codec:
    def decode(raw: str) -> float | None:
        value = _parse_finite_float(raw)
        if value is None:
            return None
        scaled = value * factor + offset
        return round(scaled, precision) if math.isfinite(scaled) else None

    def encode(value: Any) -> str:
        return str(round((float(value) - offset) / factor, precision))

    return Codec("scaled", decode, encode)


def _make_color_temp_pct(*, warmest_kelvin: float, coolest_kelvin: float) -> Codec:
    """Bound per entity at compile time from the channel's own physical parameters (P-09):
    never hardcode 2700-6500K.
    """
    span = coolest_kelvin - warmest_kelvin

    def decode(raw: str) -> int | None:
        pct = _parse_finite_float(raw)
        if pct is None:
            return None
        pct = max(0.0, min(100.0, pct))
        return round(warmest_kelvin + span * pct / 100)

    def encode(value: Any) -> str:
        pct = (float(value) - warmest_kelvin) / span * 100 if span else 0.0
        return str(round(max(0.0, min(100.0, pct))))

    return Codec("color_temp_pct", decode, encode)


def _decode_string(raw: str) -> str | None:
    return raw if raw != "" else None


def _encode_string(value: Any) -> str:
    return str(value)


def _make_string() -> Codec:
    return Codec("string", _decode_string, _encode_string)


def _decode_trigger(_raw: str) -> bool:
    return True


def _encode_trigger(_value: Any) -> str:
    raise CommandError("trigger is an event-only codec and cannot be encoded")


def _make_trigger() -> Codec:
    return Codec("trigger", _decode_trigger, _encode_trigger)


_REGISTRY: dict[str, Callable[..., Codec]] = {
    "bool01": _make_bool01,
    "int": _make_int,
    "float": _make_float,
    "percent_int": _make_percent_int,
    "percent_inverted": _make_percent_inverted,
    "enum": _make_enum,
    "scaled": _make_scaled,
    "color_temp_pct": _make_color_temp_pct,
    "string": _make_string,
    "trigger": _make_trigger,
}


KNOWN_CODECS: frozenset[str] = frozenset(_REGISTRY)
"""Every codec name `build_codec` accepts -- the closed set profile validation (`model/profiles.py`)
checks a profile's `codec:` string against, so a typo fails at load time, not at compile time.
"""


def build_codec(name: str, **params: Any) -> Codec:
    """Build a bound `Codec` from the registry (docs/03 §5). Raises `ValueError` for an unknown
    name -- a profile-authoring bug, caught at load time (`model/profiles.py`), not the hot path.
    """
    try:
        factory = _REGISTRY[name]
    except KeyError as exc:
        raise ValueError(f"unknown codec: {name!r}") from exc
    return factory(**params)
