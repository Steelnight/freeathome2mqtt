"""Tests for model/codecs.py: the codec registry (docs/03 §5; docs/11 WP3)."""

import math

import pytest
from hypothesis import given
from hypothesis import strategies as st

from freeathome2mqtt.model.codecs import KNOWN_CODECS, CommandError, build_codec

# Every codec name that takes no required construction parameters, plus one representative
# parametrised instance each -- used by test_all_codecs_map_empty_to_none and the totality
# property, so a newly added codec must be added here to be covered (docs/10 §3.3's stated intent).
ALL_CODECS = {
    "bool01": build_codec("bool01"),
    "int": build_codec("int"),
    "float": build_codec("float", precision=2),
    "percent_int": build_codec("percent_int"),
    "percent_inverted": build_codec("percent_inverted"),
    "enum": build_codec(
        "enum", decode_values={"0": "off"}, encode_values={"off": "0"}, default=None
    ),
    "scaled": build_codec("scaled", factor=0.1),
    "color_temp_pct": build_codec("color_temp_pct", warmest_kelvin=2700, coolest_kelvin=6500),
    "string": build_codec("string"),
}
# `trigger` is deliberately excluded (docs/03 §5): it is event-only and "any value -> true",
# including "" -- the frame's arrival is the signal, not its payload, so there is no "absent"
# case to map to None the way every other codec has one.
TRIGGER_CODEC = build_codec("trigger")


@pytest.mark.parametrize("name", sorted(ALL_CODECS))
def test_all_codecs_map_empty_to_none(name: str) -> None:
    assert ALL_CODECS[name].decode("") is None


def test_trigger_maps_empty_to_true_not_none() -> None:
    assert TRIGGER_CODEC.decode("") is True


def test_build_codec_rejects_unknown_name() -> None:
    with pytest.raises(ValueError, match="unknown codec"):
        build_codec("not-a-real-codec")


def test_known_codecs_matches_every_name_all_codecs_covers() -> None:
    # model/profiles.py validates a profile's `codec:` string against this constant at load time
    # (a profile-authoring trust boundary, CLAUDE.md rule 5) -- it must never drift from the
    # registry `build_codec` actually dispatches through.
    assert set(ALL_CODECS) | {"trigger"} == KNOWN_CODECS


# Regression for a real bug hypothesis found: float() parses "inf"/"nan" without raising, and
# passing those through round()/int() raises OverflowError/ValueError -- uncaught by a bare
# `except ValueError` around float(raw) alone. Pinned as explicit examples because hypothesis's
# plain st.text() is unlikely to reliably rediscover this exact string on a fresh run/CI without
# a persisted example database (docs/10 §5 property tests must still hold on a clean checkout).
NON_FINITE_INPUTS = ["inf", "-inf", "Infinity", "-Infinity", "nan", "NaN", "-nan"]
# Only codecs that parse the raw string as a float are exposed to this; bool01/enum/string treat
# "inf"/"nan" as just another unrecognised (or, for string, perfectly valid) piece of text.
NUMERIC_CODEC_NAMES = [
    "int",
    "float",
    "percent_int",
    "percent_inverted",
    "scaled",
    "color_temp_pct",
]


@pytest.mark.parametrize("name", NUMERIC_CODEC_NAMES)
@pytest.mark.parametrize("raw", NON_FINITE_INPUTS)
def test_numeric_codecs_reject_non_finite_float_strings(name: str, raw: str) -> None:
    assert ALL_CODECS[name].decode(raw) is None


# ------------------------------------------------------------------------------- bool01


@pytest.mark.parametrize(("raw", "expected"), [("1", True), ("0", False)])
def test_bool01_decode(raw: str, expected: bool) -> None:
    assert build_codec("bool01").decode(raw) is expected


@pytest.mark.parametrize("raw", ["2", "garbage", "-1"])
def test_bool01_decode_unrecognised_value_is_none(raw: str) -> None:
    assert build_codec("bool01").decode(raw) is None


def test_bool01_encode() -> None:
    codec = build_codec("bool01")
    assert codec.encode(True) == "1"
    assert codec.encode(False) == "0"


@pytest.mark.parametrize("raw", ["OFF", "off", "false", "False", "0", "no"])
def test_bool01_encode_recognises_falsy_strings(raw: str) -> None:
    # docs/11 WP10, regression: Home Assistant's MQTT JSON light schema sends {"state": "OFF"} --
    # a non-empty string, which plain `"1" if value else "0"` truthiness always read as True.
    assert build_codec("bool01").encode(raw) == "0"


@pytest.mark.parametrize("raw", ["ON", "on", "true", "True", "1", "yes"])
def test_bool01_encode_recognises_truthy_strings(raw: str) -> None:
    assert build_codec("bool01").encode(raw) == "1"


# ------------------------------------------------------------------------------- int


def test_int_codec_accepts_float_strings() -> None:
    # P-05: integer-typed datapoints arrive as "42.0"; int(v) would raise, int(float(v)) must not.
    assert build_codec("int").decode("42.0") == 42


def test_int_codec_decode_malformed_is_none() -> None:
    assert build_codec("int").decode("not-a-number") is None


def test_int_codec_encode() -> None:
    assert build_codec("int").encode(7) == "7"


# ------------------------------------------------------------------------------- float


def test_float_codec_rounds_to_precision() -> None:
    codec = build_codec("float", precision=1)
    assert codec.decode("21.567") == 21.6


def test_float_codec_decode_malformed_is_none() -> None:
    assert build_codec("float", precision=2).decode("nope") is None


# ------------------------------------------------------------------------------- percent_int


def test_percent_int_clamps_decode() -> None:
    codec = build_codec("percent_int")
    assert codec.decode("150") == 100
    assert codec.decode("-5") == 0


def test_percent_int_clamps_encode() -> None:
    codec = build_codec("percent_int")
    assert codec.encode(150) == "100"
    assert codec.encode(-5) == "0"


# ------------------------------------------------------------------------------- percent_inverted


def test_percent_inverted_covers_the_boundary_conditions() -> None:
    # docs/01 §6.1: free@home 0 = open, 100 = closed; our own convention is the opposite.
    codec = build_codec("percent_inverted")
    assert codec.decode("0") == 100
    assert codec.decode("100") == 0


def test_percent_inverted_roundtrip() -> None:
    codec = build_codec("percent_inverted")
    assert codec.decode(codec.encode(37)) == 37


# ------------------------------------------------------------------------------- enum


def test_enum_codec_has_independent_decode_and_encode_maps() -> None:
    # P-03: AL_FORCED writes 2/3 but AL_INFO_FORCE reads 4/5 -- asymmetric, never derive one map
    # from the other.
    codec = build_codec(
        "enum",
        decode_values={"0": "deactivated", "4": "forced_on", "5": "forced_off"},
        encode_values={"deactivated": "0", "forced_on": "3", "forced_off": "2"},
        default="unknown",
    )
    assert codec.decode("4") == "forced_on"
    assert codec.encode("forced_on") == "3"


def test_enum_codec_decode_unmapped_value_is_default() -> None:
    codec = build_codec(
        "enum", decode_values={"0": "off"}, encode_values={"off": "0"}, default="unknown"
    )
    assert codec.decode("99") == "unknown"


def test_enum_codec_encode_unknown_value_raises_command_error() -> None:
    codec = build_codec("enum", decode_values={"0": "off"}, encode_values={"off": "0"})
    with pytest.raises(CommandError):
        codec.encode("not-a-member")


# ------------------------------------------------------------------------------- scaled


def test_scaled_codec_applies_factor_and_offset() -> None:
    codec = build_codec("scaled", factor=0.1, offset=-40, precision=1)
    assert codec.decode("500") == 10.0


def test_scaled_codec_roundtrip() -> None:
    codec = build_codec("scaled", factor=0.5, offset=2, precision=2)
    assert codec.decode(codec.encode(12.0)) == 12.0


# ------------------------------------------------------------------------------- color_temp_pct


def test_color_temp_pct_uses_channel_parameters() -> None:
    # P-09: never hardcode 2700-6500K; bind the channel's own physical bounds.
    codec = build_codec("color_temp_pct", warmest_kelvin=2000, coolest_kelvin=4000)
    assert codec.decode("0") == 2000
    assert codec.decode("100") == 4000
    assert codec.decode("50") == 3000


def test_color_temp_pct_roundtrip() -> None:
    codec = build_codec("color_temp_pct", warmest_kelvin=2700, coolest_kelvin=6500)
    assert codec.decode(codec.encode(4600)) == 4600


# ------------------------------------------------------------------------------- string


def test_string_codec_passthrough() -> None:
    codec = build_codec("string")
    assert codec.decode("hello") == "hello"
    assert codec.encode("hello") == "hello"


# ------------------------------------------------------------------------------- trigger


def test_trigger_decode_ignores_payload() -> None:
    codec = build_codec("trigger")
    assert codec.decode("anything") is True
    assert codec.decode("0") is True


def test_trigger_encode_raises() -> None:
    codec = build_codec("trigger")
    with pytest.raises(CommandError):
        codec.encode(True)


# ------------------------------------------------------------------------------- property tests


@pytest.mark.parametrize("name", sorted(ALL_CODECS))
@given(raw=st.text(max_size=32))
def test_codec_totality(name: str, raw: str) -> None:
    # No decoder may raise for ANY string input (docs/10 §5); malformed input yields None instead.
    ALL_CODECS[name].decode(raw)


@given(raw=st.text(max_size=32))
def test_trigger_codec_totality(raw: str) -> None:
    assert TRIGGER_CODEC.decode(raw) is True


@given(value=st.booleans())
def test_bool01_round_trip(value: bool) -> None:
    codec = build_codec("bool01")
    assert codec.decode(codec.encode(value)) is value


@given(value=st.integers(min_value=-1_000_000, max_value=1_000_000))
def test_int_round_trip(value: int) -> None:
    codec = build_codec("int")
    assert codec.decode(codec.encode(value)) == value


@given(value=st.integers(min_value=0, max_value=100))
def test_percent_int_round_trip(value: int) -> None:
    codec = build_codec("percent_int")
    assert codec.decode(codec.encode(value)) == value


@given(value=st.integers(min_value=0, max_value=100))
def test_percent_inverted_round_trip_property(value: int) -> None:
    codec = build_codec("percent_inverted")
    assert codec.decode(codec.encode(value)) == value


@given(
    value=st.floats(min_value=-1000, max_value=1000, allow_nan=False, allow_infinity=False).map(
        lambda v: round(v, 3)
    )
)
def test_float_round_trip(value: float) -> None:
    codec = build_codec("float", precision=3)
    result = codec.decode(codec.encode(value))
    assert result is not None
    assert math.isclose(result, value, abs_tol=1e-6)
