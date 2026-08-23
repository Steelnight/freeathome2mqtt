"""Tests for sysap/schema.py: TypedDicts (no validation) and the functionID hex parser (P-06)."""

import pytest

from freeathome2mqtt.sysap.schema import parse_function_id


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("7", 7),
        ("1B", 0x1B),
        ("20A", 0x20A),
        ("0", 0),
        ("ffff", 0xFFFF),
    ],
)
def test_function_id_hex_parsing(raw: str, expected: int) -> None:
    assert parse_function_id(raw) == expected


def test_function_id_decimal_looking_value_is_hex_not_decimal() -> None:
    # P-06: "12" must parse as 0x12 (18), never decimal 12 -- exactly the bug this pitfall names.
    assert parse_function_id("12") == 0x12
    assert parse_function_id("12") != 12


@pytest.mark.parametrize("raw", ["", None, "not-hex", "12G"])
def test_function_id_missing_or_unparseable_returns_none(raw: str | None) -> None:
    assert parse_function_id(raw) is None
