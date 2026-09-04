"""Validate tests/fixtures/*.json against docs/10 §3.2 -- these are inputs for later WPs' compiler
and integration tests, so their shape and content matter as much as their existence.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from freeathome2mqtt.sysap.schema import parse_function_id

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


@pytest.mark.parametrize("name", ["minimal.json", "typical.json", "nasty.json"])
def test_fixture_is_valid_json_wrapped_by_one_sysap_uuid(name: str) -> None:
    data = _load(name)
    assert len(data) == 1
    (uuid, body) = next(iter(data.items()))
    assert isinstance(uuid, str)
    assert "devices" in body
    assert "floorplan" in body


def _all_channels(body: dict[str, Any]) -> list[dict[str, Any]]:
    channels = []
    for device in body["devices"].values():
        channels.extend(device["channels"].values())
    return channels


def test_minimal_fixture_has_exactly_one_device_and_channel() -> None:
    body = next(iter(_load("minimal.json").values()))
    assert len(body["devices"]) == 1
    assert len(_all_channels(body)) == 1


def test_typical_fixture_is_a_realistic_mix() -> None:
    body = next(iter(_load("typical.json").values()))
    devices = body["devices"]
    channels = _all_channels(body)
    # docs/10 §3.2: "~40 devices, ~180 channels, realistic mix" -- ballpark, not exact.
    assert 35 <= len(devices) <= 55
    assert 100 <= len(channels) <= 220
    function_ids = {parse_function_id(ch["functionID"]) for ch in channels}
    # A realistic mix touches several distinct tier-1 function types (docs/03 §9), not just one.
    assert len(function_ids) >= 8


def test_nasty_fixture_uses_a_non_default_sysap_uuid() -> None:
    (uuid,) = _load("nasty.json").keys()
    assert uuid != "00000000-0000-0000-0000-000000000000"


def test_nasty_fixture_contains_an_empty_datapoint_value() -> None:
    body = next(iter(_load("nasty.json").values()))
    values = [dp["value"] for ch in _all_channels(body) for dp in ch.get("outputs", {}).values()]
    assert "" in values


def test_nasty_fixture_contains_a_float_looking_integer_value() -> None:
    body = next(iter(_load("nasty.json").values()))
    values = [dp["value"] for ch in _all_channels(body) for dp in ch.get("outputs", {}).values()]
    assert "42.0" in values


def test_nasty_fixture_contains_placeholder_channel_names() -> None:
    body = next(iter(_load("nasty.json").values()))
    names = [ch.get("displayName") for ch in _all_channels(body)]
    assert "Ⓐ" in names
    assert None in names


def test_nasty_fixture_contains_a_null_rooms_floor() -> None:
    body = next(iter(_load("nasty.json").values()))
    floors = body["floorplan"]["floors"]
    assert any(floor.get("rooms") is None for floor in floors.values())


def test_nasty_fixture_contains_an_orphan_channel() -> None:
    body = next(iter(_load("nasty.json").values()))
    orphans = [
        (device, ch)
        for device in body["devices"].values()
        for ch in device["channels"].values()
        if "floor" not in device and "floor" not in ch
    ]
    assert orphans


def test_nasty_fixture_contains_unknown_and_unparseable_function_ids() -> None:
    body = next(iter(_load("nasty.json").values()))
    raw_ids = [ch["functionID"] for ch in _all_channels(body)]
    assert "9999" in raw_ids  # parseable hex, not a recognised Function
    assert parse_function_id("9999") is not None
    assert "" in raw_ids  # missing
    assert "ZZ" in raw_ids  # unparseable
    assert parse_function_id("ZZ") is None


def test_nasty_fixture_contains_colliding_channel_display_names() -> None:
    body = next(iter(_load("nasty.json").values()))
    names = [ch["displayName"] for ch in _all_channels(body) if ch.get("displayName")]
    duplicates = {name for name in names if names.count(name) > 1}
    assert "Deckenlicht" in duplicates


def test_nasty_fixture_contains_umlauts_and_eszett() -> None:
    body = next(iter(_load("nasty.json").values()))
    blob = json.dumps(body, ensure_ascii=False)
    assert "ü" in blob or "Ü" in blob
    assert "ß" in blob
