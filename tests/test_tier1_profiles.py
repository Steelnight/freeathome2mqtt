"""Round-trip fixtures for the shipped tier-1 profiles (docs/03 §9; docs/11 WP4).

Each fixture compiles a single, fully-provisioned channel against the real, shipped
`ProfileRegistry` (not a synthetic in-memory one, unlike tests/test_compiler.py) and checks the
resulting entity's attributes/commands decode and encode correctly. `test_profile_wellformed`
additionally checks every shipped profile's own internal consistency.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from freeathome2mqtt.model.codecs import KNOWN_CODECS, build_codec
from freeathome2mqtt.model.compiler import CompileOptions, compile
from freeathome2mqtt.model.profiles import ProfileRegistry, load_profile_registry

PROFILES_DIR = Path(__file__).resolve().parent.parent / "src" / "freeathome2mqtt" / "profiles"
REGISTRY: ProfileRegistry = load_profile_registry(PROFILES_DIR)
_TIER1_MATCH_RATE_FLOOR = 0.85


def _config(
    channel: dict[str, Any], *, functionID: str, extra_device: dict[str, Any] | None = None
) -> dict[str, Any]:
    device: dict[str, Any] = {
        "displayName": "Test Device",
        "interface": "TP",
        "floor": "01",
        "room": "0C",
        "channels": {
            "ch0000": {**channel, "functionID": functionID, "displayName": "Test Channel"}
        },
    }
    if extra_device:
        device.update(extra_device)
    return {
        "floorplan": {"floors": {"01": {"name": "GF", "rooms": {"0C": {"name": "Room"}}}}},
        "devices": {"ABB700000001": device},
    }


def _compile_one(config: dict[str, Any]) -> Any:
    model = compile(config, REGISTRY, CompileOptions())
    assert len(model.entities) == 1, model.stats
    return model


# ------------------------------------------------------------------------------- wellformedness


@pytest.mark.parametrize("profile", list(REGISTRY), ids=lambda p: p.id)
def test_profile_wellformed(profile: Any) -> None:
    assert profile.attributes, f"{profile.id}: profile must have at least one attribute"

    for cmd_name, spec in profile.commands.items():
        if spec.optimistic is not None:
            assert spec.optimistic in profile.attributes, (
                f"{profile.id}: command {cmd_name!r}'s optimistic: {spec.optimistic!r} "
                "does not name a real attribute"
            )

    for kind, name, spec in [
        (k, n, s)
        for k, group in [("attribute", profile.attributes), ("command", profile.commands)]
        for n, s in group.items()
    ]:
        assert spec.codec in KNOWN_CODECS, f"{profile.id}: {kind} {name!r} has an unknown codec"
        if spec.codec == "enum":
            assert spec.values, f"{profile.id}: {kind} {name!r} is codec: enum with no values"
        elif spec.codec != "color_temp_pct":
            # color_temp_pct needs channel parameters (model/compiler.py); every other codec must
            # be buildable from the profile's own declared params alone.
            build_codec(spec.codec, **spec.codec_params)

    if profile.transform == "cover_with_slats":
        assert list(profile.attributes)[:2] == ["position", "slat_position"], (
            f"{profile.id}: cover_with_slats needs position, slat_position at indices 0, 1"
        )
    if profile.transform == "room_temperature_controller":
        assert list(profile.attributes)[:3] == ["on_off", "eco", "mode"], (
            f"{profile.id}: room_temperature_controller needs on_off, eco, mode at indices 0-2"
        )


# ------------------------------------------------------------------------------- lighting


def test_switch_actuator_round_trip() -> None:
    channel = {
        "inputs": {"idp0000": {"pairingID": 1, "value": "0"}},
        "outputs": {"odp0000": {"pairingID": 256, "value": "1"}},
    }
    model = _compile_one(_config(channel, functionID="7"))
    entity = model.entities[0]
    assert entity.profile == "switch_actuator"
    assert model.initial_values[0] == (True,)
    assert model.egress[(0, "state")].encode(False) == "0"


def test_dimming_actuator_round_trip() -> None:
    channel = {
        "inputs": {
            "idp0000": {"pairingID": 1, "value": "1"},
            "idp0001": {"pairingID": 17, "value": "50"},
            "idp0002": {"pairingID": 3, "value": "0"},
        },
        "outputs": {
            "odp0000": {"pairingID": 256, "value": "1"},
            "odp0001": {"pairingID": 272, "value": "50"},
            "odp0002": {"pairingID": 257, "value": "4"},
        },
    }
    model = _compile_one(_config(channel, functionID="12"))
    entity = model.entities[0]
    assert entity.profile == "dimming_actuator"
    assert set(entity.attr_names) == {"state", "brightness", "forced_position"}
    values = dict(zip(entity.attr_names, model.initial_values[0], strict=True))
    assert values == {"state": True, "brightness": 50, "forced_position": "forced_on"}
    assert model.egress[(0, "brightness")].validate(0) == 1  # P-08: never 0, clamps to 1
    assert model.egress[(0, "forced_position")].encode("forced_on") == "3"  # P-03: asymmetric


def test_colour_temperature_actuator_round_trip() -> None:
    channel = {
        "inputs": {
            "idp0000": {"pairingID": 1, "value": "1"},
            "idp0001": {"pairingID": 17, "value": "50"},
            "idp0002": {"pairingID": 22, "value": "50"},
        },
        "outputs": {
            "odp0000": {"pairingID": 256, "value": "1"},
            "odp0001": {"pairingID": 272, "value": "50"},
            "odp0002": {"pairingID": 280, "value": "50"},
        },
        "parameters": {"par00f6": "2700", "par00f5": "6500"},
    }
    model = _compile_one(_config(channel, functionID="12"))
    entity = model.entities[0]
    assert entity.profile == "colour_temperature_actuator"
    values = dict(zip(entity.attr_names, model.initial_values[0], strict=True))
    assert values["color_temp"] == 4600  # 50% between 2700 and 6500
    assert model.egress[(0, "color_temp")].encode(2700) == "0"


# ------------------------------------------------------------------------------- covers


def test_cover_position_inversion_roundtrip() -> None:
    # P-07's named test: free@home 0 = open, 100 = closed; our own convention is the opposite.
    channel = {
        "inputs": {"idp0000": {"pairingID": 35, "value": "0"}},
        "outputs": {"odp0000": {"pairingID": 289, "value": "0"}},
    }
    model = _compile_one(_config(channel, functionID="9"))
    entity = model.entities[0]
    assert entity.profile == "cover_actuator"
    assert model.initial_values[0] == (100,)  # raw 0 (open) -> our 100 (open)

    egress = model.egress[(0, "position")]
    assert egress.encode(100) == "0"  # our 100 (open) -> raw 0
    assert egress.encode(0) == "100"  # our 0 (closed) -> raw 100
    binding = model.ingress["ABB700000001/ch0000/odp0000"]
    assert binding.decode(egress.encode(0)) == 0
    assert binding.decode(egress.encode(100)) == 100


def test_cover_with_slats_round_trip() -> None:
    channel = {
        "inputs": {
            "idp0000": {"pairingID": 35, "value": "0"},
            "idp0001": {"pairingID": 36, "value": "0"},
        },
        "outputs": {
            "odp0000": {"pairingID": 289, "value": "20"},
            "odp0001": {"pairingID": 290, "value": "50"},
        },
    }
    model = _compile_one(_config(channel, functionID="61"))
    entity = model.entities[0]
    assert entity.profile == "cover_with_slats"
    assert entity.attr_names == ("position", "slat_position")
    values = dict(zip(entity.attr_names, model.initial_values[0], strict=True))
    assert values == {"position": 80, "slat_position": 50}


# ------------------------------------------------------------------------------- climate


def test_room_temperature_controller_round_trip() -> None:
    channel = {
        "inputs": {
            "idp0000": {"pairingID": 51, "value": "21.0"},
            "idp0001": {"pairingID": 56, "value": "1"},
            "idp0002": {"pairingID": 58, "value": "0"},
            "idp0003": {"pairingID": 61, "value": "0"},
        },
        "outputs": {
            "odp0000": {"pairingID": 304, "value": "21.0"},
            "odp0001": {"pairingID": 305, "value": "0"},
            "odp0002": {"pairingID": 56, "value": "1"},
            "odp0003": {"pairingID": 68, "value": "0"},
            "odp0004": {"pairingID": 62, "value": "0"},
        },
    }
    model = _compile_one(_config(channel, functionID="a"))
    entity = model.entities[0]
    assert entity.profile == "room_temperature_controller"
    assert entity.attr_names[:3] == ("on_off", "eco", "mode")
    values = dict(zip(entity.attr_names, model.initial_values[0], strict=True))
    assert values["on_off"] is True
    assert values["eco"] is False
    assert values["mode"] == "heating"
    assert model.egress[(0, "setpoint_temperature")].encode(21.5) == "21.5"


def test_room_temperature_controller_basic_matches_the_simpler_channel() -> None:
    # The typical.json fixture's RTC channels have no on_off/eco/mode -- the basic profile is what
    # they match instead (docs/03 §3.4's most-specific-wins picks the full profile only when the
    # extra pairings are actually present).
    channel = {
        "inputs": {"idp0000": {"pairingID": 51, "value": "21.0"}},
        "outputs": {
            "odp0000": {"pairingID": 304, "value": "21.0"},
            "odp0001": {"pairingID": 305, "value": "0"},
        },
    }
    model = _compile_one(_config(channel, functionID="a"))
    assert model.entities[0].profile == "room_temperature_controller_basic"


def test_heating_actuator_round_trip() -> None:
    channel = {
        "inputs": {"idp0000": {"pairingID": 48, "value": "0"}},
        "outputs": {"odp0000": {"pairingID": 48, "value": "42"}},
    }
    model = _compile_one(_config(channel, functionID="27"))
    entity = model.entities[0]
    assert entity.profile == "heating_actuator"
    assert model.initial_values[0] == (42,)
    assert model.egress[(0, "heating_value")].encode(150) == "100"


# ------------------------------------------------------------------------------- sensors


def test_window_door_sensor_round_trip() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 53, "value": "1"}}}
    model = _compile_one(_config(channel, functionID="f"))
    assert model.entities[0].profile == "window_door_sensor"
    assert model.initial_values[0] == (True,)


def test_movement_detector_round_trip() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 358, "value": "1"}}}
    model = _compile_one(_config(channel, functionID="11"))
    assert model.entities[0].profile == "movement_detector"
    assert model.initial_values[0] == (True,)


def test_brightness_sensor_round_trip() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 1027, "value": "234"}}}
    model = _compile_one(_config(channel, functionID="41"))
    assert model.entities[0].profile == "brightness_sensor"
    assert model.initial_values[0] == (234,)


def test_temperature_sensor_round_trip() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 304, "value": "21.5"}}}
    model = _compile_one(_config(channel, functionID="43"))
    assert model.entities[0].profile == "temperature_sensor"
    assert model.initial_values[0] == (21.5,)


def test_switch_sensor_round_trip() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 1, "value": "1"}}}
    model = _compile_one(_config(channel, functionID="0"))
    entity = model.entities[0]
    assert entity.profile == "switch_sensor"
    assert entity.attr_kinds == (1,)  # AttrKind.EVENT


def test_trigger_round_trip() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 1, "value": ""}}}
    model = _compile_one(_config(channel, functionID="18"))
    entity = model.entities[0]
    assert entity.profile == "trigger"
    assert model.initial_values[0] == (True,)  # trigger: any payload, including "", means fired


def test_smoke_detector_round_trip() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 54, "value": "1"}}}
    model = _compile_one(_config(channel, functionID="7d"))
    assert model.entities[0].profile == "smoke_detector"
    assert model.initial_values[0] == (True,)


# ---------------------------------------------------------------------------- named P-03/P-08 tests


def test_forced_position_roundtrip_asymmetric() -> None:
    # P-03's named test: AL_FORCED writes 2/3, AL_INFO_FORCE reads 4/5 -- reading back after a
    # write must not silently become "unknown".
    channel = {
        "inputs": {
            "idp0000": {"pairingID": 1, "value": "1"},
            "idp0001": {"pairingID": 17, "value": "50"},
            "idp0002": {"pairingID": 3, "value": "0"},
        },
        "outputs": {
            "odp0000": {"pairingID": 256, "value": "1"},
            "odp0001": {"pairingID": 272, "value": "50"},
            "odp0002": {"pairingID": 257, "value": "0"},
        },
    }
    model = _compile_one(_config(channel, functionID="12"))
    egress = model.egress[(0, "forced_position")]
    assert egress.encode("forced_on") == "3"
    assert egress.encode("forced_off") == "2"
    binding = model.ingress["ABB700000001/ch0000/odp0002"]
    assert binding.decode("4") == "forced_on"
    assert binding.decode("5") == "forced_off"


def test_brightness_zero_maps_to_off() -> None:
    # P-08's named test: AL_ABSOLUTE_SET_VALUE_CONTROL accepts 1..100 only -- clamp, don't send 0.
    channel = {
        "inputs": {
            "idp0000": {"pairingID": 1, "value": "0"},
            "idp0001": {"pairingID": 17, "value": "0"},
        },
        "outputs": {
            "odp0000": {"pairingID": 256, "value": "0"},
            "odp0001": {"pairingID": 272, "value": "0"},
        },
    }
    model = _compile_one(_config(channel, functionID="12"))
    assert model.egress[(0, "brightness")].validate(0) == 1
    assert model.egress[(0, "brightness")].encode(1) == "1"


# ------------------------------------------------------------------------------- coverage target


def test_typical_fixture_matches_at_least_85_percent_of_channels() -> None:
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "typical.json"
    raw = json.loads(fixture_path.read_text(encoding="utf-8"))
    ((_, config),) = raw.items()

    model = compile(config, REGISTRY, CompileOptions())
    match_rate = model.stats.entities_created / model.stats.channels_total
    assert match_rate >= _TIER1_MATCH_RATE_FLOOR, (
        f"only {match_rate:.1%} of typical.json's {model.stats.channels_total} channels matched "
        f"a tier-1 profile (need >= {_TIER1_MATCH_RATE_FLOOR:.0%}): {model.stats}"
    )
