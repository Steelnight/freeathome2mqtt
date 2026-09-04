"""Round-trip fixtures for the shipped tier-2/tier-3 profiles (docs/03 §9; docs/11 WP11) --
mirrors tests/test_tier1_profiles.py's own structure and helpers.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from freeathome2mqtt.homeassistant.discovery import DiscoveryOptions, build_model_discovery
from freeathome2mqtt.model.compiler import CompileOptions, compile
from freeathome2mqtt.model.profiles import ProfileRegistry, load_profile_registry

PROFILES_DIR = Path(__file__).resolve().parent.parent / "src" / "freeathome2mqtt" / "profiles"
REGISTRY: ProfileRegistry = load_profile_registry(PROFILES_DIR)
_TIER23_MATCH_RATE_FLOOR = 0.85


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


# --------------------------------------------------------------------------------- environment


def test_voc_sensor_round_trip() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 1569, "value": "120"}}}
    model = _compile_one(_config(channel, functionID="bb"))
    entity = model.entities[0]
    assert entity.profile == "voc_sensor"
    assert model.initial_values[0] == (120,)


def test_carbon_monoxide_sensor_round_trip() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 708, "value": "1"}}}
    model = _compile_one(_config(channel, functionID="7e"))
    entity = model.entities[0]
    assert entity.profile == "carbon_monoxide_sensor"
    assert model.initial_values[0] == (True,)


def test_rain_alarm_sensor_round_trip() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 39, "value": "1"}}}
    model = _compile_one(_config(channel, functionID="e"))
    entity = model.entities[0]
    assert entity.profile == "rain_alarm_sensor"
    assert model.initial_values[0] == (True,)


def test_rain_sensor_round_trip() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 1029, "value": "42"}}}
    model = _compile_one(_config(channel, functionID="42"))
    entity = model.entities[0]
    assert entity.profile == "rain_sensor"
    assert model.initial_values[0] == (42,)


def test_wind_alarm_sensor_round_trip() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 37, "value": "0"}}}
    model = _compile_one(_config(channel, functionID="c"))
    entity = model.entities[0]
    assert entity.profile == "wind_alarm_sensor"
    assert model.initial_values[0] == (False,)


def test_wind_sensor_round_trip() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 1028, "value": "3.5"}}}
    model = _compile_one(_config(channel, functionID="44"))
    entity = model.entities[0]
    assert entity.profile == "wind_sensor"
    assert model.initial_values[0] == (3.5,)


def test_blind_sensor_is_event_kind_and_passes_the_raw_value_through() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 32, "value": "1"}}}
    model = _compile_one(_config(channel, functionID="3"))
    entity = model.entities[0]
    assert entity.profile == "blind_sensor"
    binding = model.ingress["ABB700000001/ch0000/odp0000"]
    assert binding.decode("1") == "1"  # codec: string -- verbatim passthrough, no enum guess


def test_dimming_sensor_round_trip() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 16, "value": "5"}}}
    model = _compile_one(_config(channel, functionID="1"))
    assert model.entities[0].profile == "dimming_sensor"


def test_staircase_light_sensor_round_trip() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 1, "value": "1"}}}
    model = _compile_one(_config(channel, functionID="4"))
    assert model.entities[0].profile == "staircase_light_sensor"


def test_force_on_off_sensor_round_trip() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 3, "value": "2"}}}
    model = _compile_one(_config(channel, functionID="5"))
    assert model.entities[0].profile == "force_on_off_sensor"


# ---------------------------------------------------------------------------------------- access


def test_des_door_opener_round_trip() -> None:
    channel = {
        "inputs": {"idp0000": {"pairingID": 5, "value": "0"}},
        "outputs": {"odp0000": {"pairingID": 1545, "value": "0"}},
    }
    model = _compile_one(_config(channel, functionID="1a"))
    entity = model.entities[0]
    assert entity.profile == "des_door_opener"
    assert model.egress[(0, "state")].encode(True) == "1"
    # Not optimistic -- AL_INFO_DOOR reflects physical state, not "a command was sent".
    assert model.egress[(0, "state")].optimistic_attr is None


def test_des_door_ringing_sensor_round_trip() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 1, "value": "1"}}}
    model = _compile_one(_config(channel, functionID="1f"))
    entity = model.entities[0]
    assert entity.profile == "des_door_ringing_sensor"
    binding = model.ingress["ABB700000001/ch0000/odp0000"]
    assert binding.decode("1") is True  # codec: trigger -- the edge is the signal


def test_welcome_ip_mute_round_trip() -> None:
    channel = {
        "inputs": {"idp0000": {"pairingID": 1093, "value": "0"}},
        "outputs": {"odp0000": {"pairingID": 1122, "value": "1"}},
    }
    model = _compile_one(_config(channel, functionID="5c"))
    entity = model.entities[0]
    assert entity.profile == "welcome_ip_mute"
    assert model.initial_values[0] == (True,)
    assert model.egress[(0, "state")].optimistic_attr == 0


# ---------------------------------------------------------------------------------------- energy


def test_energy_inverter_round_trip() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 1215, "value": "1500.5"}}}
    model = _compile_one(_config(channel, functionID="4e"))
    entity = model.entities[0]
    assert entity.profile == "energy_inverter"
    assert model.initial_values[0] == (1500.5,)


def test_energy_battery_round_trip() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 1218, "value": "-250.0"}}}
    model = _compile_one(_config(channel, functionID="50"))
    entity = model.entities[0]
    assert entity.profile == "energy_battery"
    assert model.initial_values[0] == (-250.0,)


def test_energy_meter_round_trip() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 1216, "value": "780.2"}}}
    model = _compile_one(_config(channel, functionID="4f"))
    entity = model.entities[0]
    assert entity.profile == "energy_meter"
    assert model.initial_values[0] == (780.2,)


# -------------------------------------------------------------------------- M-Wire (tier-1 reuse)


def test_mwire_switch_actuator_reuses_switch_actuator_profile() -> None:
    channel = {
        "inputs": {"idp0000": {"pairingID": 1, "value": "0"}},
        "outputs": {"odp0000": {"pairingID": 256, "value": "1"}},
    }
    model = _compile_one(_config(channel, functionID="10e8"))  # FID_MWIRE_SWITCH_OUTPUT_TYPE0
    entity = model.entities[0]
    assert entity.profile == "switch_actuator"
    assert model.initial_values[0] == (True,)


# --------------------------------------------------------- tier-3 virtual device (docs/01 §4.5)


def test_virtual_energy_battery_compiles_when_opted_in() -> None:
    # Virtual devices report the exact same functionIDs a physical device would (docs/01 §4.5), so
    # tier-3's "virtual energy battery" needs no separate profile -- just the existing
    # include_virtual_devices opt-in already covering it, same as any other virtual channel.
    channel = {"outputs": {"odp0000": {"pairingID": 1218, "value": "42.0"}}}
    config = _config(channel, functionID="50", extra_device={"interface": "VD"})
    model = compile(config, REGISTRY, CompileOptions(include_virtual_devices=True))
    assert len(model.entities) == 1
    assert model.entities[0].profile == "energy_battery"


def test_virtual_device_excluded_by_default_even_with_a_tier2_profile_match() -> None:
    channel = {"outputs": {"odp0000": {"pairingID": 1218, "value": "42.0"}}}
    config = _config(channel, functionID="50", extra_device={"interface": "VD"})
    model = compile(config, REGISTRY, CompileOptions())
    assert model.entities == ()


# --------------------------------------------------------------- discovery smoke (docs/04 §6)

_DISCOVERY_CASES = [
    ("bb", {"outputs": {"odp0000": {"pairingID": 1569, "value": "1"}}}, "sensor"),
    ("7e", {"outputs": {"odp0000": {"pairingID": 708, "value": "1"}}}, "binary_sensor"),
    ("3", {"outputs": {"odp0000": {"pairingID": 32, "value": "1"}}}, "event"),
    (
        "1a",
        {
            "inputs": {"idp0000": {"pairingID": 5, "value": "0"}},
            "outputs": {"odp0000": {"pairingID": 1545, "value": "0"}},
        },
        "switch",
    ),
    (
        "5c",
        {
            "inputs": {"idp0000": {"pairingID": 1093, "value": "0"}},
            "outputs": {"odp0000": {"pairingID": 1122, "value": "1"}},
        },
        "switch",
    ),
    ("4e", {"outputs": {"odp0000": {"pairingID": 1215, "value": "1.0"}}}, "sensor"),
]


@pytest.mark.parametrize(("function_id", "channel", "component"), _DISCOVERY_CASES)
def test_new_profiles_build_discovery_without_raising(
    function_id: str, channel: dict[str, Any], component: str
) -> None:
    config = _config(channel, functionID=function_id)
    model = compile(config, REGISTRY, CompileOptions())
    with_discovery = build_model_discovery(model, REGISTRY, config, DiscoveryOptions())
    assert len(with_discovery.discovery) == 1
    topic, _ = with_discovery.discovery[0]
    assert topic.startswith(f"homeassistant/{component}/")


# ------------------------------------------------------------------------------- coverage target


def _load_tier2_tier3_fixture() -> dict[str, Any]:
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "tier2_tier3.json"
    raw = json.loads(fixture_path.read_text(encoding="utf-8"))
    ((_, config),) = raw.items()
    return config  # type: ignore[no-any-return]


def test_tier2_tier3_fixture_matches_at_least_85_percent_of_channels() -> None:
    # docs/11 WP11's acceptance criterion, mirrored from WP4's own test_typical_fixture_... --
    # a purpose-built fixture (not typical.json, which already matched 100% under tier-1 alone
    # and so wouldn't actually exercise any of these new profiles) covering every tier-2 profile
    # plus two genuinely unsupported channels, so the floor is meaningful rather than trivial.
    config = _load_tier2_tier3_fixture()
    model = compile(config, REGISTRY, CompileOptions())
    match_rate = model.stats.entities_created / model.stats.channels_total
    assert match_rate >= _TIER23_MATCH_RATE_FLOOR, (
        f"only {match_rate:.1%} of tier2_tier3.json's {model.stats.channels_total} channels "
        f"matched a profile (need >= {_TIER23_MATCH_RATE_FLOOR:.0%}): {model.stats}"
    )


def test_tier2_tier3_fixture_virtual_devices_also_match_when_opted_in() -> None:
    config = _load_tier2_tier3_fixture()
    model = compile(config, REGISTRY, CompileOptions(include_virtual_devices=True))
    virtual_entities = [e for e in model.entities if e.device_serial.startswith("6000")]
    assert {e.profile for e in virtual_entities} == {"switch_actuator", "energy_battery"}
