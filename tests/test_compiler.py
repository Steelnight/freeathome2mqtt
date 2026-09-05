"""Tests for model/compiler.py: the pure compile() (docs/03 §4; docs/11 WP3)."""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from freeathome2mqtt.model.compiler import CompileError, CompileOptions, compile
from freeathome2mqtt.model.profiles import build_registry, parse_profile

# ------------------------------------------------------------------------------- fixtures

_SWITCH_PROFILE = {
    "id": "switch_actuator",
    "functions": ["FID_SWITCH_ACTUATOR"],
    "attributes": {"state": {"pairing": "AL_INFO_ON_OFF", "codec": "bool01"}},
    "commands": {
        "state": {"pairing": "AL_SWITCH_ON_OFF", "codec": "bool01", "optimistic": "state"}
    },
}

_DIMMING_PROFILE = {
    "id": "dimming_actuator",
    "functions": ["FID_DIMMING_ACTUATOR"],
    "requires": {"outputs": ["AL_INFO_ON_OFF", "AL_INFO_ACTUAL_DIMMING_VALUE"]},
    "attributes": {
        "state": {"pairing": "AL_INFO_ON_OFF", "codec": "bool01"},
        "brightness": {"pairing": "AL_INFO_ACTUAL_DIMMING_VALUE", "codec": "percent_int"},
        "forced_position": {
            "pairing": "AL_INFO_FORCE",
            "codec": "enum",
            "values": {"0": "deactivated", "4": "forced_on", "5": "forced_off"},
            "default": "unknown",
        },
    },
    "commands": {
        "state": {"pairing": "AL_SWITCH_ON_OFF", "codec": "bool01", "optimistic": "state"},
        "brightness": {
            "pairing": "AL_ABSOLUTE_SET_VALUE_CONTROL",
            "codec": "percent_int",
            "range": [1, 100],
            "continuous": True,
            "optimistic": "brightness",
        },
        "forced_position": {
            "pairing": "AL_FORCED",
            "codec": "enum",
            "values": {"deactivated": "0", "forced_on": "3", "forced_off": "2"},
        },
    },
}


def _switch_registry() -> Any:
    return build_registry([parse_profile(_SWITCH_PROFILE, source="<test>")])


def _dimming_registry() -> Any:
    return build_registry([parse_profile(_DIMMING_PROFILE, source="<test>")])


def _switch_channel(**overrides: Any) -> dict[str, Any]:
    channel: dict[str, Any] = {
        "displayName": "Deckenlicht",
        "functionID": "7",
        "floor": "01",
        "room": "0C",
        "inputs": {"idp0000": {"pairingID": 1, "value": "0"}},
        "outputs": {"odp0000": {"pairingID": 256, "value": "1"}},
    }
    channel.update(overrides)
    return channel


def _device(channels: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    device: dict[str, Any] = {
        "displayName": "Kitchen Ceiling",
        "interface": "TP",
        "floor": "01",
        "room": "0C",
        "channels": channels,
    }
    device.update(overrides)
    return device


def _config(devices: dict[str, Any], **overrides: Any) -> dict[str, Any]:
    config: dict[str, Any] = {
        "floorplan": {
            "floors": {"01": {"name": "Ground Floor", "rooms": {"0C": {"name": "Küche"}}}}
        },
        "devices": devices,
    }
    config.update(overrides)
    return config


# ------------------------------------------------------------------------------- basic compilation


def test_compile_produces_one_entity_for_one_matching_channel() -> None:
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel()})})
    model = compile(config, _switch_registry(), CompileOptions())
    assert len(model.entities) == 1
    entity = model.entities[0]
    assert entity.id == "ABB700990001_ch0000"
    assert entity.profile == "switch_actuator"
    assert entity.name == "Deckenlicht"
    assert entity.area == "Küche"
    assert entity.device_serial == "ABB700990001"
    assert entity.channel_id == "ch0000"
    assert entity.attr_names == ("state",)
    assert entity.idx == 0


def test_compile_entity_transform_defaults_to_none_when_profile_has_none() -> None:
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel()})})
    model = compile(config, _switch_registry(), CompileOptions())
    assert model.entities[0].transform is None


def test_compile_entity_transform_is_populated_from_the_profile() -> None:
    transformed_profile = {**_SWITCH_PROFILE, "transform": "cover_with_slats"}
    registry = build_registry([parse_profile(transformed_profile, source="<test>")])
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel()})})
    model = compile(config, registry, CompileOptions())
    assert model.entities[0].transform == "cover_with_slats"


def test_compile_builds_topics_from_the_resolved_slug() -> None:
    # A single, uncontested channel keeps the bare slug -- naming.py only prefixes the area when
    # the bare name collides with another entity (docs/03 §1.1).
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel()})})
    model = compile(config, _switch_registry(), CompileOptions())
    entity = model.entities[0]
    assert entity.state_topic == "freeathome2mqtt/deckenlicht"
    assert entity.set_topic == "freeathome2mqtt/deckenlicht/set"
    assert entity.get_topic == "freeathome2mqtt/deckenlicht/get"
    assert entity.availability_topic == "freeathome2mqtt/deckenlicht/availability"


def test_compile_honours_a_custom_topic_prefix() -> None:
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel()})})
    model = compile(config, _switch_registry(), CompileOptions(topic_prefix="fh2m"))
    assert model.entities[0].state_topic == "fh2m/deckenlicht"


def test_compile_builds_ingress_and_egress_tables() -> None:
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel()})})
    model = compile(config, _switch_registry(), CompileOptions())
    binding = model.ingress["ABB700990001/ch0000/odp0000"]
    assert binding.entity_idx == 0
    assert binding.attr_idx == 0
    assert binding.attr_bit == 0b1
    assert binding.decode("1") is True

    egress = model.egress[(0, "state")]
    assert egress.rest_path == "ABB700990001.ch0000.idp0000"
    assert egress.encode(True) == "1"
    assert egress.optimistic_attr == 0
    assert egress.continuous is False
    assert egress.confirm is True


def test_compile_carries_confirm_false_through_to_the_egress_binding() -> None:
    # P-19: not every channel type echoes; a profile author opts a command out of reconciliation
    # entirely via `confirm: false` (docs/03 §3.3), and that must survive compilation.
    profile = {
        **_SWITCH_PROFILE,
        "commands": {
            "state": {
                "pairing": "AL_SWITCH_ON_OFF",
                "codec": "bool01",
                "optimistic": "state",
                "confirm": False,
            }
        },
    }
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel()})})
    registry = build_registry([parse_profile(profile, source="<test>")])
    model = compile(config, registry, CompileOptions())
    assert model.egress[(0, "state")].confirm is False


def test_compile_seeds_initial_values_from_the_snapshot() -> None:
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel()})})
    model = compile(config, _switch_registry(), CompileOptions())
    assert model.initial_values == ((True,),)


def test_compile_builds_by_id_and_by_topic() -> None:
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel()})})
    model = compile(config, _switch_registry(), CompileOptions())
    assert model.by_id["ABB700990001_ch0000"] == 0
    assert model.by_topic["deckenlicht"] == 0


def test_compile_no_matching_profile_is_skipped_and_counted() -> None:
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel(functionID="18")})})
    model = compile(config, _switch_registry(), CompileOptions())
    assert model.entities == ()
    assert model.stats.channels_unsupported == 1


def test_compile_empty_installation_produces_an_empty_but_valid_model() -> None:
    model = compile(_config({}), _switch_registry(), CompileOptions())
    assert model.entities == ()
    assert model.ingress == {}
    assert model.egress == {}
    assert model.stats.devices_total == 0


# ------------------------------------------------------------------------------- floor/room (P-14)


def test_compile_channel_floor_room_takes_priority_over_device() -> None:
    config = _config(
        {
            "ABB700990001": _device(
                {"ch0000": _switch_channel(floor="01", room="0C")}, floor="01", room="0D"
            )
        },
        floorplan={
            "floors": {
                "01": {"name": "GF", "rooms": {"0C": {"name": "Küche"}, "0D": {"name": "Büro"}}}
            }
        },
    )
    model = compile(config, _switch_registry(), CompileOptions())
    assert model.entities[0].area == "Küche"


def test_compile_channel_falls_back_to_device_floor_room() -> None:
    channel = _switch_channel()
    del channel["floor"]
    del channel["room"]
    config = _config({"ABB700990001": _device({"ch0000": channel}, floor="01", room="0C")})
    model = compile(config, _switch_registry(), CompileOptions())
    assert model.entities[0].area == "Küche"


def test_orphan_filter_counts_reported() -> None:
    # P-18's named test (docs/09): excluded orphans are counted, not just silently dropped.
    channel = _switch_channel()
    del channel["floor"]
    del channel["room"]
    device = _device({"ch0000": channel})
    del device["floor"]
    del device["room"]
    config = _config({"ABB700990001": device})
    model = compile(config, _switch_registry(), CompileOptions())
    assert model.entities == ()
    assert model.stats.channels_orphaned == 1


def test_compile_orphan_channel_included_when_option_set() -> None:
    channel = _switch_channel()
    del channel["floor"]
    del channel["room"]
    device = _device({"ch0000": channel})
    del device["floor"]
    del device["room"]
    config = _config({"ABB700990001": device})
    model = compile(config, _switch_registry(), CompileOptions(include_orphan_channels=True))
    assert len(model.entities) == 1
    assert model.entities[0].area is None


def test_floorplan_null_rooms() -> None:
    # P-14's named test (docs/09): floorplan.floors[x].rooms is null, not just absent.
    channel = _switch_channel(floor="02", room="05")
    config = _config(
        {"ABB700990001": _device({"ch0000": channel}, floor="02", room="05")},
        floorplan={
            "floors": {"01": {"name": "GF", "rooms": {}}, "02": {"name": "1F", "rooms": None}}
        },
    )
    model = compile(config, _switch_registry(), CompileOptions())
    assert len(model.entities) == 1
    assert model.entities[0].area is None


# ------------------------------------------------------------------------------- names (P-15)


def test_placeholder_channel_names_fall_back() -> None:
    # P-15's named test (docs/09).
    channel = _switch_channel(displayName="Ⓐ")  # circled A
    config = _config({"ABB700990001": _device({"ch0000": channel}, displayName="Sensor Module")})
    model = compile(config, _switch_registry(), CompileOptions())
    assert model.entities[0].name == "Sensor Module"


def test_compile_null_channel_name_falls_back_to_device_name() -> None:
    channel = _switch_channel(displayName=None)
    config = _config({"ABB700990001": _device({"ch0000": channel}, displayName="Sensor Module")})
    model = compile(config, _switch_registry(), CompileOptions())
    assert model.entities[0].name == "Sensor Module"


def test_compile_placeholder_names_fall_back_to_channel_id_as_a_last_resort() -> None:
    channel = _switch_channel(displayName=None)
    config = _config({"ABB700990001": _device({"ch0000": channel}, displayName=None)})
    model = compile(config, _switch_registry(), CompileOptions())
    assert model.entities[0].name == "Channel ch0000"


# --------------------------------------------------------------------------- function ID resolution


def test_compile_missing_function_id_is_skipped_and_counted() -> None:
    channel = _switch_channel()
    del channel["functionID"]
    config = _config({"ABB700990001": _device({"ch0000": channel})})
    model = compile(config, _switch_registry(), CompileOptions())
    assert model.entities == ()
    assert model.stats.channels_unknown_function == 1


def test_compile_garbled_function_id_is_skipped_and_counted() -> None:
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel(functionID="ZZ")})})
    model = compile(config, _switch_registry(), CompileOptions())
    assert model.stats.channels_unknown_function == 1


def test_compile_unrecognised_function_id_is_skipped_and_counted() -> None:
    # Valid hex, but no such Function member (P-06's "int(v) instead of int(v, 16)" trap ruled
    # out separately in sysap.schema; this is the "parses fine, no matching member" case).
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel(functionID="1234")})})
    model = compile(config, _switch_registry(), CompileOptions())
    assert model.stats.channels_unknown_function == 1


def test_compile_logs_each_distinct_unknown_function_id_once(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # docs/03 §3.4 step 1: log once per distinct function ID, not once per channel.
    config = _config(
        {
            "ABB700990001": _device({"ch0000": _switch_channel(functionID="1234")}),
            "ABB700990002": _device({"ch0000": _switch_channel(functionID="1234")}),
        }
    )
    with caplog.at_level("INFO"):
        model = compile(config, _switch_registry(), CompileOptions())
    assert model.stats.channels_unknown_function == 2
    assert sum("unsupported function ID" in message for message in caplog.messages) == 1


# ------------------------------------------------------------- interface / virtual device filtering


def test_compile_excludes_hue_devices_by_default() -> None:
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel()}, interface="hue")})
    model = compile(config, _switch_registry(), CompileOptions())
    assert model.entities == ()
    assert model.stats.devices_excluded_by_interface == 1


def test_default_interface_filter_excludes_hue_sonos() -> None:
    # docs/09 P-17's named test: a native Hue or Sonos integration double-bridging the same
    # lamp/speaker as this bridge causes state ping-pong (each sees the other's change as
    # external and re-asserts) -- CompileOptions.excluded_interfaces defaults to both.
    config = _config(
        {
            "ABB700990001": _device({"ch0000": _switch_channel()}, interface="hue"),
            "ABB700990002": _device({"ch0000": _switch_channel()}, interface="sonos"),
        }
    )
    model = compile(config, _switch_registry(), CompileOptions())
    assert model.entities == ()
    assert model.stats.devices_excluded_by_interface == 2


def test_compile_excludes_virtual_devices_by_serial_prefix_by_default() -> None:
    config = _config({"6000AB01": _device({"ch0000": _switch_channel()}, interface="VD")})
    model = compile(config, _switch_registry(), CompileOptions())
    assert model.entities == ()


def test_compile_includes_virtual_devices_when_opted_in() -> None:
    config = _config({"6000AB01": _device({"ch0000": _switch_channel()}, interface="VD")})
    model = compile(config, _switch_registry(), CompileOptions(include_virtual_devices=True))
    assert len(model.entities) == 1


def test_compile_excludes_entity_ids_in_excluded_entity_ids() -> None:
    """entity/remove and entity/options {"enabled": false} (docs/04 §5, docs/11 WP9) flow into
    `excluded_entity_ids`, symmetric with `aliases` -- the entity simply never exists in the next
    compiled `Model`, so the ordinary removed-entity retraction path (P-35) handles the rest.
    """
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel()})})
    model = compile(
        config,
        _switch_registry(),
        CompileOptions(excluded_entity_ids=frozenset({"ABB700990001_ch0000"})),
    )
    assert model.entities == ()
    assert model.stats.channels_excluded_by_option == 1


def test_compile_includes_undefined_interface_by_default() -> None:
    device = _device({"ch0000": _switch_channel()})
    del device["interface"]
    config = _config({"ABB700990001": device})
    model = compile(config, _switch_registry(), CompileOptions())
    assert len(model.entities) == 1


# ------------------------------------------------------------------------------- P-01/P-02


def test_compiler_rejects_attribute_bound_to_input() -> None:
    bad_profile = {
        "id": "bad_switch",
        "functions": ["FID_SWITCH_ACTUATOR"],
        "attributes": {"state": {"pairing": "AL_SWITCH_ON_OFF", "codec": "bool01"}},
    }
    registry = build_registry([parse_profile(bad_profile, source="<test>")])
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel()})})
    with pytest.raises(CompileError, match="P-01"):
        compile(config, registry, CompileOptions())


def test_compiler_rejects_command_bound_to_output() -> None:
    bad_profile = {
        "id": "bad_switch",
        "functions": ["FID_SWITCH_ACTUATOR"],
        "attributes": {"state": {"pairing": "AL_INFO_ON_OFF", "codec": "bool01"}},
        "commands": {"state": {"pairing": "AL_INFO_ON_OFF", "codec": "bool01"}},
    }
    registry = build_registry([parse_profile(bad_profile, source="<test>")])
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel()})})
    with pytest.raises(CompileError, match="P-02"):
        compile(config, registry, CompileOptions())


# --------------------------------------------------------------------- optional attributes/commands


def test_compile_drops_an_attribute_whose_datapoint_is_absent() -> None:
    channel = _switch_channel(functionID="12")  # hex for FID_DIMMING_ACTUATOR = 18
    channel["outputs"] = {
        "odp0000": {"pairingID": 256, "value": "1"}
    }  # no AL_INFO_ACTUAL_DIMMING_VALUE
    config = _config({"ABB700990001": _device({"ch0000": channel})})
    model = compile(config, _dimming_registry(), CompileOptions())
    assert model.entities == ()
    assert model.stats.channels_unsupported == 1


def test_compile_drops_a_non_required_attribute_but_keeps_the_entity() -> None:
    optional_dimming = {
        "id": "dimming_soft",
        "functions": ["FID_DIMMING_ACTUATOR"],
        "attributes": {
            "state": {"pairing": "AL_INFO_ON_OFF", "codec": "bool01"},
            "brightness": {"pairing": "AL_INFO_ACTUAL_DIMMING_VALUE", "codec": "percent_int"},
        },
    }
    registry = build_registry([parse_profile(optional_dimming, source="<test>")])
    channel = _switch_channel(functionID="12")  # hex for FID_DIMMING_ACTUATOR = 18
    channel["outputs"] = {"odp0000": {"pairingID": 256, "value": "1"}}
    config = _config({"ABB700990001": _device({"ch0000": channel})})
    model = compile(config, registry, CompileOptions())
    assert len(model.entities) == 1
    assert model.entities[0].attr_names == ("state",)


def test_compile_drops_a_non_required_command_but_keeps_the_entity() -> None:
    profile = {
        "id": "switch_with_optional_command",
        "functions": ["FID_SWITCH_ACTUATOR"],
        "attributes": {"state": {"pairing": "AL_INFO_ON_OFF", "codec": "bool01"}},
        "commands": {
            "state": {"pairing": "AL_SWITCH_ON_OFF", "codec": "bool01"},
            "extra": {"pairing": "AL_FORCED", "codec": "bool01"},  # absent from this channel
        },
    }
    registry = build_registry([parse_profile(profile, source="<test>")])
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel()})})
    model = compile(config, registry, CompileOptions())
    assert len(model.entities) == 1
    assert (0, "state") in model.egress
    assert (0, "extra") not in model.egress


def test_compile_command_without_a_range_validates_as_the_identity() -> None:
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel()})})
    model = compile(config, _switch_registry(), CompileOptions())
    assert model.egress[(0, "state")].validate(True) is True
    assert model.egress[(0, "state")].validate(150) == 150


def test_compile_channel_name_of_only_whitespace_falls_back_to_device_name() -> None:
    channel = _switch_channel(displayName="   ")
    config = _config({"ABB700990001": _device({"ch0000": channel}, displayName="Sensor Module")})
    model = compile(config, _switch_registry(), CompileOptions())
    assert model.entities[0].name == "Sensor Module"


def test_compile_dimming_profile_exercises_enum_range_and_precision_codecs() -> None:
    # A fully-provisioned dimming channel: every attribute/command datapoint present, so this is
    # the one test that walks the enum and range-clamped and precision-forwarding codec paths that
    # the partial fixtures elsewhere (deliberately) never reach.
    profile = {
        "id": "dimming_full",
        "functions": ["FID_DIMMING_ACTUATOR"],
        "attributes": {
            "state": {"pairing": "AL_INFO_ON_OFF", "codec": "bool01"},
            "brightness": {"pairing": "AL_INFO_ACTUAL_DIMMING_VALUE", "codec": "percent_int"},
            "forced_position": {
                "pairing": "AL_INFO_FORCE",
                "codec": "enum",
                "values": {"0": "deactivated", "4": "forced_on", "5": "forced_off"},
                "default": "unknown",
            },
            "temperature": {"pairing": "AL_MEASURED_TEMPERATURE", "codec": "float", "precision": 1},
        },
        "commands": {
            "state": {"pairing": "AL_SWITCH_ON_OFF", "codec": "bool01"},
            "brightness": {
                "pairing": "AL_ABSOLUTE_SET_VALUE_CONTROL",
                "codec": "percent_int",
                "range": [1, 100],
            },
            "forced_position": {
                "pairing": "AL_FORCED",
                "codec": "enum",
                "values": {"deactivated": "0", "forced_on": "3", "forced_off": "2"},
            },
        },
    }
    registry = build_registry([parse_profile(profile, source="<test>")])
    channel = {
        "displayName": "Deckenlicht",
        "functionID": "12",  # hex for FID_DIMMING_ACTUATOR = 18
        "floor": "01",
        "room": "0C",
        "inputs": {
            "idp0000": {"pairingID": 1, "value": "0"},  # AL_SWITCH_ON_OFF
            "idp0001": {"pairingID": 17, "value": "50"},  # AL_ABSOLUTE_SET_VALUE_CONTROL
            "idp0002": {"pairingID": 3, "value": "0"},  # AL_FORCED
        },
        "outputs": {
            "odp0000": {"pairingID": 256, "value": "1"},  # AL_INFO_ON_OFF
            "odp0001": {"pairingID": 272, "value": "60"},  # AL_INFO_ACTUAL_DIMMING_VALUE
            "odp0002": {"pairingID": 257, "value": "4"},  # AL_INFO_FORCE
            "odp0003": {"pairingID": 304, "value": "21.456"},  # AL_MEASURED_TEMPERATURE
        },
    }
    config = _config({"ABB700990099": _device({"ch0000": channel})})
    model = compile(config, registry, CompileOptions())

    assert len(model.entities) == 1
    entity = model.entities[0]
    assert set(entity.attr_names) == {"state", "brightness", "forced_position", "temperature"}

    values = dict(zip(entity.attr_names, model.initial_values[0], strict=True))
    assert values["forced_position"] == "forced_on"
    assert values["temperature"] == 21.5  # rounded to precision=1

    assert model.egress[(0, "forced_position")].encode("forced_on") == "3"
    assert model.egress[(0, "brightness")].validate(150) == 100  # clamped to range [1, 100]
    assert model.egress[(0, "brightness")].validate(0) == 1


# --------------------------------------------------------------- color_temp_pct channel parameters


def _color_temp_profile(**overrides: Any) -> dict[str, Any]:
    profile: dict[str, Any] = {
        "id": "colour_temperature_actuator",
        "functions": ["FID_DIMMING_ACTUATOR"],
        "attributes": {
            "color_temp": {"pairing": "AL_INFO_COLOR_TEMPERATURE", "codec": "color_temp_pct"},
        },
        "commands": {
            "color_temp": {"pairing": "AL_COLOR_TEMPERATURE", "codec": "color_temp_pct"},
        },
        "parameters": {
            "color_temp_warmest": "PID_TEMPERATURE_COLOR_PHYSICAL_WARMEST",
            "color_temp_coolest": "PID_TEMPERATURE_COLOR_PHYSICAL_COOLEST",
        },
    }
    profile.update(overrides)
    return profile


def _color_temp_channel(**overrides: Any) -> dict[str, Any]:
    channel: dict[str, Any] = {
        "displayName": "Deckenlicht",
        "functionID": "12",  # hex for FID_DIMMING_ACTUATOR = 18
        "floor": "01",
        "room": "0C",
        "inputs": {"idp0000": {"pairingID": 22, "value": "50"}},  # AL_COLOR_TEMPERATURE
        "outputs": {"odp0000": {"pairingID": 280, "value": "50"}},  # AL_INFO_COLOR_TEMPERATURE
        "parameters": {"par00f6": "2700", "par00f5": "6500"},  # WARMEST=0xf6, COOLEST=0xf5
    }
    channel.update(overrides)
    return channel


def test_color_temp_uses_channel_parameters() -> None:
    # P-09's named test: never hardcode 2700-6500K -- these channel parameters (2000/4000) are
    # deliberately different from the common defaults, so a hardcoded-bounds bug would fail this.
    registry = build_registry([parse_profile(_color_temp_profile(), source="<test>")])
    channel = _color_temp_channel(parameters={"par00f6": "2000", "par00f5": "4000"})
    config = _config({"ABB700990099": _device({"ch0000": channel})})
    model = compile(config, registry, CompileOptions())

    assert len(model.entities) == 1
    assert model.initial_values[0][0] == 3000  # 50% between 2000 and 4000
    assert model.egress[(0, "color_temp")].encode(2000) == "0"
    assert model.egress[(0, "color_temp")].encode(4000) == "100"


def test_color_temp_pct_channel_parameter_key_case_and_width_are_tolerant() -> None:
    # Real installations may not zero-pad or lowercase parameter keys identically; the lookup
    # matches by parsed hex value, not by exact key string.
    registry = build_registry([parse_profile(_color_temp_profile(), source="<test>")])
    channel = _color_temp_channel(parameters={"PAR00F6": "2700", "par0F5": "6500"})
    config = _config({"ABB700990099": _device({"ch0000": channel})})
    model = compile(config, registry, CompileOptions())
    assert model.initial_values[0][0] == 4600  # 50% between 2700 and 6500


def test_compiler_rejects_color_temp_pct_without_declared_parameters() -> None:
    # A profile-authoring bug (static -- would fail identically for every channel), not a
    # per-installation quirk: fail loudly at compile time, not silently at runtime.
    bad_profile = _color_temp_profile(parameters={})
    registry = build_registry([parse_profile(bad_profile, source="<test>")])
    config = _config({"ABB700990099": _device({"ch0000": _color_temp_channel()})})
    with pytest.raises(CompileError, match="color_temp_pct"):
        compile(config, registry, CompileOptions())


def test_compile_color_temp_pct_missing_channel_parameter_drops_the_attribute() -> None:
    # The profile correctly declares which parameters to use; this specific channel just doesn't
    # have them set -- not a static bug, so the attribute is dropped rather than crashing the
    # whole compile (same policy as an absent, non-required datapoint).
    profile = _color_temp_profile()
    profile["attributes"]["state"] = {"pairing": "AL_INFO_ON_OFF", "codec": "bool01"}
    registry = build_registry([parse_profile(profile, source="<test>")])
    channel = _color_temp_channel(parameters={})
    channel["outputs"]["odp0001"] = {"pairingID": 256, "value": "1"}  # AL_INFO_ON_OFF
    config = _config({"ABB700990099": _device({"ch0000": channel})})
    model = compile(config, registry, CompileOptions())

    assert len(model.entities) == 1
    assert model.entities[0].attr_names == ("state",)
    assert (0, "color_temp") not in model.egress


def test_compile_ignores_a_malformed_parameter_key() -> None:
    # A channel parameter key that isn't valid hex (garbled, like the functionID case P-06 also
    # guards against) is skipped, not treated as a crash or a false match. Uppercase-prefixed so
    # it sorts before the real "par00f5"/"par00f6" keys and is actually visited first.
    registry = build_registry([parse_profile(_color_temp_profile(), source="<test>")])
    channel = _color_temp_channel(
        parameters={"PARZZZZ": "9999", "par00f6": "2700", "par00f5": "6500"}
    )
    config = _config({"ABB700990099": _device({"ch0000": channel})})
    model = compile(config, registry, CompileOptions())
    assert model.initial_values[0][0] == 4600  # unaffected by the garbled key


# -------------------------------------------------------------------------- profile matching (§3.4)


def test_compile_picks_the_most_specific_profile_when_several_match() -> None:
    generic = {
        "id": "zzz_generic",
        "functions": ["FID_SWITCH_ACTUATOR"],
        "attributes": {"state": {"pairing": "AL_INFO_ON_OFF", "codec": "bool01"}},
    }
    specific = {
        "id": "aaa_specific",
        "functions": ["FID_SWITCH_ACTUATOR"],
        "requires": {"outputs": ["AL_INFO_ON_OFF"]},
        "attributes": {"state": {"pairing": "AL_INFO_ON_OFF", "codec": "bool01"}},
    }
    registry = build_registry(
        [parse_profile(generic, source="<test>"), parse_profile(specific, source="<test>")]
    )
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel()})})
    model = compile(config, registry, CompileOptions())
    assert model.entities[0].profile == "aaa_specific"


def test_compile_tie_break_uses_the_lower_profile_id_and_is_counted() -> None:
    zzz = {
        "id": "zzz",
        "functions": ["FID_SWITCH_ACTUATOR"],
        "attributes": {"state": {"pairing": "AL_INFO_ON_OFF", "codec": "bool01"}},
    }
    aaa = {
        "id": "aaa",
        "functions": ["FID_SWITCH_ACTUATOR"],
        "attributes": {"state": {"pairing": "AL_INFO_ON_OFF", "codec": "bool01"}},
    }
    registry = build_registry(
        [parse_profile(zzz, source="<test>"), parse_profile(aaa, source="<test>")]
    )
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel()})})
    model = compile(config, registry, CompileOptions())
    assert model.entities[0].profile == "aaa"
    assert model.stats.channels_ambiguous_profile == 1


# ------------------------------------------------------------------------- naming collisions (P-39)


def test_compile_resolves_name_collisions_across_channels() -> None:
    config = _config(
        {
            "ABB700990001": _device({"ch0000": _switch_channel()}, floor="01", room="0C"),
            "ABB700990002": _device(
                {"ch0000": _switch_channel(floor="01", room="0D")}, floor="01", room="0D"
            ),
        },
        floorplan={
            "floors": {
                "01": {"name": "GF", "rooms": {"0C": {"name": "Küche"}, "0D": {"name": "Büro"}}}
            }
        },
    )
    model = compile(config, _switch_registry(), CompileOptions())
    slugs = {e.id: e.state_topic.rsplit("/", 1)[-1] for e in model.entities}
    assert slugs["ABB700990001_ch0000"] == "kueche_deckenlicht"
    assert slugs["ABB700990002_ch0000"] == "buero_deckenlicht"


def test_compile_topic_style_id_uses_the_slugified_entity_id() -> None:
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel()})})
    model = compile(config, _switch_registry(), CompileOptions(topic_style="id"))
    assert model.entities[0].state_topic == "freeathome2mqtt/abb700990001_ch0000"


def test_compile_alias_overrides_the_channel_name_for_slugging() -> None:
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel()})})
    options = CompileOptions(aliases={"ABB700990001_ch0000": "My Custom Name"})
    model = compile(config, _switch_registry(), options)
    assert model.entities[0].state_topic == "freeathome2mqtt/my_custom_name"
    # The alias only affects the topic slug, never the channel's own display name.
    assert model.entities[0].name == "Deckenlicht"


# ------------------------------------------------------------------------------- determinism


def test_compiler_is_deterministic() -> None:
    # P-54's named test (docs/09): compile twice from shuffled inputs, compare byte-for-byte.
    config = _config(
        {
            "ABB700990002": _device({"ch0000": _switch_channel(floor="01", room="0C")}),
            "ABB700990001": _device({"ch0000": _switch_channel()}),
        }
    )
    reordered = json.loads(json.dumps(config))  # fresh dict, same content
    reordered_devices = dict(reversed(list(reordered["devices"].items())))
    reordered["devices"] = reordered_devices

    model_a = compile(copy.deepcopy(config), _switch_registry(), CompileOptions())
    model_b = compile(copy.deepcopy(reordered), _switch_registry(), CompileOptions())

    assert model_a.entities == model_a.entities  # sanity: comparable at all
    ids_a = [e.id for e in model_a.entities]
    ids_b = [e.id for e in model_b.entities]
    assert ids_a == ids_b
    assert model_a.by_id == model_b.by_id
    assert model_a.by_topic == model_b.by_topic


def test_compile_does_not_mutate_the_input_config() -> None:
    config = _config({"ABB700990001": _device({"ch0000": _switch_channel()})})
    before = copy.deepcopy(config)
    compile(config, _switch_registry(), CompileOptions())
    assert config == before


@given(
    # A small, repetition-heavy alphabet so name collisions -- P-39/P-54's actual failure mode --
    # are common, not a rare corner the property test might never reach.
    names=st.lists(
        st.sampled_from(["Deckenlicht", "Steckdose", "Lampe", "Schalter"]), min_size=1, max_size=10
    )
)
def test_compiler_is_deterministic_under_shuffled_input(names: list[str]) -> None:
    devices = {
        f"DEV{i:04d}": _device({"ch0000": _switch_channel(displayName=name)})
        for i, name in enumerate(names)
    }
    registry = _switch_registry()

    forward = compile(_config(dict(devices)), registry, CompileOptions())
    backward = compile(_config(dict(reversed(list(devices.items())))), registry, CompileOptions())

    assert [e.id for e in forward.entities] == [e.id for e in backward.entities]
    assert forward.by_id == backward.by_id
    assert forward.by_topic == backward.by_topic
    assert forward.stats == backward.stats


# ------------------------------------------------------------------------------- real fixture


def test_compile_nasty_fixture_produces_no_exception() -> None:
    raw = json.loads(Path("tests/fixtures/nasty.json").read_text(encoding="utf-8"))
    ((_, config),) = raw.items()
    model = compile(config, _switch_registry(), CompileOptions())
    assert model.stats.channels_total > 0
    assert model.stats.channels_unknown_function >= 1
    assert model.stats.channels_orphaned >= 1


def test_compile_nasty_fixture_is_deterministic() -> None:
    raw = json.loads(Path("tests/fixtures/nasty.json").read_text(encoding="utf-8"))
    ((_, config),) = raw.items()
    model_a = compile(copy.deepcopy(config), _switch_registry(), CompileOptions())
    model_b = compile(copy.deepcopy(config), _switch_registry(), CompileOptions())
    assert [e.id for e in model_a.entities] == [e.id for e in model_b.entities]
    assert model_a.stats == model_b.stats


# ------------------------------------- WP16: entities.exclude / entities.include (docs/07 §2)


def _two_switches() -> dict[str, Any]:
    return _config(
        {
            "ABB700990001": _device({"ch0000": _switch_channel()}),
            "ABB700990002": _device({"ch0000": _switch_channel()}),
        }
    )


def test_exclude_patterns_drop_matching_entities() -> None:
    """`entities.exclude` (docs/07 §2) -- accepted and validated since WP9, enforced from WP16."""
    model = compile(
        _two_switches(),
        _switch_registry(),
        CompileOptions(exclude_patterns=("ABB700990002_*",)),
    )
    assert [e.id for e in model.entities] == ["ABB700990001_ch0000"]


def test_exclude_matches_an_exact_id_without_wildcards() -> None:
    model = compile(
        _two_switches(),
        _switch_registry(),
        CompileOptions(exclude_patterns=("ABB700990001_ch0000",)),
    )
    assert [e.id for e in model.entities] == ["ABB700990002_ch0000"]


def test_include_is_an_allowlist_applied_after_exclude() -> None:
    """docs/07 §2: "if non-empty, an allowlist applied after exclude"."""
    model = compile(
        _two_switches(),
        _switch_registry(),
        CompileOptions(include_patterns=("ABB700990001_*",)),
    )
    assert [e.id for e in model.entities] == ["ABB700990001_ch0000"]


def test_exclude_wins_over_include_for_the_same_entity() -> None:
    """ "Applied after exclude" settles the overlap: an entity named by both is excluded."""
    model = compile(
        _two_switches(),
        _switch_registry(),
        CompileOptions(exclude_patterns=("ABB700990001_*",), include_patterns=("ABB70099*",)),
    )
    assert [e.id for e in model.entities] == ["ABB700990002_ch0000"]


def test_an_empty_include_list_excludes_nothing() -> None:
    """The empty-allowlist trap: `include: []` is the *default*, and must not mean "allow none"."""
    model = compile(_two_switches(), _switch_registry(), CompileOptions(include_patterns=()))
    assert len(model.entities) == 2


def test_pattern_excluded_channels_are_tallied_like_other_option_exclusions() -> None:
    """docs/03 §3.4: exclusions are counted so `bridge/info` can show them, never silent."""
    model = compile(
        _two_switches(),
        _switch_registry(),
        CompileOptions(exclude_patterns=("ABB700990002_*",)),
    )
    assert model.stats.channels_excluded_by_option == 1


def test_exclude_patterns_match_the_stable_id_not_the_renameable_topic() -> None:
    """Patterns match the entity id (docs/07 §2: "entity ids or glob patterns"), deliberately.

    The topic segment is a slugified friendly name that `bridge/request/entity/rename` can change
    at runtime (ADR-010); the id never changes. Matching topics would mean a rename could silently
    add or drop an entity from the exclusion set, which is exactly the failure ADR-010 introduced
    stable ids to avoid.
    """
    model = compile(
        _two_switches(),
        _switch_registry(),
        CompileOptions(exclude_patterns=("*deckenlicht*",)),
    )
    assert len(model.entities) == 2
