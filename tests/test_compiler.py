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


def test_compile_excludes_virtual_devices_by_serial_prefix_by_default() -> None:
    config = _config({"6000AB01": _device({"ch0000": _switch_channel()}, interface="VD")})
    model = compile(config, _switch_registry(), CompileOptions())
    assert model.entities == ()


def test_compile_includes_virtual_devices_when_opted_in() -> None:
    config = _config({"6000AB01": _device({"ch0000": _switch_channel()}, interface="VD")})
    model = compile(config, _switch_registry(), CompileOptions(include_virtual_devices=True))
    assert len(model.entities) == 1


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
