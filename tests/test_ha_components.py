"""Tests for homeassistant/components.py: per-HA-platform discovery payload builders (docs/04
§6.2; docs/11 WP10).

Each real profile's channel fixture is reused from `tests/test_tier1_profiles.py` (the same
compiled-against-the-real-registry style) rather than re-derived, so these tests exercise the
same `Entity`/`Profile`/`Channel`/`Device` shapes the real pipeline produces.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path
from typing import Any

import pytest

from freeathome2mqtt.homeassistant.components import (
    COMPONENT_BUILDERS,
    ComponentContext,
    DiscoveryError,
    DiscoveryOptions,
    build_binary_sensor,
    build_climate,
    build_cover,
    build_event,
    build_light,
    build_number,
    build_sensor,
    build_switch,
)
from freeathome2mqtt.model.compiler import CompileOptions, Model, compile
from freeathome2mqtt.model.profiles import ProfileRegistry, load_profile_registry

PROFILES_DIR = Path(__file__).resolve().parent.parent / "src" / "freeathome2mqtt" / "profiles"
REGISTRY: ProfileRegistry = load_profile_registry(PROFILES_DIR)
DEVICE_SERIAL = "ABB700000001"


def _config(
    channel: dict[str, Any], *, functionID: str, extra_device: dict[str, Any] | None = None
) -> dict[str, Any]:
    device: dict[str, Any] = {
        "displayName": "Test Device",
        "deviceId": "910C",
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
        "devices": {DEVICE_SERIAL: device},
    }


def _compile_one(config: dict[str, Any]) -> Model:
    model = compile(config, REGISTRY, CompileOptions())
    assert len(model.entities) == 1, model.stats
    return model


def _context(
    config: dict[str, Any], model: Model, *, options: DiscoveryOptions | None = None
) -> ComponentContext:
    entity = model.entities[0]
    profile = REGISTRY[entity.profile]
    channel = config["devices"][entity.device_serial]["channels"][entity.channel_id]
    device = config["devices"][entity.device_serial]
    commands = frozenset(name for idx, name in model.egress if idx == entity.idx)
    return ComponentContext(
        entity=entity,
        profile=profile,
        channel=channel,
        device=device,
        commands=commands,
        options=options or DiscoveryOptions(),
    )


# ------------------------------------------------------------------------------------ switch


def test_build_switch_uses_scalar_shorthand_topics() -> None:
    config = _config(
        {
            "inputs": {"idp0000": {"pairingID": 1, "value": "0"}},
            "outputs": {"odp0000": {"pairingID": 256, "value": "1"}},
        },
        functionID="7",
    )
    ctx = _context(config, _compile_one(config))
    payload = build_switch(ctx)
    assert payload["state_topic"] == ctx.entity.state_topic
    assert payload["command_topic"] == ctx.entity.set_topic
    assert payload["value_template"] == "{{ 'ON' if value_json.state else 'OFF' }}"
    assert payload["payload_on"] == "ON"
    assert payload["payload_off"] == "OFF"


# ------------------------------------------------------------------------------------- light


def test_build_light_dimming_only_has_brightness_not_color_temp() -> None:
    config = _config(
        {
            "inputs": {
                "idp0000": {"pairingID": 1, "value": "1"},
                "idp0001": {"pairingID": 17, "value": "50"},
            },
            "outputs": {
                "odp0000": {"pairingID": 256, "value": "1"},
                "odp0001": {"pairingID": 272, "value": "50"},
            },
        },
        functionID="12",
    )
    ctx = _context(config, _compile_one(config))
    payload = build_light(ctx)
    assert payload["schema"] == "json"
    assert payload["brightness"] is True
    assert payload["brightness_scale"] == 100
    assert payload["supported_color_modes"] == ["brightness"]
    assert "color_temp_kelvin" not in payload


def test_build_light_raises_when_profile_lacks_color_temp_parameter_declarations() -> None:
    config = _config(
        {
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
        },
        functionID="12",
    )
    ctx = _context(config, _compile_one(config))
    broken_profile = dataclasses.replace(ctx.profile, parameters={})
    broken_ctx = dataclasses.replace(ctx, profile=broken_profile)
    with pytest.raises(DiscoveryError, match="color_temp_warmest"):
        build_light(broken_ctx)


def test_build_light_raises_when_channel_parameters_are_malformed_or_absent() -> None:
    config = _config(
        {
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
        },
        functionID="12",
    )
    ctx = _context(config, _compile_one(config))
    # A malformed key exercises _find_parameter_value's hex-tolerant except/continue branch too.
    broken_channel = {**ctx.channel, "parameters": {"parZZZZ": "garbage"}}
    broken_ctx = dataclasses.replace(ctx, channel=broken_channel)
    with pytest.raises(DiscoveryError, match="channel parameters are absent"):
        build_light(broken_ctx)


def test_build_light_colour_temperature_reports_channel_kelvin_bounds() -> None:
    config = _config(
        {
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
        },
        functionID="12",
    )
    ctx = _context(config, _compile_one(config))
    payload = build_light(ctx)
    assert payload["color_temp_kelvin"] is True
    assert payload["min_kelvin"] == 2700
    assert payload["max_kelvin"] == 6500
    assert payload["supported_color_modes"] == ["color_temp"]


# ------------------------------------------------------------------------------------- cover


def test_build_cover_plain_has_position_and_stop_but_no_tilt() -> None:
    config = _config(
        {
            "inputs": {"idp0000": {"pairingID": 35, "value": "0"}},
            "outputs": {"odp0000": {"pairingID": 289, "value": "0"}},
        },
        functionID="9",
    )
    ctx = _context(config, _compile_one(config))
    payload = build_cover(ctx)
    assert payload["position_topic"] == ctx.entity.state_topic
    assert payload["position_template"] == "{{ value_json.position }}"
    assert payload["set_position_topic"] == f"{ctx.entity.state_topic}/set/position"
    assert "tilt_status_topic" not in payload
    assert "command_topic" not in payload  # no stop command declared on this fixture


def test_build_cover_position_attribute_without_a_command_omits_set_position() -> None:
    config = _config(
        {
            "inputs": {"idp0000": {"pairingID": 35, "value": "0"}},
            "outputs": {"odp0000": {"pairingID": 289, "value": "0"}},
        },
        functionID="9",
    )
    ctx = _context(config, _compile_one(config))
    no_commands_ctx = dataclasses.replace(ctx, commands=frozenset())
    payload = build_cover(no_commands_ctx)
    assert "position_topic" in payload  # attribute still present
    assert "set_position_topic" not in payload
    assert "command_topic" not in payload


def test_build_cover_with_slats_has_tilt_topics() -> None:
    config = _config(
        {
            "inputs": {
                "idp0000": {"pairingID": 35, "value": "0"},
                "idp0001": {"pairingID": 36, "value": "0"},
            },
            "outputs": {
                "odp0000": {"pairingID": 289, "value": "20"},
                "odp0001": {"pairingID": 290, "value": "50"},
            },
        },
        functionID="61",
    )
    ctx = _context(config, _compile_one(config))
    payload = build_cover(ctx)
    assert payload["tilt_status_topic"] == ctx.entity.state_topic
    assert payload["tilt_status_template"] == "{{ value_json.slat_position }}"
    assert payload["tilt_command_topic"] == f"{ctx.entity.state_topic}/set/slat_position"
    assert payload["tilt_min"] == 0
    assert payload["tilt_max"] == 100


# ------------------------------------------------------------------------------- binary_sensor


def test_build_binary_sensor() -> None:
    config = _config({"outputs": {"odp0000": {"pairingID": 53, "value": "1"}}}, functionID="f")
    ctx = _context(config, _compile_one(config))
    payload = build_binary_sensor(ctx)
    assert payload["state_topic"] == ctx.entity.state_topic
    assert payload["value_template"] == "{{ 'ON' if value_json.state else 'OFF' }}"


def test_build_binary_sensor_rejects_a_multi_attribute_entity() -> None:
    config = _config(
        {
            "inputs": {
                "idp0000": {"pairingID": 1, "value": "0"},
                "idp0001": {"pairingID": 17, "value": "0"},
            },
            "outputs": {
                "odp0000": {"pairingID": 256, "value": "0"},
                "odp0001": {"pairingID": 272, "value": "0"},
            },
        },
        functionID="12",
    )
    ctx = _context(config, _compile_one(config))
    with pytest.raises(DiscoveryError, match="exactly one attribute"):
        build_binary_sensor(ctx)


# ------------------------------------------------------------------------------------ sensor


def test_build_sensor_includes_unit_and_measurement_state_class() -> None:
    config = _config({"outputs": {"odp0000": {"pairingID": 304, "value": "21.5"}}}, functionID="43")
    ctx = _context(config, _compile_one(config))
    payload = build_sensor(ctx)
    assert payload["value_template"] == "{{ value_json.temperature }}"
    assert payload["unit_of_measurement"] == "°C"
    assert payload["state_class"] == "measurement"


# ------------------------------------------------------------------------------------ number


def test_build_number_heating_actuator() -> None:
    config = _config(
        {
            "inputs": {"idp0000": {"pairingID": 48, "value": "0"}},
            "outputs": {"odp0000": {"pairingID": 48, "value": "42"}},
        },
        functionID="27",
    )
    ctx = _context(config, _compile_one(config))
    payload = build_number(ctx)
    assert payload["state_topic"] == ctx.entity.state_topic
    assert payload["command_topic"] == f"{ctx.entity.state_topic}/set/heating_value"
    assert payload["min"] == 0
    assert payload["max"] == 100
    assert payload["unit_of_measurement"] == "%"


# ----------------------------------------------------------------------------------- climate


def _rtc_config() -> dict[str, Any]:
    return _config(
        {
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
        },
        functionID="a",
    )


def _rtc_basic_config() -> dict[str, Any]:
    """A controller with no on/off input, so `room_temperature_controller_basic` claims it: a bare
    setpoint with nothing to derive a mode -- or a preset -- from.
    """
    return _config(
        {
            "inputs": {"idp0000": {"pairingID": 51, "value": "21.0"}},
            "outputs": {
                "odp0000": {"pairingID": 304, "value": "21.0"},
                "odp0001": {"pairingID": 305, "value": "0"},
            },
        },
        functionID="a",
    )


def test_build_climate_full_profile_wires_real_mode_topics() -> None:
    # room_temperature_controller has on_off (docs/03 §7's transform makes hvac_mode a real,
    # working attribute+command now) -- mode control must no longer be omitted.
    config = _rtc_config()
    ctx = _context(config, _compile_one(config))
    payload = build_climate(ctx)
    assert payload["current_temperature_topic"] == ctx.entity.state_topic
    assert payload["temperature_command_topic"] == (
        f"{ctx.entity.state_topic}/set/setpoint_temperature"
    )
    assert payload["mode_state_topic"] == ctx.entity.state_topic
    assert payload["mode_command_topic"] == f"{ctx.entity.state_topic}/set/hvac_mode"
    assert set(payload["modes"]) == {"off", "heat", "cool"}


def test_build_climate_mode_state_template_maps_our_vocabulary_to_ha() -> None:
    # A literal string check, matching how every other value_template in this test suite is
    # verified (e.g. test_build_switch_uses_scalar_shorthand_topics) -- Jinja itself is never a
    # project dependency; Home Assistant is the one that renders these, never this bridge.
    config = _rtc_config()
    ctx = _context(config, _compile_one(config))
    payload = build_climate(ctx)
    assert payload["mode_state_template"] == (
        "{{ {'off': 'off', 'eco': 'heat', 'heating': 'heat', 'cooling': 'cool'}"
        ".get(value_json.hvac_mode, 'off') }}"
    )


def test_build_climate_mode_command_template_maps_ha_vocabulary_to_ours() -> None:
    """No `auto` entry any more: eco moved to the preset axis in WP18, and HA's `heat` must mean
    `heating` unambiguously rather than sometimes meaning eco.
    """
    config = _rtc_config()
    ctx = _context(config, _compile_one(config))
    payload = build_climate(ctx)
    assert payload["mode_command_template"] == (
        "{{ {'off': 'off', 'heat': 'heating', 'cool': 'cooling'}.get(value, 'off') }}"
    )


def test_build_climate_basic_profile_still_omits_mode_topics() -> None:
    # room_temperature_controller_basic has no on_off/eco/mode at all -- there is nothing for
    # hvac_mode to derive from, so mode control must stay omitted.
    config = _config(
        {
            "inputs": {"idp0000": {"pairingID": 51, "value": "21.0"}},
            "outputs": {
                "odp0000": {"pairingID": 304, "value": "21.0"},
                "odp0001": {"pairingID": 305, "value": "0"},
            },
        },
        functionID="a",
    )
    ctx = _context(config, _compile_one(config))
    payload = build_climate(ctx)
    assert "mode_command_topic" not in payload
    assert "mode_state_topic" not in payload


# ------------------------------------------------------------------------------------- event


def test_build_event_trigger() -> None:
    config = _config({"outputs": {"odp0000": {"pairingID": 1, "value": ""}}}, functionID="18")
    ctx = _context(config, _compile_one(config))
    payload = build_event(ctx)
    assert payload["state_topic"] == f"{ctx.entity.state_topic}/event"
    assert payload["event_types"] == ["press"]
    assert payload["value_template"] == '{"event_type": "{{ value_json.event }}"}'


# ---------------------------------------------------------------------------- registry coverage


@pytest.mark.parametrize("profile", list(REGISTRY), ids=lambda p: p.id)
def test_every_profile_with_a_homeassistant_component_has_a_registered_builder(
    profile: Any,
) -> None:
    component = profile.homeassistant.get("component")
    if component is None:
        pytest.skip(f"{profile.id} has no homeassistant.component yet")
    assert component in COMPONENT_BUILDERS


def test_component_builders_registry_is_closed_and_matches_known_platforms() -> None:
    assert set(COMPONENT_BUILDERS) == {
        "switch",
        "light",
        "cover",
        "binary_sensor",
        "sensor",
        "number",
        "climate",
        "event",
    }


# ------------------------------------------------- WP18: eco as a preset_mode (docs/12 §8)


def test_climate_maps_eco_to_a_preset_rather_than_an_hvac_mode() -> None:
    """WP10 mapped our `eco` onto HA's `auto` HVAC mode, and said so as a documented
    simplification. HA models eco as a *preset*, on an axis of its own, so a user who selects
    `auto` in HA gets something that is not what free@home calls eco, and the real eco state is
    invisible. WP18 gives it the axis it belongs on.

    The wire vocabulary this bridge publishes (`off`/`eco`/`heating`/`cooling`) does not change:
    the translation stays at the HA discovery boundary, so ADR-009 holds and no existing MQTT
    consumer is affected.
    """
    config = _rtc_config()
    payload = build_climate(_context(config, _compile_one(config)))

    assert "auto" not in payload["modes"]
    assert payload["preset_modes"] == ["none", "eco"]
    assert payload["preset_mode_state_topic"]
    assert payload["preset_mode_command_topic"]


def test_climate_hvac_modes_no_longer_include_the_eco_stand_in() -> None:
    config = _rtc_config()
    payload = build_climate(_context(config, _compile_one(config)))
    assert set(payload["modes"]) == {"off", "heat", "cool"}


def test_climate_preset_state_template_reports_eco_only_when_eco_is_set() -> None:
    config = _rtc_config()
    payload = build_climate(_context(config, _compile_one(config)))
    template = payload["preset_mode_state_template"]
    assert "eco" in template
    assert "none" in template


def test_climate_without_on_off_still_has_no_mode_or_preset_axis() -> None:
    """`room_temperature_controller_basic` is a bare setpoint with nothing to derive a mode from;
    it must not grow a preset axis it cannot drive.
    """
    config = _rtc_basic_config()
    payload = build_climate(_context(config, _compile_one(config)))
    assert "preset_modes" not in payload
    assert "modes" not in payload
