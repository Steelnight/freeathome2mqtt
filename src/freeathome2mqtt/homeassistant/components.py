"""Profile -> Home Assistant component mapping tables (docs/04 §6.2; docs/11 WP10).

`COMPONENT_BUILDERS` is a fixed, closed dict keyed by the `component:` string a profile's
`homeassistant:` YAML block names (CLAUDE.md rule 8 -- the same "named, reviewed set" discipline
`model/transforms.py`'s `@transform` registry uses, not an open lookup from an arbitrary string).
Each builder is a pure function of a `ComponentContext` -- the already-compiled `Entity`, its
`Profile`, the channel/device it came from, and which of the profile's commands actually survived
compilation -- to the HA-component-specific payload fields (`state_topic`, `command_topic`,
capability flags, ...). `homeassistant/discovery.py` adds the fields every component shares
(`name`, `unique_id`, `device`, `availability`, `origin`) on top.

Two deliberate simplifications, named rather than silently dropped:

* `climate` maps HA's `HVACMode` vocabulary to `model/transforms.py`'s own "off"/"eco"/"heating"/
  "cooling" via `_dict_lookup_template` in `mode_state_template`/`mode_command_template`, rather
  than renaming this bridge's own published `hvac_mode` vocabulary (a breaking change for anyone
  already consuming it). HA has no native "eco" HVAC mode, so it is mapped to HA's `auto`; a
  proper `preset_mode` axis for it is future work. `room_temperature_controller_basic` has no
  `on_off`/`eco`/`mode` at all, so it gets no mode topics -- there is nothing to derive one from.
* `heating_actuator` maps to `number`, not `valve` -- it is a bare 0-100% actuating value with no
  open/close semantics, which fits HA's generic `number` platform more directly than `valve`.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from freeathome2mqtt.model.entity import Entity
from freeathome2mqtt.model.profiles import Profile
from freeathome2mqtt.mqtt.topics import entity_event_topic, entity_set_attribute_topic
from freeathome2mqtt.sysap.codes import Parameter
from freeathome2mqtt.sysap.schema import Channel, Device

_EVENT_TYPE = "press"

# docs/03 §7: RoomTemperatureControllerTransform derives/accepts "off"/"eco"/"heating"/"cooling",
# never HA's own HVACMode vocabulary. HA has no native "eco" HVAC mode -- mapping it to "auto" is
# a documented simplification; a proper preset_mode axis is future work, named not silently
# dropped. The mapping lives here, at the HA-payload boundary, rather than by renaming this
# bridge's own published vocabulary (a breaking change for anyone already consuming `hvac_mode`).
_HVAC_MODE_TO_HA = {"off": "off", "eco": "auto", "heating": "heat", "cooling": "cool"}
_HA_TO_HVAC_MODE = {ha: ours for ours, ha in _HVAC_MODE_TO_HA.items()}


class DiscoveryError(Exception):
    """A profile's `homeassistant:` block names an unknown component, or a component builder's
    assumption about its own profile (e.g. "exactly one attribute") does not hold -- a
    profile-authoring bug, the same trust boundary as an unknown codec (CLAUDE.md rule 5).
    """


@dataclass(frozen=True, slots=True)
class DiscoveryOptions:
    """Everything about discovery generation that is a *choice* rather than a fact from the
    compiled model (docs/04 §6).
    """

    enabled: bool = True
    discovery_topic: str = "homeassistant"
    base_topic: str = "freeathome2mqtt"
    sysap_serial: str = ""
    bridge_version: str = "0.0.0-dev"


@dataclass(frozen=True, slots=True)
class ComponentContext:
    """Everything one component builder needs, bundled to keep builder signatures small."""

    entity: Entity
    profile: Profile
    channel: Channel
    device: Device
    commands: frozenset[str]
    options: DiscoveryOptions


def _json_value_template(key: str) -> str:
    return f"{{{{ value_json.{key} }}}}"


def _dict_lookup_template(mapping: Mapping[str, str], source: str, default: str) -> str:
    pairs = ", ".join(f"'{key}': '{value}'" for key, value in mapping.items())
    return f"{{{{ {{{pairs}}}.get({source}, '{default}') }}}}"


def _sole_attribute(ctx: ComponentContext) -> str:
    if len(ctx.entity.attr_names) != 1:
        raise DiscoveryError(
            f"{ctx.profile.id}: its homeassistant component expects exactly one attribute, "
            f"got {ctx.entity.attr_names!r}"
        )
    return ctx.entity.attr_names[0]


def _find_parameter_value(parameters: Mapping[str, str], parameter: Parameter) -> str | None:
    # Mirrors model/compiler.py's `_find_parameter_value` (hex-tolerant "parNNNN" key matching,
    # docs/01 §4.3) -- kept as a small, self-contained duplicate rather than importing a private
    # compiler helper, per CLAUDE.md's "three similar lines beats a premature abstraction"
    # (the same tradeoff `mqtt/client.py` already documents for its own backoff helper).
    for key in sorted(parameters):
        suffix = key.removeprefix("par").removeprefix("PAR")
        try:
            parsed = int(suffix, 16)
        except ValueError:
            continue
        if parsed == parameter.value:
            return parameters[key]
    return None


def _color_temp_kelvin_bounds(ctx: ComponentContext) -> tuple[float, float]:
    """Re-derive the same per-channel Kelvin bounds `model/compiler.py`'s `color_temp_pct` codec
    was bound with (P-09). Safe to assume they resolve: `color_temp` only appears in
    `entity.attr_names` at all when compilation already resolved them for this exact channel.
    """
    warmest_param = ctx.profile.parameters.get("color_temp_warmest")
    coolest_param = ctx.profile.parameters.get("color_temp_coolest")
    if warmest_param is None or coolest_param is None:
        raise DiscoveryError(
            f"{ctx.profile.id}: has a color_temp attribute but no color_temp_warmest/"
            "color_temp_coolest parameters entry"
        )
    parameters = ctx.channel.get("parameters", {})
    warmest_raw = _find_parameter_value(parameters, warmest_param)
    coolest_raw = _find_parameter_value(parameters, coolest_param)
    if warmest_raw is None or coolest_raw is None:
        raise DiscoveryError(
            f"{ctx.entity.id}: color_temp attribute present but its channel parameters are absent"
        )
    return float(warmest_raw), float(coolest_raw)


def build_switch(ctx: ComponentContext) -> dict[str, Any]:
    attr = _sole_attribute(ctx)
    return {
        "state_topic": ctx.entity.state_topic,
        "value_template": f"{{{{ 'ON' if value_json.{attr} else 'OFF' }}}}",
        # Scalar shorthand (docs/04 §3.3) -- HA's simple switch component sends a bare ON/OFF.
        "command_topic": ctx.entity.set_topic,
        "payload_on": "ON",
        "payload_off": "OFF",
    }


def build_light(ctx: ComponentContext) -> dict[str, Any]:
    entity = ctx.entity
    payload: dict[str, Any] = {
        "schema": "json",
        "state_topic": entity.state_topic,
        "command_topic": entity.set_topic,
    }
    supported_modes: list[str] = []
    if "brightness" in entity.attr_names:
        payload["brightness"] = True
        payload["brightness_scale"] = 100
        supported_modes.append("brightness")
    if "color_temp" in entity.attr_names:
        warmest, coolest = _color_temp_kelvin_bounds(ctx)
        payload["color_temp_kelvin"] = True
        payload["min_kelvin"] = round(min(warmest, coolest))
        payload["max_kelvin"] = round(max(warmest, coolest))
        supported_modes = ["color_temp"]
    payload["supported_color_modes"] = supported_modes or ["onoff"]
    return payload


def build_cover(ctx: ComponentContext) -> dict[str, Any]:
    entity = ctx.entity
    payload: dict[str, Any] = {}
    if "position" in entity.attr_names:
        payload["position_topic"] = entity.state_topic
        payload["position_template"] = _json_value_template("position")
    if "position" in ctx.commands:
        payload["set_position_topic"] = entity_set_attribute_topic(entity.state_topic, "position")
    if "stop" in ctx.commands:
        payload["command_topic"] = entity_set_attribute_topic(entity.state_topic, "stop")
        payload["payload_stop"] = "true"
    if "slat_position" in entity.attr_names:
        payload["tilt_status_topic"] = entity.state_topic
        payload["tilt_status_template"] = _json_value_template("slat_position")
    if "slat_position" in ctx.commands:
        payload["tilt_command_topic"] = entity_set_attribute_topic(
            entity.state_topic, "slat_position"
        )
        payload["tilt_min"] = 0
        payload["tilt_max"] = 100
    return payload


def build_binary_sensor(ctx: ComponentContext) -> dict[str, Any]:
    attr = _sole_attribute(ctx)
    return {
        "state_topic": ctx.entity.state_topic,
        "value_template": f"{{{{ 'ON' if value_json.{attr} else 'OFF' }}}}",
        "payload_on": "ON",
        "payload_off": "OFF",
    }


def build_sensor(ctx: ComponentContext) -> dict[str, Any]:
    attr = _sole_attribute(ctx)
    spec = ctx.profile.attributes[attr]
    payload: dict[str, Any] = {
        "state_topic": ctx.entity.state_topic,
        "value_template": _json_value_template(attr),
    }
    if spec.unit:
        payload["unit_of_measurement"] = spec.unit
        payload["state_class"] = "measurement"
    return payload


def build_number(ctx: ComponentContext) -> dict[str, Any]:
    attr = _sole_attribute(ctx)
    spec = ctx.profile.attributes[attr]
    payload: dict[str, Any] = {
        "state_topic": ctx.entity.state_topic,
        "value_template": _json_value_template(attr),
        "min": 0,
        "max": 100,
        "step": 1,
        "mode": "slider",
    }
    if attr in ctx.commands:
        payload["command_topic"] = entity_set_attribute_topic(ctx.entity.state_topic, attr)
    if spec.unit:
        payload["unit_of_measurement"] = spec.unit
    return payload


def build_climate(ctx: ComponentContext) -> dict[str, Any]:
    entity = ctx.entity
    payload: dict[str, Any] = {}
    if "measured_temperature" in entity.attr_names:
        payload["current_temperature_topic"] = entity.state_topic
        payload["current_temperature_template"] = _json_value_template("measured_temperature")
    if "setpoint_temperature" in ctx.commands:
        payload["temperature_command_topic"] = entity_set_attribute_topic(
            entity.state_topic, "setpoint_temperature"
        )
    # "on_off" is exactly the discriminator between room_temperature_controller (has the
    # RoomTemperatureControllerTransform's on_off/eco/mode inputs, docs/03 §7) and
    # room_temperature_controller_basic (a bare setpoint with nothing to derive a mode from).
    if "on_off" in entity.attr_names:
        payload["mode_state_topic"] = entity.state_topic
        payload["mode_state_template"] = _dict_lookup_template(
            _HVAC_MODE_TO_HA, "value_json.hvac_mode", "off"
        )
        payload["modes"] = list(dict.fromkeys(_HVAC_MODE_TO_HA.values()))
        payload["mode_command_topic"] = entity_set_attribute_topic(entity.state_topic, "hvac_mode")
        payload["mode_command_template"] = _dict_lookup_template(_HA_TO_HVAC_MODE, "value", "off")
    return payload


def build_event(ctx: ComponentContext) -> dict[str, Any]:
    return {
        "state_topic": entity_event_topic(ctx.entity.state_topic),
        "value_template": '{"event_type": "{{ value_json.event }}"}',
        "event_types": [_EVENT_TYPE],
    }


COMPONENT_BUILDERS: Mapping[str, Callable[[ComponentContext], dict[str, Any]]] = {
    "switch": build_switch,
    "light": build_light,
    "cover": build_cover,
    "binary_sensor": build_binary_sensor,
    "sensor": build_sensor,
    "number": build_number,
    "climate": build_climate,
    "event": build_event,
}
