"""Build, publish and retract Home Assistant discovery payloads (docs/04 §6; docs/11 WP10).

`build_model_discovery` is the "build" half (ADR-004: pre-serialised, once, off the hot path). It
runs *after* `model.compiler.compile()`, not inside it -- `Entity` is deliberately not frozen
(docs/03 §2) specifically so this can mutate each entity's `.discovery` field in place and hand
back a `Model` with its own `.discovery` tuple recomputed, matching docs/02 §1's architecture
diagram (`TBL -.-> HAD`: Discovery reads the compiled tables as a downstream consumer, `compile()`
itself stays free of any Home Assistant knowledge).

`DiscoveryPublisher` is the "publish + retract" half: changed-only publishing backed by
`persistence.DiscoveryStore` (docs/07 §4.2) so a restart with an unchanged installation publishes
zero discovery messages, plus a force-everything path for `bridge/request/discovery/republish` and
the delayed HA-birth republish (P-36, P-37). *When* to call which -- the debounce, the birth-message
delay, cross-restart stale-topic retraction -- is `supervisor.py`'s job, the same split every other
owned-state class in this codebase uses (`BridgeAvailability`, `CommandDispatcher`, ...).
"""

from __future__ import annotations

import dataclasses
from collections.abc import Iterable, Mapping
from itertools import chain
from typing import TYPE_CHECKING, Any

import orjson

from freeathome2mqtt.homeassistant.components import (
    COMPONENT_BUILDERS,
    ComponentContext,
    DiscoveryError,
    DiscoveryOptions,
)
from freeathome2mqtt.mqtt.topics import bridge_state_topic, ha_discovery_config_topic
from freeathome2mqtt.sysap.codes import Function
from freeathome2mqtt.sysap.schema import Device, parse_function_id

if TYPE_CHECKING:
    from freeathome2mqtt.model.compiler import Model
    from freeathome2mqtt.model.entity import Entity
    from freeathome2mqtt.model.profiles import Profile, ProfileRegistry
    from freeathome2mqtt.mqtt.client import MqttClient
    from freeathome2mqtt.persistence import DiscoveryStore
    from freeathome2mqtt.sysap.schema import Channel, Configuration

__all__ = ["DiscoveryOptions", "DiscoveryPublisher", "build_model_discovery"]

_MANUFACTURER = "Busch-Jaeger / ABB"
_ORIGIN_URL = "https://github.com/Steelnight/freeathome2mqtt"
_ORIGIN_NAME = "freeathome2mqtt"


def _resolve_model_name(channel: Channel, profile: Profile) -> str:
    function_id = parse_function_id(channel.get("functionID"))
    if function_id is not None:
        try:
            function = Function(function_id)
        except ValueError:
            function = None
        if function is not None:
            return f"{function.name} (0x{function.value:X})"
    return profile.id


def _device_block(ctx: ComponentContext) -> dict[str, Any]:
    entity, device, options = ctx.entity, ctx.device, ctx.options
    block: dict[str, Any] = {
        "identifiers": [entity.device_serial],
        "name": device.get("displayName") or entity.device_serial,
        "manufacturer": _MANUFACTURER,
        "model": _resolve_model_name(ctx.channel, ctx.profile),
    }
    hw_version = device.get("deviceId")
    if hw_version:
        block["hw_version"] = hw_version
    if entity.area is not None:
        block["suggested_area"] = entity.area
    if options.sysap_serial:
        block["via_device"] = options.sysap_serial
    return block


def _common_envelope(ctx: ComponentContext) -> dict[str, Any]:
    entity, options = ctx.entity, ctx.options
    object_id = entity.state_topic.rsplit("/", 1)[-1]
    availability: list[dict[str, str]] = [
        {
            "topic": bridge_state_topic(options.base_topic),
            "value_template": "{{ value_json.state }}",
        }
    ]
    if entity.availability_topic is not None:
        availability.append(
            {"topic": entity.availability_topic, "value_template": "{{ value_json.state }}"}
        )
    return {
        "name": entity.name,
        "unique_id": entity.id,  # P-34: never derived from the (renameable) display name
        "object_id": object_id,
        "availability": availability,
        "availability_mode": "all",
        "device": _device_block(ctx),
        "origin": {"name": _ORIGIN_NAME, "sw": options.bridge_version, "url": _ORIGIN_URL},
    }


def _build_entity_discovery(
    entity: Entity,
    profile: Profile,
    channel: Channel,
    device: Device,
    *,
    commands: frozenset[str],
    options: DiscoveryOptions,
    override: Mapping[str, Any],
) -> tuple[tuple[str, bytes], ...]:
    component = profile.homeassistant.get("component")
    if component is None:
        return ()  # no homeassistant: mapping yet (a tier-2/3 profile, a later WP)
    builder = COMPONENT_BUILDERS.get(component)
    if builder is None:
        raise DiscoveryError(f"{profile.id}: unknown homeassistant component {component!r}")

    ctx = ComponentContext(
        entity=entity,
        profile=profile,
        channel=channel,
        device=device,
        commands=commands,
        options=options,
    )
    payload: dict[str, Any] = dict(builder(ctx))
    device_class = profile.homeassistant.get("device_class")
    if device_class is not None:
        payload["device_class"] = device_class
    payload.update(_common_envelope(ctx))
    if override:
        payload.update(override)  # entity/options {"homeassistant": {...}}, docs/04 §5

    object_id = entity.state_topic.rsplit("/", 1)[-1]
    topic = ha_discovery_config_topic(options.discovery_topic, component, entity.id, object_id)
    return ((topic, orjson.dumps(payload, option=orjson.OPT_SORT_KEYS)),)


def build_model_discovery(
    model: Model,
    profiles: ProfileRegistry,
    config: Configuration,
    options: DiscoveryOptions,
    entity_overrides: Mapping[str, Mapping[str, Any]] | None = None,
) -> Model:
    """Pure: mutate every entity's `.discovery` (docs/03 §2's deliberately-not-frozen `Entity`)
    and return `model` with its own `.discovery` recomputed. When `options.enabled` is False every
    entity's `.discovery` becomes `()`, matching a fresh compile with Home Assistant off.

    `entity_overrides` is `entity_id -> homeassistant option dict` (docs/04 §5's `entity/options`
    `homeassistant` field, round-tripped through `entities.json`): each entity's own override, if
    any, is shallow-merged on top of its auto-built payload before serialising, so a user can add
    `device_class`/`entity_category`/etc per entity without a profile change.
    """
    devices = config.get("devices", {})
    commands_by_entity: dict[int, set[str]] = {}
    for entity_idx, command_name in model.egress:
        commands_by_entity.setdefault(entity_idx, set()).add(command_name)
    overrides = entity_overrides or {}

    for entity in model.entities:
        if not options.enabled:
            entity.discovery = ()
            continue
        profile = profiles[entity.profile]
        device = devices.get(entity.device_serial, {})
        channel = device.get("channels", {}).get(entity.channel_id, {})
        commands = frozenset(commands_by_entity.get(entity.idx, ()))
        entity.discovery = _build_entity_discovery(
            entity,
            profile,
            channel,
            device,
            commands=commands,
            options=options,
            override=overrides.get(entity.id, {}),
        )

    discovery = tuple(chain.from_iterable(entity.discovery for entity in model.entities))
    return dataclasses.replace(model, discovery=discovery)


class DiscoveryPublisher:
    """Owns publishing and retracting discovery topics, backed by `persistence.DiscoveryStore`
    (docs/07 §4.2) -- the same "an owning object, state passed explicitly" pattern
    `BridgeAvailability`/`CommandDispatcher` already use (CLAUDE.md rule 6).
    """

    def __init__(self, *, mqtt: MqttClient, store: DiscoveryStore) -> None:
        self._mqtt = mqtt
        self._store = store

    async def publish_changed(self, model: Model) -> None:
        """Publish only topics whose payload differs from what `store` last recorded -- a no-op
        reload publishes zero messages (docs/05 §5, docs/11 WP10's acceptance criterion).
        """
        changed = False
        for topic, payload in model.discovery:
            if not self._store.is_changed(topic, payload):
                continue
            await self._mqtt.publish(topic, payload, qos=1, retain=True)
            self._store.mark(topic, payload)
            changed = True
        if changed:
            await self._store.save()

    async def publish_all(self, model: Model) -> None:
        """Force-republish every discovery topic regardless of `store` state -- `discovery/
        republish` (docs/04 §5) and the delayed HA-birth republish (P-36, P-37) both need this,
        not the changed-only path `publish_changed` uses for ordinary resyncs.
        """
        for topic, payload in model.discovery:
            await self._mqtt.publish(topic, payload, qos=1, retain=True)
            self._store.mark(topic, payload)
        if model.discovery:
            await self._store.save()

    async def retract(self, topics: Iterable[str]) -> None:
        """Publish an empty retained payload for each topic and forget it (P-35)."""
        removed = False
        for topic in topics:
            await self._mqtt.publish(topic, b"", qos=1, retain=True)
            self._store.remove(topic)
            removed = True
        if removed:
            await self._store.save()

    def stale_topics(self, model: Model) -> list[str]:
        """Topics `store` remembers publishing in a *previous run* that `model` would no longer
        publish -- the cross-restart half of P-35 the in-memory old-model-vs-new-model diff in
        `supervisor._diff_and_apply` can't see (there is no old model on the very first compile).
        """
        live = {topic for topic, _ in model.discovery}
        return [topic for topic in self._store.hashes if topic not in live]
