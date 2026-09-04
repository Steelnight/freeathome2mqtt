"""Tests for homeassistant/discovery.py: build_model_discovery (pure) and DiscoveryPublisher
(changed-only publish, force republish, retract, cross-restart stale detection) (docs/04 §6;
docs/07 §4.2; docs/11 WP10).
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import aiomqtt
import orjson
import pytest

from fakes.fake_broker import running_fake_broker
from freeathome2mqtt.homeassistant.components import DiscoveryError
from freeathome2mqtt.homeassistant.discovery import (
    DiscoveryOptions,
    DiscoveryPublisher,
    build_model_discovery,
)
from freeathome2mqtt.model.compiler import CompileOptions, compile
from freeathome2mqtt.model.profiles import (
    ProfileRegistry,
    build_registry,
    load_profile_registry,
    parse_profile,
)
from freeathome2mqtt.mqtt.client import MqttClient
from freeathome2mqtt.persistence import DiscoveryStore

PROFILES_DIR = Path(__file__).resolve().parent.parent / "src" / "freeathome2mqtt" / "profiles"
REGISTRY: ProfileRegistry = load_profile_registry(PROFILES_DIR)
SERIAL = "ABB7F500E17A"
BASE = "freeathome2mqtt"
_FAST = {"backoff_initial": 0.02, "backoff_factor": 2.0, "backoff_cap": 0.2}


def _switch_config(*, name: str = "Switch") -> dict[str, Any]:
    return {
        "floorplan": {"floors": {"01": {"name": "GF", "rooms": {"0C": {"name": "Room"}}}}},
        "devices": {
            SERIAL: {
                "displayName": name,
                "deviceId": "910C",
                "interface": "TP",
                "floor": "01",
                "room": "0C",
                "channels": {
                    "ch0000": {
                        "displayName": "Switch",
                        "functionID": "7",
                        "inputs": {"idp0000": {"pairingID": 1, "value": "0"}},
                        "outputs": {"odp0000": {"pairingID": 256, "value": "1"}},
                        "floor": "01",
                        "room": "0C",
                    }
                },
            }
        },
    }


def _compiled(
    config: dict[str, Any],
    *,
    aliases: dict[str, str] | None = None,
    registry: ProfileRegistry = REGISTRY,
) -> Any:
    options = CompileOptions(topic_prefix=BASE, aliases=aliases or {})
    return compile(config, registry, options)


def _client(broker) -> MqttClient:
    return MqttClient(
        host="127.0.0.1", port=broker.port, base_topic=BASE, sysap_serial=SERIAL, **_FAST
    )


async def _wait_until(predicate, *, timeout_seconds: float = 5.0, interval: float = 0.01) -> None:
    async with asyncio.timeout(timeout_seconds):
        while not predicate():  # noqa: ASYNC110 -- generic poll, no single event to await
            await asyncio.sleep(interval)


# ------------------------------------------------------------------------ build_model_discovery


def test_disabled_options_leaves_every_entity_undiscovered() -> None:
    model = build_model_discovery(
        _compiled(_switch_config()), REGISTRY, _switch_config(), DiscoveryOptions(enabled=False)
    )
    assert model.discovery == ()
    assert model.entities[0].discovery == ()


def test_switch_entity_gets_one_discovery_topic_with_a_switch_component() -> None:
    config = _switch_config()
    model = build_model_discovery(_compiled(config), REGISTRY, config, DiscoveryOptions())
    assert len(model.discovery) == 1
    topic, payload = model.discovery[0]
    assert topic.startswith("homeassistant/switch/")
    assert topic.endswith("/config")
    body = orjson.loads(payload)
    assert body["unique_id"] == f"{SERIAL}_ch0000"
    assert body["device"]["identifiers"] == [SERIAL]
    assert body["device"]["manufacturer"] == "Busch-Jaeger / ABB"
    assert body["device"]["hw_version"] == "910C"


def test_unique_id_stable_across_rename() -> None:
    # P-34's named test: unique_id must never change even though the topic (object_id) does.
    config = _switch_config()
    before = build_model_discovery(_compiled(config), REGISTRY, config, DiscoveryOptions())
    after = build_model_discovery(
        _compiled(config, aliases={f"{SERIAL}_ch0000": "kitchen_light"}),
        REGISTRY,
        config,
        DiscoveryOptions(),
    )
    before_topic, before_payload = before.discovery[0]
    after_topic, after_payload = after.discovery[0]
    assert orjson.loads(before_payload)["unique_id"] == orjson.loads(after_payload)["unique_id"]
    assert before_topic != after_topic  # object_id (the slug) legitimately changes


def test_via_device_and_sw_version_come_from_options() -> None:
    config = _switch_config()
    options = DiscoveryOptions(sysap_serial="ABB700ABCDEF", bridge_version="1.2.3")
    model = build_model_discovery(_compiled(config), REGISTRY, config, options)
    body = orjson.loads(model.discovery[0][1])
    assert body["device"]["via_device"] == "ABB700ABCDEF"
    assert body["origin"]["sw"] == "1.2.3"


def test_entity_override_merges_on_top_of_the_built_payload() -> None:
    # entity/options {"homeassistant": {...}} (docs/04 §5) round-trips through entities.json --
    # this is the point where it finally takes effect, without touching the profile itself.
    config = _switch_config()
    entity_id = f"{SERIAL}_ch0000"
    model = build_model_discovery(
        _compiled(config),
        REGISTRY,
        config,
        DiscoveryOptions(),
        entity_overrides={entity_id: {"device_class": "outlet", "entity_category": "config"}},
    )
    body = orjson.loads(model.discovery[0][1])
    assert body["device_class"] == "outlet"
    assert body["entity_category"] == "config"


def test_entity_override_for_a_different_entity_id_has_no_effect() -> None:
    config = _switch_config()
    model = build_model_discovery(
        _compiled(config),
        REGISTRY,
        config,
        DiscoveryOptions(),
        entity_overrides={"some_other_entity": {"device_class": "outlet"}},
    )
    body = orjson.loads(model.discovery[0][1])
    assert "device_class" not in body


def test_no_entity_overrides_leaves_the_payload_unchanged() -> None:
    config = _switch_config()
    without = build_model_discovery(_compiled(config), REGISTRY, config, DiscoveryOptions())
    with_empty = build_model_discovery(
        _compiled(config), REGISTRY, config, DiscoveryOptions(), entity_overrides={}
    )
    assert without.discovery == with_empty.discovery


def _window_door_config() -> dict[str, Any]:
    return {
        "floorplan": {"floors": {"01": {"name": "GF", "rooms": {"0C": {"name": "Room"}}}}},
        "devices": {
            SERIAL: {
                "displayName": "Front Door",
                "interface": "TP",
                "floor": "01",
                "room": "0C",
                "channels": {
                    "ch0000": {
                        "displayName": "Door Sensor",
                        "functionID": "f",  # FID_WINDOW_DOOR_SENSOR
                        "outputs": {"odp0000": {"pairingID": 53, "value": "1"}},
                        "floor": "01",
                        "room": "0C",
                    }
                },
            }
        },
    }


def test_device_class_from_the_profile_is_included_in_the_payload() -> None:
    config = _window_door_config()
    model = build_model_discovery(_compiled(config), REGISTRY, config, DiscoveryOptions())
    body = orjson.loads(model.discovery[0][1])
    assert body["device_class"] == "window"


def test_entity_category_from_the_sole_attribute_is_included_in_the_payload() -> None:
    # window_door_sensor has exactly one attribute (`state`), itself marked
    # `entity_category: diagnostic` in profiles/sensors.yaml -- the unambiguous case where a
    # per-attribute category is also unambiguously the whole entity's category.
    config = _window_door_config()
    model = build_model_discovery(_compiled(config), REGISTRY, config, DiscoveryOptions())
    body = orjson.loads(model.discovery[0][1])
    assert body["entity_category"] == "diagnostic"


def test_entity_category_is_absent_for_a_multi_attribute_entity() -> None:
    # dimming_actuator's `forced_position` attribute declares entity_category: diagnostic, but
    # it is one of three attributes (state, brightness, forced_position) sharing one `light`
    # discovery payload -- HA's entity_category is whole-entity, so marking the whole light
    # diagnostic would hide it from the dashboard. Only the single-attribute case is wired.
    config = {
        "floorplan": {"floors": {"01": {"name": "GF", "rooms": {"0C": {"name": "Room"}}}}},
        "devices": {
            SERIAL: {
                "displayName": "Dimmer",
                "interface": "TP",
                "floor": "01",
                "room": "0C",
                "channels": {
                    "ch0000": {
                        "displayName": "Dimmer",
                        "functionID": "12",  # FID_DIMMING_ACTUATOR
                        "outputs": {
                            "odp0000": {"pairingID": 256, "value": "1"},  # AL_INFO_ON_OFF
                            "odp0001": {  # AL_INFO_ACTUAL_DIMMING_VALUE
                                "pairingID": 272,
                                "value": "50",
                            },
                        },
                        "floor": "01",
                        "room": "0C",
                    }
                },
            }
        },
    }
    model = build_model_discovery(_compiled(config), REGISTRY, config, DiscoveryOptions())
    body = orjson.loads(model.discovery[0][1])
    assert "entity_category" not in body


def test_unknown_homeassistant_component_raises() -> None:
    bad_profile = parse_profile(
        {
            "id": "bad_component",
            "functions": ["FID_SWITCH_ACTUATOR"],
            "homeassistant": {"component": "not_a_real_platform"},
            "attributes": {"state": {"pairing": "AL_INFO_ON_OFF", "codec": "bool01"}},
        },
        source="<test>",
    )
    registry = build_registry([bad_profile])
    config = _switch_config()
    compiled = _compiled(config, registry=registry)
    with pytest.raises(DiscoveryError, match="unknown homeassistant component"):
        build_model_discovery(compiled, registry, config, DiscoveryOptions())


def test_profile_without_a_homeassistant_component_is_skipped() -> None:
    # A future tier-2/3 profile with no homeassistant: block at all -- must not raise, just
    # produce no discovery for that entity, the same way a channel with no profile match does.
    no_ha_profile = parse_profile(
        {
            "id": "no_ha_mapping",
            "functions": ["FID_SWITCH_ACTUATOR"],
            "attributes": {"state": {"pairing": "AL_INFO_ON_OFF", "codec": "bool01"}},
        },
        source="<test>",
    )
    registry = build_registry([no_ha_profile])
    config = _switch_config()
    compiled = _compiled(config, registry=registry)
    model = build_model_discovery(compiled, registry, config, DiscoveryOptions())
    assert model.discovery == ()
    assert model.entities[0].discovery == ()


# ------------------------------------------------------------------------------ DiscoveryPublisher


async def test_publish_changed_publishes_a_new_topic_once(tmp_path: Path) -> None:
    async with running_fake_broker() as broker:
        client = _client(broker)
        task = asyncio.create_task(client.run())
        try:
            await _wait_until(lambda: client.reconnect_count >= 1)
            store = DiscoveryStore(tmp_path / "discovery.json")
            publisher = DiscoveryPublisher(mqtt=client, store=store)
            config = _switch_config()
            model = build_model_discovery(_compiled(config), REGISTRY, config, DiscoveryOptions())
            topic = model.discovery[0][0]

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer:
                await observer.subscribe(topic)
                async with asyncio.timeout(5.0):
                    async_iter = aiter(observer.messages)
                    await publisher.publish_changed(model)
                    msg = await anext(async_iter)
                assert msg.payload == model.discovery[0][1]

            # A second publish_changed with the SAME model must publish nothing.
            await publisher.publish_changed(model)
            assert store.hashes[topic] is not None
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_publish_changed_uses_the_configured_qos(tmp_path: Path) -> None:
    async with running_fake_broker() as broker:
        client = _client(broker)
        task = asyncio.create_task(client.run())
        try:
            await _wait_until(lambda: client.reconnect_count >= 1)
            store = DiscoveryStore(tmp_path / "discovery.json")
            publisher = DiscoveryPublisher(mqtt=client, store=store, qos=0)
            config = _switch_config()
            model = build_model_discovery(_compiled(config), REGISTRY, config, DiscoveryOptions())
            topic = model.discovery[0][0]

            await publisher.publish_changed(model)
            await _wait_until(lambda: broker.retained_messages.get(topic) is not None)
            assert broker.retained_messages[topic].qos == 0
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_publish_changed_skips_an_unchanged_topic(tmp_path: Path) -> None:
    async with running_fake_broker() as broker:
        client = _client(broker)
        task = asyncio.create_task(client.run())
        try:
            await _wait_until(lambda: client.reconnect_count >= 1)
            store = DiscoveryStore(tmp_path / "discovery.json")
            publisher = DiscoveryPublisher(mqtt=client, store=store)
            config = _switch_config()
            model = build_model_discovery(_compiled(config), REGISTRY, config, DiscoveryOptions())
            topic, payload = model.discovery[0]
            store.mark(topic, payload)

            published: list[str] = []
            original_publish = client.publish

            async def _tracking_publish(topic_: str, payload_: bytes, **kwargs: Any) -> None:
                published.append(topic_)
                await original_publish(topic_, payload_, **kwargs)

            client.publish = _tracking_publish  # type: ignore[method-assign]
            await publisher.publish_changed(model)
            assert published == []
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_publish_all_republishes_regardless_of_store_state(tmp_path: Path) -> None:
    async with running_fake_broker() as broker:
        client = _client(broker)
        task = asyncio.create_task(client.run())
        try:
            await _wait_until(lambda: client.reconnect_count >= 1)
            store = DiscoveryStore(tmp_path / "discovery.json")
            publisher = DiscoveryPublisher(mqtt=client, store=store)
            config = _switch_config()
            model = build_model_discovery(_compiled(config), REGISTRY, config, DiscoveryOptions())
            topic, payload = model.discovery[0]
            store.mark(topic, payload)  # already "seen" -- publish_changed would skip it

            published: list[str] = []
            original_publish = client.publish

            async def _tracking_publish(topic_: str, payload_: bytes, **kwargs: Any) -> None:
                published.append(topic_)
                await original_publish(topic_, payload_, **kwargs)

            client.publish = _tracking_publish  # type: ignore[method-assign]
            await publisher.publish_all(model)
            assert published == [topic]
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_retract_publishes_empty_payload_and_forgets_the_topic(tmp_path: Path) -> None:
    async with running_fake_broker() as broker:
        client = _client(broker)
        task = asyncio.create_task(client.run())
        try:
            await _wait_until(lambda: client.reconnect_count >= 1)
            store = DiscoveryStore(tmp_path / "discovery.json")
            store.mark("homeassistant/switch/x/config", b'{"a":1}')
            publisher = DiscoveryPublisher(mqtt=client, store=store)

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer:
                await observer.subscribe("homeassistant/switch/x/config")
                async with asyncio.timeout(5.0):
                    async_iter = aiter(observer.messages)
                    await publisher.retract(["homeassistant/switch/x/config"])
                    msg = await anext(async_iter)
                assert msg.payload == b""
            assert "homeassistant/switch/x/config" not in store.hashes
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


def test_stale_topics_finds_a_topic_the_store_remembers_but_the_model_no_longer_has(
    tmp_path: Path,
) -> None:
    store = DiscoveryStore(tmp_path / "discovery.json")
    store.mark("homeassistant/switch/gone/config", b'{"a":1}')
    config = _switch_config()
    model = build_model_discovery(_compiled(config), REGISTRY, config, DiscoveryOptions())
    publisher = DiscoveryPublisher(mqtt=None, store=store)  # type: ignore[arg-type]
    stale = publisher.stale_topics(model)
    assert stale == ["homeassistant/switch/gone/config"]


async def test_publish_all_with_no_discovery_does_not_touch_the_store(tmp_path: Path) -> None:
    store = DiscoveryStore(tmp_path / "discovery.json")
    config = _switch_config()
    model = build_model_discovery(
        _compiled(config), REGISTRY, config, DiscoveryOptions(enabled=False)
    )
    publisher = DiscoveryPublisher(mqtt=None, store=store)  # type: ignore[arg-type]
    await publisher.publish_all(model)  # no discovery topics -- must not call mqtt or save
    assert store.hashes == {}


async def test_retract_with_no_topics_does_not_touch_the_store(tmp_path: Path) -> None:
    store = DiscoveryStore(tmp_path / "discovery.json")
    publisher = DiscoveryPublisher(mqtt=None, store=store)  # type: ignore[arg-type]
    await publisher.retract([])
    assert store.hashes == {}
