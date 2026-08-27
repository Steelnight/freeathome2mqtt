"""Tests for availability.py: bridge + per-device availability (ADR-008; docs/06 §1, §5;
docs/11 WP8).
"""

from __future__ import annotations

import asyncio

import aiomqtt
import orjson
import pytest

from fakes.fake_broker import running_fake_broker
from freeathome2mqtt.availability import (
    BridgeAvailability,
    DeviceAvailabilityPublisher,
    device_availability,
)
from freeathome2mqtt.model.entity import AttrKind, Entity
from freeathome2mqtt.mqtt.client import MqttClient

BASE = "freeathome2mqtt"
SERIAL = "ABB7F500E17A"


def _entity(idx: int, *, availability: bool) -> Entity:
    return Entity(
        idx=idx,
        id=f"{SERIAL}_ch{idx:04d}",
        profile="test_profile",
        name="Test",
        area=None,
        device_serial=SERIAL,
        channel_id=f"ch{idx:04d}",
        attr_names=("state",),
        attr_kinds=(AttrKind.STATE,),
        state_topic=f"{BASE}/test{idx}",
        set_topic=f"{BASE}/test{idx}/set",
        get_topic=f"{BASE}/test{idx}/get",
        availability_topic=f"{BASE}/test{idx}/availability" if availability else None,
        optimistic=False,
        discovery=(),
    )


async def _wait_until(predicate, *, timeout_seconds: float = 5.0, interval: float = 0.005) -> None:
    async with asyncio.timeout(timeout_seconds):
        while not predicate():  # noqa: ASYNC110 -- generic poll, no single event to await
            await asyncio.sleep(interval)


async def _connected_client(broker) -> tuple[MqttClient, asyncio.Task[None]]:
    client = MqttClient(
        host="127.0.0.1",
        port=broker.port,
        base_topic=BASE,
        sysap_serial=SERIAL,
        backoff_initial=0.02,
        backoff_cap=0.2,
    )
    task = asyncio.create_task(client.run())
    await _wait_until(lambda: client.reconnect_count >= 1)
    return client, task


# --------------------------------------------------------------------------- BridgeAvailability


async def test_online_only_when_all_three_signals_are_true() -> None:
    async with running_fake_broker() as broker:
        client, task = await _connected_client(broker)
        try:
            availability = BridgeAvailability(mqtt=client, base_topic=BASE, grace_seconds=10.0)
            assert availability.online is False
            availability.set_mqtt_connected(True)
            assert availability.online is False
            availability.set_sysap_connected(True)
            assert availability.online is False
            availability.set_model_loaded(True)
            assert availability.online is True
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_publish_now_publishes_online_once_all_signals_are_true() -> None:
    async with running_fake_broker() as broker:
        client, task = await _connected_client(broker)
        try:
            availability = BridgeAvailability(mqtt=client, base_topic=BASE)
            async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer:
                await observer.subscribe(f"{BASE}/bridge/state")
                availability.set_mqtt_connected(True)
                availability.set_sysap_connected(True)
                availability.set_model_loaded(True)
                await availability.publish_now()

                async with asyncio.timeout(5.0):
                    msg = await anext(aiter(observer.messages))
                assert orjson.loads(msg.payload) == {"state": "online"}
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_publish_now_is_a_noop_when_unchanged() -> None:
    async with running_fake_broker() as broker:
        client, task = await _connected_client(broker)
        try:
            availability = BridgeAvailability(mqtt=client, base_topic=BASE)
            availability.set_mqtt_connected(True)
            availability.set_sysap_connected(True)
            availability.set_model_loaded(True)
            await availability.publish_now()
            await availability.publish_now()  # unchanged -- must not publish a second time

            assert client.last_published(f"{BASE}/bridge/state") == orjson.dumps(
                {"state": "online"}
            )
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_going_offline_is_held_for_the_grace_period() -> None:
    async with running_fake_broker() as broker:
        client, task = await _connected_client(broker)
        try:
            availability = BridgeAvailability(mqtt=client, base_topic=BASE, grace_seconds=0.2)
            availability.set_mqtt_connected(True)
            availability.set_sysap_connected(True)
            availability.set_model_loaded(True)
            await availability.publish_now()

            availability.set_sysap_connected(False)
            await asyncio.sleep(0.05)  # well inside the grace window
            assert client.last_published(f"{BASE}/bridge/state") == orjson.dumps(
                {"state": "online"}
            )

            await asyncio.sleep(0.3)  # grace period has now elapsed
            assert client.last_published(f"{BASE}/bridge/state") == orjson.dumps(
                {"state": "offline"}
            )
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_reconnecting_within_the_grace_period_never_publishes_offline() -> None:
    async with running_fake_broker() as broker:
        client, task = await _connected_client(broker)
        try:
            availability = BridgeAvailability(mqtt=client, base_topic=BASE, grace_seconds=0.1)
            availability.set_mqtt_connected(True)
            availability.set_sysap_connected(True)
            availability.set_model_loaded(True)
            await availability.publish_now()

            availability.set_sysap_connected(False)
            await asyncio.sleep(0.02)
            availability.set_sysap_connected(True)  # recovers before the grace timer fires
            await asyncio.sleep(0.2)

            assert client.last_published(f"{BASE}/bridge/state") == orjson.dumps(
                {"state": "online"}
            )
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_publish_forced_offline_bypasses_the_change_only_guard() -> None:
    async with running_fake_broker() as broker:
        client, task = await _connected_client(broker)
        try:
            availability = BridgeAvailability(mqtt=client, base_topic=BASE)
            await availability.publish_forced_offline()  # never was online -- still publishes
            assert client.last_published(f"{BASE}/bridge/state") == orjson.dumps(
                {"state": "offline"}
            )
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_bridge_state_is_retained_qos_1() -> None:
    async with running_fake_broker() as broker:
        client, task = await _connected_client(broker)
        try:
            availability = BridgeAvailability(mqtt=client, base_topic=BASE)
            availability.set_mqtt_connected(True)
            availability.set_sysap_connected(True)
            availability.set_model_loaded(True)
            await availability.publish_now()
            await asyncio.sleep(0.05)
            assert f"{BASE}/bridge/state" in broker.retained_messages
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


# ---------------------------------------------------------------------- per-device availability


def test_device_availability_reports_online_by_default() -> None:
    assert device_availability({}) == device_availability({"unresponsive": False, "defect": False})


def test_device_availability_reports_unresponsive() -> None:
    result = device_availability({"unresponsive": True, "unresponsiveCounter": 7})
    assert result.state == "offline"
    assert result.reason == "unresponsive"
    assert result.unresponsive_counter == 7


def test_device_availability_defect_takes_priority_over_unresponsive() -> None:
    result = device_availability({"unresponsive": True, "defect": True})
    assert result.reason == "defect"


async def test_device_availability_publisher_publishes_only_entities_with_availability() -> None:
    async with running_fake_broker() as broker:
        client, task = await _connected_client(broker)
        try:
            publisher = DeviceAvailabilityPublisher(mqtt=client)
            entities = [_entity(0, availability=True), _entity(1, availability=False)]
            await publisher.publish(entities, {SERIAL: {"unresponsive": True}})
            await asyncio.sleep(0.05)

            assert client.last_published(f"{BASE}/test0/availability") is not None
            assert client.last_published(f"{BASE}/test1/availability") is None
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_device_availability_publisher_is_change_only() -> None:
    async with running_fake_broker() as broker:
        client, task = await _connected_client(broker)
        try:
            publisher = DeviceAvailabilityPublisher(mqtt=client)
            entities = [_entity(0, availability=True)]

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer:
                await observer.subscribe(f"{BASE}/test0/availability")
                messages = observer.messages

                await publisher.publish(entities, {SERIAL: {}})
                async with asyncio.timeout(5.0):
                    await anext(aiter(messages))  # the one real publish

                await publisher.publish(entities, {SERIAL: {}})  # unchanged -- no second message
                with pytest.raises(TimeoutError):
                    async with asyncio.timeout(0.2):
                        await anext(aiter(messages))
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)
