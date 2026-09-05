"""Tests for bus/events.py: the non-coalescing edge path (ADR-005; docs/04 §2.1; docs/11 WP5;
closes P-32).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import aiomqtt
import orjson
import pytest

from fakes.fake_broker import running_fake_broker
from freeathome2mqtt.bus.events import EventPublisher
from freeathome2mqtt.model.entity import AttrKind, Entity
from freeathome2mqtt.mqtt.client import MqttClient

BASE = "freeathome2mqtt"


def _entity() -> Entity:
    return Entity(
        idx=0,
        id="ABB7F500E17A_ch0002",
        profile="switch_sensor",
        name="Wall Switch",
        area=None,
        device_serial="ABB7F500E17A",
        channel_id="ch0002",
        attr_names=("state",),
        attr_kinds=(AttrKind.EVENT,),
        state_topic=f"{BASE}/wall_switch",
        set_topic=f"{BASE}/wall_switch/set",
        get_topic=f"{BASE}/wall_switch/get",
        availability_topic=None,
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
        sysap_serial="ABB7F500E17A",
        backoff_initial=0.02,
        backoff_cap=0.2,
    )
    task = asyncio.create_task(client.run())
    await _wait_until(lambda: client.reconnect_count >= 1)
    return client, task


async def test_events_are_published_to_the_entity_event_topic() -> None:
    entity = _entity()
    fixed = datetime(2026, 8, 23, 14, 31, 2, 118000, tzinfo=UTC)
    async with running_fake_broker() as broker:
        client, task = await _connected_client(broker)
        try:
            events = EventPublisher(mqtt=client, base_topic=BASE, clock=lambda: fixed)

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer:
                await observer.subscribe(f"{BASE}/wall_switch/event")
                messages = observer.messages

                await events.emit(entity, attribute="state", value=1, event="press")

                async with asyncio.timeout(5.0):
                    msg = await anext(aiter(messages))
                assert str(msg.topic) == f"{BASE}/wall_switch/event"
                assert orjson.loads(msg.payload) == {
                    "id": "ABB7F500E17A_ch0002",
                    "event": "press",
                    "attribute": "state",
                    "value": 1,
                    "timestamp": "2026-08-23T14:31:02.118+00:00",
                }
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_events_are_not_retained() -> None:
    entity = _entity()
    async with running_fake_broker() as broker:
        client, task = await _connected_client(broker)
        try:
            events = EventPublisher(mqtt=client, base_topic=BASE)
            await events.emit(entity, attribute="state", value=1, event="press")
            await asyncio.sleep(0.1)
            assert f"{BASE}/wall_switch/event" not in broker.retained_messages

            # A subscriber that joins *after* the event must never see it replayed.
            async with aiomqtt.Client("127.0.0.1", port=broker.port) as latecomer:
                await latecomer.subscribe(f"{BASE}/wall_switch/event")
                with pytest.raises(TimeoutError):
                    async with asyncio.timeout(0.2):
                        await anext(aiter(latecomer.messages))
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_two_identical_presses_produce_two_messages() -> None:
    entity = _entity()
    async with running_fake_broker() as broker:
        client, task = await _connected_client(broker)
        try:
            events = EventPublisher(mqtt=client, base_topic=BASE)

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer:
                await observer.subscribe(f"{BASE}/wall_switch/event")
                received = []

                async def collect() -> None:
                    async for msg in observer.messages:
                        received.append(msg.payload)
                        if len(received) >= 2:
                            return

                collector = asyncio.create_task(collect())
                await events.emit(entity, attribute="state", value=1, event="press")
                await events.emit(entity, attribute="state", value=1, event="press")
                await asyncio.wait_for(collector, timeout=5.0)

                assert len(received) == 2  # no dedup, no change detection (ADR-005)
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_bridge_event_is_published_under_the_configured_base_topic() -> None:
    """WP15: `EventPublisher` learned to publish `bridge/event` so `bus/ingress.py` can report a
    scene trigger. Its base topic must be the configured one -- a default would put a real
    installation's events on the wrong topic silently.
    """
    async with running_fake_broker() as broker:
        client = MqttClient(
            host="127.0.0.1",
            port=broker.port,
            base_topic="custombase",
            sysap_serial="ABB7F500E17A",
            backoff_initial=0.02,
            backoff_cap=0.2,
        )
        task = asyncio.create_task(client.run())
        await _wait_until(lambda: client.reconnect_count >= 1)
        try:
            async with aiomqtt.Client("127.0.0.1", port=broker.port) as outsider:
                await outsider.subscribe("custombase/bridge/event")
                events = EventPublisher(mqtt=client, base_topic="custombase")
                await events.emit_bridge_event("scene_triggered", {"serial": "ABB1"})

                async with asyncio.timeout(5.0):
                    async for message in outsider.messages:
                        assert str(message.topic) == "custombase/bridge/event"
                        assert orjson.loads(message.payload) == {
                            "type": "scene_triggered",
                            "data": {"serial": "ABB1"},
                        }
                        break
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)
