"""Tests for mqtt/client.py: MqttClient -- LWT, narrow subscriptions, resubscribe, MQTT5/3.1.1
fallback, client id, retained republish, wildcard guard (docs/04; docs/06 §3; docs/11 WP5).
"""

from __future__ import annotations

import asyncio

import aiomqtt
import pytest

from fakes.fake_broker import running_fake_broker
from freeathome2mqtt.mqtt.client import MqttClient, MqttClientNotConnectedError
from freeathome2mqtt.mqtt.topics import (
    WildcardTopicError,
    command_subscriptions,
    raw_command_subscription,
)

BASE = "freeathome2mqtt"
SERIAL = "ABB7F500E17A"

# amqtt (the in-process test broker, docs/10 §3.4) never persists sessions or subscriptions
# across a fresh Broker instance, so every connect looks like a first connect from its side --
# exactly the scenario these small backoff/delay values let us exercise quickly.
_FAST = {"backoff_initial": 0.02, "backoff_factor": 2.0, "backoff_cap": 0.2}


def _client_for(broker, **kwargs) -> MqttClient:
    return MqttClient(
        host="127.0.0.1", port=broker.port, base_topic=BASE, sysap_serial=SERIAL, **_FAST, **kwargs
    )


async def _wait_until(predicate, *, timeout_seconds: float = 5.0, interval: float = 0.01) -> None:
    async with asyncio.timeout(timeout_seconds):
        while not predicate():  # noqa: ASYNC110 -- generic poll, no single event to await
            await asyncio.sleep(interval)


async def test_client_id_includes_sysap_serial() -> None:
    async with running_fake_broker() as broker:
        client = _client_for(broker)
        assert client.client_id == "freeathome2mqtt_ABB7F500E17A"


async def test_explicit_client_id_overrides_the_derived_default() -> None:
    async with running_fake_broker() as broker:
        client = _client_for(broker, client_id="my-fixed-id")
        assert client.client_id == "my-fixed-id"


async def test_bridge_subscribes_only_to_command_topics() -> None:
    async with running_fake_broker() as broker:
        client = _client_for(broker)
        task = asyncio.create_task(client.run())
        try:
            await _wait_until(lambda: client.reconnect_count >= 1)
            await _wait_until(lambda: set(broker.subscriptions) == set(command_subscriptions(BASE)))
            assert set(broker.subscriptions) == set(command_subscriptions(BASE))
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_raw_command_topic_subscribed_only_when_raw_mode_enabled() -> None:
    async with running_fake_broker() as broker:
        client = _client_for(broker, raw_mode_enabled=True)
        task = asyncio.create_task(client.run())
        try:
            await _wait_until(lambda: raw_command_subscription(BASE) in broker.subscriptions)
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_raw_command_topic_not_subscribed_by_default() -> None:
    async with running_fake_broker() as broker:
        client = _client_for(broker)
        assert raw_command_subscription(BASE) not in client.subscriptions


async def test_homeassistant_birth_topic_subscribed_when_configured() -> None:
    async with running_fake_broker() as broker:
        client = _client_for(broker, homeassistant_discovery_topic="homeassistant")
        task = asyncio.create_task(client.run())
        try:
            await _wait_until(lambda: "homeassistant/status" in broker.subscriptions)
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_resubscribe_after_reconnect() -> None:
    # Simulate a broker restart (docs/09 P-27): a fresh broker instance on the same port
    # remembers nothing -- no subscriptions -- until the client re-subscribes on its own. The two
    # `running_fake_broker` scopes are sequential, not nested, so broker1 fully releases the port
    # before broker2 binds it.
    async with running_fake_broker() as broker1:
        port = broker1.port
        client = _client_for(broker1)
        task = asyncio.create_task(client.run())
        await _wait_until(lambda: client.reconnect_count >= 1)
        await _wait_until(lambda: set(broker1.subscriptions) == set(command_subscriptions(BASE)))

    try:
        async with running_fake_broker(port=port) as broker2:
            await _wait_until(lambda: client.reconnect_count >= 2, timeout_seconds=10.0)
            await _wait_until(
                lambda: set(broker2.subscriptions) == set(command_subscriptions(BASE)),
                timeout_seconds=10.0,
            )
            assert set(broker2.subscriptions) == set(command_subscriptions(BASE))
    finally:
        await client.stop()
        await asyncio.wait_for(task, timeout=5.0)


async def test_retained_republish_after_reconnect() -> None:
    topic = f"{BASE}/kueche_deckenlicht"
    payload = b'{"id":"x","state":true}'
    async with running_fake_broker() as broker1:
        port = broker1.port
        client = _client_for(broker1, retained_republish_delay=0.05)
        task = asyncio.create_task(client.run())
        await _wait_until(lambda: client.reconnect_count >= 1)
        await client.publish(topic, payload, qos=0, retain=True)
        await _wait_until(lambda: broker1.retained_messages.get(topic) is not None)

    try:
        async with running_fake_broker(port=port) as broker2:
            await _wait_until(lambda: client.reconnect_count >= 2, timeout_seconds=10.0)
            await _wait_until(
                lambda: broker2.retained_messages.get(topic) is not None, timeout_seconds=10.0
            )
            assert broker2.retained_messages[topic].data == payload
    finally:
        await client.stop()
        await asyncio.wait_for(task, timeout=5.0)


async def test_force_disable_retain_downgrades_every_retained_publish() -> None:
    topic = f"{BASE}/kueche_deckenlicht"
    async with running_fake_broker() as broker:
        client = _client_for(broker, force_disable_retain=True)
        task = asyncio.create_task(client.run())
        try:
            await _wait_until(lambda: client.reconnect_count >= 1)
            await client.publish(topic, b'{"state":true}', qos=0, retain=True)
            assert broker.retained_messages.get(topic) is None
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_publish_rejects_wildcard_topics() -> None:
    async with running_fake_broker() as broker:
        client = _client_for(broker)
        with pytest.raises(WildcardTopicError):
            await client.publish(f"{BASE}/+/set", b"x")


async def test_publish_raises_when_not_connected() -> None:
    async with running_fake_broker() as broker:
        client = _client_for(broker)
        with pytest.raises(MqttClientNotConnectedError):
            await client.publish(f"{BASE}/kueche_deckenlicht", b"x")


async def test_last_published_bytes_are_tracked() -> None:
    topic = f"{BASE}/kueche_deckenlicht"
    async with running_fake_broker() as broker:
        client = _client_for(broker)
        task = asyncio.create_task(client.run())
        try:
            await _wait_until(lambda: client.reconnect_count >= 1)
            assert client.last_published(topic) is None
            await client.publish(topic, b'{"state":true}')
            assert client.last_published(topic) == b'{"state":true}'
            await client.publish(topic, b'{"state":false}')
            assert client.last_published(topic) == b'{"state":false}'
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_connects_with_a_will_and_an_explicit_client_id() -> None:
    # mqtt/client.py's module docstring explains why this is MQTT 3.1.1 rather than 5 for now.
    async with running_fake_broker() as broker:
        client = _client_for(broker)
        task = asyncio.create_task(client.run())
        try:
            await _wait_until(lambda: client.reconnect_count >= 1)
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_on_message_receives_only_subscribed_topics() -> None:
    received: list[str] = []
    async with running_fake_broker() as broker:
        client = _client_for(broker, on_message=lambda msg: received.append(str(msg.topic)))
        task = asyncio.create_task(client.run())
        try:
            await _wait_until(lambda: client.reconnect_count >= 1)
            await _wait_until(lambda: set(broker.subscriptions) == set(command_subscriptions(BASE)))

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as outsider:
                await outsider.publish(f"{BASE}/kueche_deckenlicht/set", b'{"state":true}')
                await outsider.publish(f"{BASE}/kueche_deckenlicht", b"should not be received")
                await outsider.publish(f"{BASE}/kueche_deckenlicht/get", b"{}")
                await asyncio.sleep(0.2)

            assert received == [
                f"{BASE}/kueche_deckenlicht/set",
                f"{BASE}/kueche_deckenlicht/get",
            ]
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)
