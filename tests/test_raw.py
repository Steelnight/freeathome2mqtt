"""Tests for bus/raw.py: the `advanced.raw_mode` pressure valve (docs/04 §7; ADR-011; docs/11
WP11). `build_raw_map` is pure (docs/02 §2); `RawStatePublisher`/`RawCommandHandler` are the
publish/subscribe halves, tested in isolation like `bus/events.py`'s `EventPublisher`.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from freeathome2mqtt.bus.raw import RawCommandHandler, RawStatePublisher, build_raw_map
from freeathome2mqtt.model.compiler import CompileOptions, compile
from freeathome2mqtt.model.profiles import build_registry, parse_profile
from freeathome2mqtt.mqtt.client import MqttClientNotConnectedError
from freeathome2mqtt.sysap.rest import SysApError

SERIAL = "ABB7F500E17A"
BASE = "freeathome2mqtt"

_SWITCH_PROFILE = parse_profile(
    {
        "id": "switch_actuator",
        "functions": ["FID_SWITCH_ACTUATOR"],
        "attributes": {"state": {"pairing": "AL_INFO_ON_OFF", "codec": "bool01"}},
        "commands": {
            "state": {"pairing": "AL_SWITCH_ON_OFF", "codec": "bool01", "optimistic": "state"}
        },
    },
    source="<test>",
)
REGISTRY = build_registry([_SWITCH_PROFILE])


def _config() -> dict[str, Any]:
    return {
        "floorplan": {"floors": {"01": {"name": "GF", "rooms": {"0C": {"name": "Room"}}}}},
        "devices": {
            SERIAL: {
                "displayName": "Test",
                "interface": "TP",
                "floor": "01",
                "room": "0C",
                "channels": {
                    "ch0000": {  # supported: matches switch_actuator
                        "displayName": "Switch",
                        "functionID": "7",
                        "inputs": {"idp0000": {"pairingID": 1, "value": "0"}},
                        "outputs": {"odp0000": {"pairingID": 256, "value": "1"}},
                        "floor": "01",
                        "room": "0C",
                    },
                    "ch0001": {  # unsupported: no profile claims this function
                        "displayName": "Mystery",
                        "functionID": "9d",
                        "inputs": {"idp0010": {"pairingID": 1, "value": "0"}},
                        "outputs": {"odp0010": {"pairingID": 53, "value": "1"}},
                        "floor": "01",
                        "room": "0C",
                    },
                },
            }
        },
    }


def _compiled() -> Any:
    return compile(_config(), REGISTRY, CompileOptions(topic_prefix=BASE))


# ---------------------------------------------------------------------------------- build_raw_map


def test_build_raw_map_is_empty_when_mode_is_false() -> None:
    raw_map = build_raw_map(_config(), _compiled(), mode=False, base_topic=BASE)
    assert raw_map.state_topics == {}
    assert raw_map.writable_channels == frozenset()


def test_build_raw_map_true_covers_every_channel() -> None:
    raw_map = build_raw_map(_config(), _compiled(), mode=True, base_topic=BASE)
    assert raw_map.state_topics[f"{SERIAL}/ch0000/odp0000"] == f"{BASE}/raw/{SERIAL}/ch0000/odp0000"
    assert raw_map.state_topics[f"{SERIAL}/ch0001/odp0010"] == f"{BASE}/raw/{SERIAL}/ch0001/odp0010"
    assert (SERIAL, "ch0000") in raw_map.writable_channels
    assert (SERIAL, "ch0001") in raw_map.writable_channels


def test_build_raw_map_unsupported_only_skips_the_matched_channel() -> None:
    raw_map = build_raw_map(_config(), _compiled(), mode="unsupported_only", base_topic=BASE)
    assert f"{SERIAL}/ch0000/odp0000" not in raw_map.state_topics
    assert (SERIAL, "ch0000") not in raw_map.writable_channels
    assert raw_map.state_topics[f"{SERIAL}/ch0001/odp0010"] == f"{BASE}/raw/{SERIAL}/ch0001/odp0010"
    assert (SERIAL, "ch0001") in raw_map.writable_channels


def test_build_raw_map_only_covers_outputs_not_inputs() -> None:
    # docs/01 §4.1: the WebSocket only ever streams changes to outputs; a raw *state* topic for
    # an input would simply never publish anything.
    raw_map = build_raw_map(_config(), _compiled(), mode=True, base_topic=BASE)
    assert f"{SERIAL}/ch0000/idp0000" not in raw_map.state_topics


# ----------------------------------------------------------------------------- RawStatePublisher


async def test_raw_state_publisher_publishes_the_raw_string_retained() -> None:
    mqtt = AsyncMock()
    publisher = RawStatePublisher(mqtt=mqtt, topics={"K": "freeathome2mqtt/raw/K"})
    assert publisher.topic_for("K") == "freeathome2mqtt/raw/K"
    assert publisher.topic_for("missing") is None

    await publisher.publish("freeathome2mqtt/raw/K", "42")

    mqtt.publish.assert_awaited_once_with("freeathome2mqtt/raw/K", b"42", qos=0, retain=True)


async def test_raw_state_publisher_logs_and_survives_disconnected_mqtt(
    caplog: pytest.LogCaptureFixture,
) -> None:
    mqtt = AsyncMock()
    mqtt.publish.side_effect = MqttClientNotConnectedError("freeathome2mqtt/raw/K")
    publisher = RawStatePublisher(mqtt=mqtt, topics={})

    with caplog.at_level("WARNING"):
        await publisher.publish("freeathome2mqtt/raw/K", "42")  # must not raise

    assert "MQTT not connected" in caplog.text


# ---------------------------------------------------------------------------- RawCommandHandler


class _FakeMessage:
    def __init__(self, topic: str, payload: bytes) -> None:
        self.topic = topic
        self.payload = payload


async def _wait_until(predicate, *, timeout_seconds: float = 5.0, interval: float = 0.005) -> None:
    async with asyncio.timeout(timeout_seconds):
        while not predicate():  # noqa: ASYNC110 -- generic poll, no single event to await
            await asyncio.sleep(interval)


async def test_raw_command_handler_writes_through_rest_for_a_writable_channel() -> None:
    rest = AsyncMock()
    handler = RawCommandHandler(
        rest=rest, base_topic=BASE, writable_channels=frozenset({(SERIAL, "ch0001")})
    )

    handler.on_message(_FakeMessage(f"{BASE}/raw/{SERIAL}/ch0001/idp0010/set", b"1"))
    await _wait_until(lambda: rest.put_datapoint.await_count >= 1)

    rest.put_datapoint.assert_awaited_once_with(f"{SERIAL}.ch0001.idp0010", "1")


async def test_raw_command_handler_ignores_a_channel_not_in_writable_channels() -> None:
    rest = AsyncMock()
    handler = RawCommandHandler(rest=rest, base_topic=BASE, writable_channels=frozenset())

    handler.on_message(_FakeMessage(f"{BASE}/raw/{SERIAL}/ch0001/idp0010/set", b"1"))
    await asyncio.sleep(0.05)

    rest.put_datapoint.assert_not_awaited()


async def test_raw_command_handler_ignores_an_unrelated_topic() -> None:
    rest = AsyncMock()
    handler = RawCommandHandler(
        rest=rest, base_topic=BASE, writable_channels=frozenset({(SERIAL, "ch0001")})
    )

    handler.on_message(_FakeMessage(f"{BASE}/some_entity/set", b"1"))
    await asyncio.sleep(0.05)

    rest.put_datapoint.assert_not_awaited()


async def test_raw_command_handler_ignores_a_malformed_raw_topic() -> None:
    # Right prefix, wrong shape: too few parts, or the terminal segment isn't "set".
    rest = AsyncMock()
    handler = RawCommandHandler(
        rest=rest, base_topic=BASE, writable_channels=frozenset({(SERIAL, "ch0001")})
    )

    handler.on_message(_FakeMessage(f"{BASE}/raw/{SERIAL}/ch0001/idp0010", b"1"))
    await asyncio.sleep(0.05)

    rest.put_datapoint.assert_not_awaited()


async def test_raw_command_handler_logs_and_survives_a_sysap_error(
    caplog: pytest.LogCaptureFixture,
) -> None:
    rest = AsyncMock()
    rest.put_datapoint.side_effect = SysApError("nope")
    handler = RawCommandHandler(
        rest=rest, base_topic=BASE, writable_channels=frozenset({(SERIAL, "ch0001")})
    )

    with caplog.at_level("WARNING"):
        handler.on_message(_FakeMessage(f"{BASE}/raw/{SERIAL}/ch0001/idp0010/set", b"1"))
        await _wait_until(lambda: rest.put_datapoint.await_count >= 1)
        await asyncio.sleep(0.02)  # let the task's except-branch actually run

    assert f"{SERIAL}.ch0001.idp0010" in caplog.text
