"""Tests for mqtt/bridge_api.py: bridge/request/<command> dispatch, the response envelope, and
BridgeApiError -> status:error conversion (docs/02 §6; docs/04 §5; docs/11 WP9).

Uses a real embedded broker and a real `MqttClient`, per docs/10 §3.4 -- mocking the MQTT client
would hide exactly the retain/QoS/subscription bugs this project cares about. The handlers
themselves are fakes here; the real ones (reload, rename, ...) are exercised in test_supervisor.py.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from typing import Any

import aiomqtt
import orjson
import pytest

from fakes.fake_broker import running_fake_broker
from freeathome2mqtt.mqtt.bridge_api import BridgeApi, BridgeApiError
from freeathome2mqtt.mqtt.client import MqttClient

BASE = "freeathome2mqtt"
SERIAL = "ABB7F500E17A"


async def _wait_until(predicate: Any, *, timeout_seconds: float = 5.0) -> None:
    async with asyncio.timeout(timeout_seconds):
        while not predicate():  # noqa: ASYNC110 -- generic poll, no single event to await
            await asyncio.sleep(0.005)


class _Env:
    def __init__(
        self, *, mqtt_client: MqttClient, bridge_api: BridgeApi, outsider: aiomqtt.Client
    ) -> None:
        self.mqtt_client = mqtt_client
        self.bridge_api = bridge_api
        self.outsider = outsider

    async def request(
        self, command: str, body: dict[str, Any] | None = None, *, retain: bool = False
    ) -> None:
        topic = f"{BASE}/bridge/request/{command}"
        payload = orjson.dumps(body) if body is not None else b""
        async with aiomqtt.Client("127.0.0.1", port=self.mqtt_client._port) as sender:
            await sender.publish(topic, payload, retain=retain)

    async def next_response(self, *, timeout_seconds: float = 5.0) -> dict[str, Any]:
        async with asyncio.timeout(timeout_seconds):
            async for message in self.outsider.messages:
                return dict(orjson.loads(message.payload))
        raise AssertionError("unreachable")  # pragma: no cover


@contextlib.asynccontextmanager
async def _environment(handlers: dict[str, Any]) -> AsyncIterator[_Env]:
    holder: list[BridgeApi] = []

    def _forward(message: aiomqtt.Message) -> None:
        holder[0].on_message(message)

    async with running_fake_broker() as broker:
        mqtt_client = MqttClient(
            host="127.0.0.1",
            port=broker.port,
            base_topic=BASE,
            sysap_serial=SERIAL,
            backoff_initial=0.02,
            backoff_cap=0.2,
            on_message=_forward,
        )
        mqtt_task = asyncio.create_task(mqtt_client.run())
        await _wait_until(lambda: mqtt_client.reconnect_count >= 1)

        bridge_api = BridgeApi(base_topic=BASE, mqtt=mqtt_client, handlers=handlers)
        holder.append(bridge_api)

        async with aiomqtt.Client("127.0.0.1", port=broker.port) as outsider:
            await outsider.subscribe(f"{BASE}/bridge/response/#")
            try:
                yield _Env(mqtt_client=mqtt_client, bridge_api=bridge_api, outsider=outsider)
            finally:
                await mqtt_client.stop()
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.wait_for(mqtt_task, timeout=5.0)


async def test_dispatches_to_the_matching_handler_with_decoded_args() -> None:
    seen: list[dict[str, Any]] = []

    async def handler(args: dict[str, Any]) -> dict[str, Any]:
        seen.append(args)
        return {}

    async with _environment({"reload": handler}) as env:
        await env.request("reload", {"foo": "bar"})
        await env.next_response()
        assert seen == [{"foo": "bar"}]


async def test_publishes_ok_envelope_with_handler_data() -> None:
    async def handler(_args: dict[str, Any]) -> dict[str, Any]:
        return {"echo": 42}

    async with _environment({"health": handler}) as env:
        await env.request("health")
        response = await env.next_response()
        assert response == {"status": "ok", "data": {"echo": 42}}


async def test_echoes_transaction_when_supplied() -> None:
    async def handler(_args: dict[str, Any]) -> dict[str, Any]:
        return {}

    async with _environment({"reload": handler}) as env:
        await env.request("reload", {"transaction": "txn-1"})
        response = await env.next_response()
        assert response == {"status": "ok", "data": {}, "transaction": "txn-1"}


async def test_omits_transaction_when_not_supplied() -> None:
    async def handler(_args: dict[str, Any]) -> dict[str, Any]:
        return {}

    async with _environment({"reload": handler}) as env:
        await env.request("reload")
        response = await env.next_response()
        assert "transaction" not in response


async def test_unknown_command_gets_an_error_response() -> None:
    async with _environment({}) as env:
        await env.request("no_such_command")
        response = await env.next_response()
        assert response["status"] == "error"
        assert "no_such_command" in response["error"]


async def test_bridge_api_error_becomes_an_error_response() -> None:
    async def handler(_args: dict[str, Any]) -> dict[str, Any]:
        raise BridgeApiError("id is required")

    async with _environment({"entity/remove": handler}) as env:
        await env.request("entity/remove", {})
        response = await env.next_response()
        assert response == {"status": "error", "error": "id is required"}


async def test_multi_segment_command_name_is_dispatched_correctly() -> None:
    async def handler(args: dict[str, Any]) -> dict[str, Any]:
        return {"id": args["id"]}

    async with _environment({"entity/rename": handler}) as env:
        await env.request("entity/rename", {"id": "x", "name": "y"})
        response = await env.next_response()
        assert response == {"status": "ok", "data": {"id": "x"}}


async def test_empty_payload_is_treated_as_empty_args() -> None:
    seen: list[dict[str, Any]] = []

    async def handler(args: dict[str, Any]) -> dict[str, Any]:
        seen.append(args)
        return {}

    async with _environment({"reload": handler}) as env:
        await env.request("reload", None)
        await env.next_response()
        assert seen == [{}]


async def test_malformed_json_payload_is_treated_as_empty_args() -> None:
    async def handler(args: dict[str, Any]) -> dict[str, Any]:
        return {"got": args}

    async with _environment({"reload": handler}) as env:
        async with aiomqtt.Client("127.0.0.1", port=env.mqtt_client._port) as sender:
            await sender.publish(f"{BASE}/bridge/request/reload", b"{not json")
        response = await env.next_response()
        assert response == {"status": "ok", "data": {"got": {}}}


async def test_non_object_json_payload_is_treated_as_empty_args() -> None:
    async def handler(args: dict[str, Any]) -> dict[str, Any]:
        return {"got": args}

    async with _environment({"reload": handler}) as env:
        async with aiomqtt.Client("127.0.0.1", port=env.mqtt_client._port) as sender:
            await sender.publish(f"{BASE}/bridge/request/reload", orjson.dumps([1, 2, 3]))
        response = await env.next_response()
        assert response == {"status": "ok", "data": {"got": {}}}


async def test_non_bridge_request_topics_are_ignored() -> None:
    async def reload_handler(_args: dict[str, Any]) -> dict[str, Any]:
        raise AssertionError("should never be called")

    async def health_handler(_args: dict[str, Any]) -> dict[str, Any]:
        return {}

    async with _environment({"reload": reload_handler, "health": health_handler}) as env:
        async with aiomqtt.Client("127.0.0.1", port=env.mqtt_client._port) as sender:
            await sender.publish(f"{BASE}/some_entity/set", b"1")
        # A real request afterwards gives us something to wait for -- if the /set message had
        # wrongly triggered "reload", its AssertionError would have crashed the dispatch task
        # already by the time this response arrives.
        await env.request("health")
        response = await env.next_response()
        assert response == {"status": "ok", "data": {}}


async def test_unexpected_exception_in_a_handler_is_not_swallowed() -> None:
    async def handler(_args: dict[str, Any]) -> dict[str, Any]:
        raise RuntimeError("boom")

    async with _environment({"reload": handler}) as env:
        with pytest.raises(RuntimeError, match="boom"):
            await env.bridge_api._dispatch("reload", b"{}")
