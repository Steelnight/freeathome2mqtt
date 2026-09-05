"""Tests for supervisor.py: startup order, task supervision, resync/reload, shutdown (ADR-001;
docs/02 §3, §7-8; docs/06 §3-4; docs/11 WP8).
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import logging
import time
from pathlib import Path
from typing import Any

import aiohttp
import aiomqtt
import orjson
import pytest

from fakes.fake_broker import free_port, running_fake_broker
from fakes.fake_sysap import FakeSysAp, running_fake_sysap
from freeathome2mqtt.metrics import LatencyHistogram, Metrics
from freeathome2mqtt.model.compiler import CompileOptions, CompileStats, Model
from freeathome2mqtt.model.compiler import compile as compile_model
from freeathome2mqtt.model.entity import AttrKind, Entity
from freeathome2mqtt.model.profiles import load_profile_registry
from freeathome2mqtt.mqtt.bridge_api import BridgeApiError
from freeathome2mqtt.mqtt.client import MqttClient
from freeathome2mqtt.supervisor import (
    _ESCALATION_THRESHOLD,
    Supervisor,
    SupervisorConfig,
    TaskDiedTooManyTimesError,
    _build_bridge_devices,
    _ReloadDebouncer,
    _split_devices_payload,
    restart_on_failure,
)
from freeathome2mqtt.sysap.rest import AuthenticationError, ServerOverloadedError
from freeathome2mqtt.sysap.ws import WsAuthenticationError

PROFILES_DIR = Path(__file__).resolve().parent.parent / "src" / "freeathome2mqtt" / "profiles"
REGISTRY = load_profile_registry(PROFILES_DIR)

SERIAL = "ABB7F500E17A"
BASE = "freeathome2mqtt"
_UUID = "00000000-0000-0000-0000-000000000000"


# ------------------------------------------------------------------------------------- fixtures


def _floorplan() -> dict[str, Any]:
    return {"floors": {"01": {"name": "Ground Floor", "rooms": {"01": {"name": "Living Room"}}}}}


def _configuration(devices: dict[str, Any]) -> dict[str, Any]:
    return {
        "sysapName": "Test House",
        "sysap": {},
        "users": {},
        "floorplan": _floorplan(),
        "devices": devices,
    }


def _switch_device(serial: str, *, state: str = "0", name: str = "Switch") -> dict[str, Any]:
    return {
        "displayName": name,
        "interface": "TP",
        "unresponsive": False,
        "unresponsiveCounter": 0,
        "defect": False,
        "channels": {
            "ch0000": {
                "displayName": "Switch",
                "functionID": "7",  # FID_SWITCH_ACTUATOR
                "inputs": {"idp0000": {"pairingID": 1, "value": "0"}},
                "outputs": {"odp0000": {"pairingID": 256, "value": state}},
                "floor": "01",
                "room": "01",
            }
        },
        "floor": "01",
        "room": "01",
    }


def _dimmer_device(serial: str, *, state: str = "0", brightness: str = "0") -> dict[str, Any]:
    return {
        "displayName": "Dimmer",
        "interface": "TP",
        "unresponsive": False,
        "unresponsiveCounter": 0,
        "defect": False,
        "channels": {
            "ch0000": {
                "displayName": "Dimmer",
                "functionID": "12",  # FID_DIMMING_ACTUATOR = 18 = 0x12
                "inputs": {
                    "idp0000": {"pairingID": 1, "value": "0"},
                    "idp0001": {"pairingID": 17, "value": "0"},
                },
                "outputs": {
                    "odp0000": {"pairingID": 256, "value": state},
                    "odp0001": {"pairingID": 272, "value": brightness},
                },
                "floor": "01",
                "room": "01",
            }
        },
        "floor": "01",
        "room": "01",
    }


def _config(tmp_path: Path, mqtt_port: int, http_client: Any, **overrides: Any) -> SupervisorConfig:
    defaults: dict[str, Any] = {
        "sysap_base_url": str(http_client.make_url("")).rstrip("/"),
        "sysap_username": "installer",
        "sysap_password": "secret",
        "mqtt_host": "127.0.0.1",
        "mqtt_port": mqtt_port,
        "base_topic": BASE,
        "data_dir": tmp_path,
        "config_refresh_interval_s": 0,
        "grace_seconds": 0.2,
        "link_backoff_initial": 0.02,
        "link_backoff_cap": 0.1,
        "reload_debounce_s": 0.05,
        "reload_min_interval_s": 0.1,
        "command_debounce_s": 0.02,
        "reconcile_delay_s": 0.1,
    }
    defaults.update(overrides)
    return SupervisorConfig(**defaults)


async def _wait_until(predicate, *, timeout_seconds: float = 5.0, interval: float = 0.005) -> None:
    async with asyncio.timeout(timeout_seconds):
        while not predicate():  # noqa: ASYNC110 -- generic poll, no single event to await
            await asyncio.sleep(interval)


def _input_value(fake: FakeSysAp, channel: str, dp: str) -> str:
    value: str = fake._configuration["devices"][SERIAL]["channels"][channel]["inputs"][dp]["value"]
    return value


# ---------------------------------------------------------------------------------- startup order


async def test_lwt_armed_before_sysap_connect(tmp_path: Path) -> None:
    # docs/02 §7 step 3 is MQTT connect (LWT armed); the WS (step 4) doesn't even exist yet.
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._ws is not None)
            assert supervisor._mqtt is not None
            assert supervisor._mqtt.reconnect_count >= 1
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_no_events_lost_during_startup_window(tmp_path: Path) -> None:
    # P-22's cold-start variant: a change arriving on the WS after it connects but before the
    # configuration fetch completes must survive, buffered, and land in the compiled state.
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL, state="0")}))
        fake.set_latency(150)  # ms -- opens a window between "WS connected" and "config fetched"
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(
                lambda: supervisor._ws is not None and supervisor._ws.reconnect_count >= 1
            )
            await fake.push_ws_frame({"datapoints": {f"{SERIAL}/ch0000/odp0000": "1"}})
            await _wait_until(lambda: supervisor._cold_start_done, timeout_seconds=10.0)

            assert supervisor._model is not None and supervisor._state is not None
            idx = supervisor._model.by_id[f"{SERIAL}_ch0000"]
            assert supervisor._state.values[idx][0] is True
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


# ----------------------------------------------------------------------- jid Basic-auth fallback


async def test_jid_fallback_lets_startup_succeed_when_username_is_rejected(tmp_path: Path) -> None:
    # docs/01 §1.1 / F4: the configured username ("installer") is rejected, but its jid
    # (FakeSysAp's default "abc123@busch-jaeger.de") is accepted -- startup must still reach
    # bridge/state: online via the one-time retry.
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        fake.require_username("abc123@busch-jaeger.de")
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done, timeout_seconds=10.0)
            assert supervisor._rest is not None
            assert supervisor._ws is not None
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_startup_fails_fatally_when_jid_fallback_also_fails(tmp_path: Path) -> None:
    # Neither "installer" nor its jid is accepted -- must not hang or retry forever (docs/06 §3).
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        fake.require_username("someone-else-entirely")
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        with pytest.raises(ExceptionGroup) as excinfo:
            await asyncio.wait_for(supervisor.run(), timeout=5.0)
        assert any(isinstance(exc, WsAuthenticationError) for exc in excinfo.value.exceptions)


# ------------------------------------------------------------------------------------- raw_mode


async def test_raw_mode_true_publishes_raw_state_for_every_output(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL, state="0")}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client, raw_mode=True),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            raw_topic = f"{BASE}/raw/{SERIAL}/ch0000/odp0000"

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer:
                await observer.subscribe(raw_topic)

                async def _wait_for_raw_message() -> bytes:
                    async for message in observer.messages:
                        return bytes(message.payload)
                    raise AssertionError("no raw message received")

                waiter = asyncio.create_task(_wait_for_raw_message())
                await fake.push_ws_frame({"datapoints": {f"{SERIAL}/ch0000/odp0000": "1"}})
                payload = await asyncio.wait_for(waiter, timeout=5.0)

            assert payload == b"1"
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_raw_mode_false_publishes_no_raw_topic(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL, state="0")}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),  # raw_mode defaults to False
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            assert supervisor._mqtt is not None
            raw_topic = f"{BASE}/raw/{SERIAL}/ch0000/odp0000"

            await fake.push_ws_frame({"datapoints": {f"{SERIAL}/ch0000/odp0000": "1"}})
            await _wait_until(lambda: supervisor._state is not None)
            await asyncio.sleep(0.1)  # let any (unwanted) raw publish have a chance to land

            assert supervisor._mqtt.last_published(raw_topic) is None
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_raw_mode_true_set_topic_writes_through_to_the_sysap(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL, state="0")}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client, raw_mode=True),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            async with aiomqtt.Client("127.0.0.1", port=broker.port) as sender:
                await sender.publish(f"{BASE}/raw/{SERIAL}/ch0000/idp0000/set", b"1")
            await _wait_until(lambda: _input_value(fake, "ch0000", "idp0000") == "1")
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


# -------------------------------------------------------------------------------------- metrics


async def test_metrics_enabled_serves_live_counters_on_the_configured_port(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        metrics_port = free_port()
        supervisor = Supervisor(
            config=_config(
                tmp_path, broker.port, http_client, metrics_enabled=True, metrics_port=metrics_port
            ),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            await fake.push_ws_frame({"datapoints": {f"{SERIAL}/ch0000/odp0000": "1"}})
            await _wait_until(lambda: supervisor.metrics.datapoints_in >= 1)

            async with (
                aiohttp.ClientSession() as session,
                session.get(f"http://127.0.0.1:{metrics_port}/metrics") as response,
            ):
                assert response.status == 200
                body = await response.text()
            assert "freeathome2mqtt_datapoints_in 1" in body
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_metrics_disabled_by_default_serves_nothing(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),  # metrics_enabled defaults False
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            assert supervisor._metrics_server is None
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


# --------------------------------------------------------------------------------------- resync


async def test_resync_issues_exactly_one_request(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            config_path = "/fhapi/v1/api/rest/configuration"
            before = fake.request_count(config_path)

            await fake.drop_websocket()
            await _wait_until(
                lambda: supervisor._ws is not None and supervisor._ws.reconnect_count >= 2,
                timeout_seconds=5.0,
            )
            await _wait_until(
                lambda: fake.request_count(config_path) >= before + 1, timeout_seconds=5.0
            )
            await asyncio.sleep(0.2)  # a wrongly-repeating resync would fetch more than once here
            assert fake.request_count(config_path) == before + 1
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_resync_publishes_only_deltas(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL, state="0")}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer:
                await observer.subscribe(f"{BASE}/switch")
                messages = observer.messages
                async with asyncio.timeout(5.0):
                    await anext(aiter(messages))  # the retained replay from subscribing

                # nothing changed -- reconnecting must publish zero new entity messages
                await fake.drop_websocket()
                await _wait_until(
                    lambda: supervisor._ws is not None and supervisor._ws.reconnect_count >= 2,
                    timeout_seconds=5.0,
                )
                with pytest.raises(TimeoutError):
                    async with asyncio.timeout(0.3):
                        await anext(aiter(messages))

                # something DID change -- exactly one delta, carrying the new value
                fake.set_datapoint(SERIAL, "ch0000", "odp0000", "1")
                await fake.drop_websocket()
                await _wait_until(
                    lambda: supervisor._ws is not None and supervisor._ws.reconnect_count >= 3,
                    timeout_seconds=5.0,
                )
                async with asyncio.timeout(5.0):
                    msg = await anext(aiter(messages))
                assert orjson.loads(msg.payload)["state"] is True
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_resync_buffers_a_frame_arriving_during_its_own_config_fetch(tmp_path: Path) -> None:
    # The same P-22 protection cold start gets (test_no_events_lost_during_startup_window) must
    # also hold for a later resync: `_resync` re-arms buffering before its own config fetch.
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL, state="0")}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)

            fake.set_latency(150)  # ms -- opens a window during the resync's own config fetch
            await fake.drop_websocket()
            await _wait_until(
                lambda: supervisor._ws is not None and supervisor._ws.reconnect_count >= 2,
                timeout_seconds=5.0,
            )
            await fake.push_ws_frame({"datapoints": {f"{SERIAL}/ch0000/odp0000": "1"}})

            await _wait_until(
                lambda: (
                    supervisor._state is not None
                    and supervisor._model is not None
                    and supervisor._state.values[supervisor._model.by_id[f"{SERIAL}_ch0000"]][0]
                    is True
                ),
                timeout_seconds=5.0,
            )
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_devices_added_triggers_reload(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            assert supervisor._model is not None
            assert f"{SERIAL}_ch0000" in supervisor._model.by_id

            new_serial = "ABB7F500E999"
            fake.set_configuration(
                _configuration(
                    {SERIAL: _switch_device(SERIAL), new_serial: _switch_device(new_serial)}
                )
            )
            await fake.push_ws_frame({"devicesAdded": [new_serial]})

            await _wait_until(
                lambda: (
                    supervisor._model is not None
                    and f"{new_serial}_ch0000" in supervisor._model.by_id
                ),
                timeout_seconds=5.0,
            )
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_devices_removed_retracts_discovery(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer:
                await observer.subscribe(f"{BASE}/switch")
                messages = observer.messages
                async with asyncio.timeout(5.0):
                    await anext(aiter(messages))  # the retained replay from subscribing

                fake.set_configuration(_configuration({}))
                await fake.push_ws_frame({"devicesRemoved": [SERIAL]})

                async with asyncio.timeout(5.0):
                    msg = await anext(aiter(messages))
                assert msg.payload == b""
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_broker_outage_state_correct_on_reconnect(tmp_path: Path) -> None:
    # docs/08 §9, F6: ingestion must never pause, and the accumulated change must publish once
    # the broker comes back -- exercising the bus/publisher.py fix this WP made along the way.
    port = free_port()
    async with running_fake_sysap(FakeSysAp()) as (fake, http_client):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL, state="0")}))

        async with running_fake_broker(port=port):
            supervisor = Supervisor(
                config=_config(tmp_path, port, http_client),
                profiles=REGISTRY,
                http_session=http_client.session,
            )
            task = asyncio.create_task(supervisor.run())
            await _wait_until(lambda: supervisor._cold_start_done)
        # the broker is now down (its `async with` block has exited)

        await fake.push_ws_frame({"datapoints": {f"{SERIAL}/ch0000/odp0000": "1"}})
        await asyncio.sleep(0.1)  # ingestion keeps applying it even with nowhere to publish

        async with running_fake_broker(port=port):
            await _wait_until(
                lambda: supervisor._mqtt is not None and supervisor._mqtt.reconnect_count >= 2,
                timeout_seconds=10.0,
            )
            async with aiomqtt.Client("127.0.0.1", port=port) as observer:
                await observer.subscribe(f"{BASE}/switch")
                async with asyncio.timeout(5.0):
                    msg = await anext(aiter(observer.messages))
                assert orjson.loads(msg.payload)["state"] is True

        await supervisor.stop()
        await asyncio.wait_for(task, timeout=5.0)


# -------------------------------------------------------------------------------------- shutdown


async def test_shutdown_flushes_pending_commands(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _dimmer_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client, command_debounce_s=5.0),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as outsider:
                await outsider.publish(f"{BASE}/dimmer/set", orjson.dumps({"brightness": 10}))
                await asyncio.sleep(0.05)
                await outsider.publish(f"{BASE}/dimmer/set", orjson.dumps({"brightness": 55}))
                await asyncio.sleep(0.05)  # let the dispatcher collapse it into `pending`

            path = f"/fhapi/v1/api/rest/datapoint/{_UUID}/{SERIAL}.ch0000.idp0001"
            assert fake.request_count(path) == 1  # only the leading edge so far

            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)

            assert fake.request_count(path) == 2
            assert _input_value(fake, "ch0000", "idp0001") == "55"
        finally:
            if not task.done():
                await supervisor.stop()
                await asyncio.wait_for(task, timeout=5.0)


async def test_shutdown_publishes_bridge_state_offline(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        await _wait_until(lambda: supervisor._cold_start_done)

        async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer:
            await observer.subscribe(f"{BASE}/bridge/state")
            async with asyncio.timeout(5.0):
                msg = await anext(aiter(observer.messages))
            assert orjson.loads(msg.payload) == {"state": "online"}

            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)

            # the broker may already have gone through the disconnect, so re-subscribe fresh
        async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer2:
            await observer2.subscribe(f"{BASE}/bridge/state")
            async with asyncio.timeout(5.0):
                msg2 = await anext(aiter(observer2.messages))
            assert orjson.loads(msg2.payload) == {"state": "offline"}


async def test_entities_json_is_persisted_on_shutdown(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        await _wait_until(lambda: supervisor._cold_start_done)
        await supervisor.stop()
        await asyncio.wait_for(task, timeout=5.0)

        assert (tmp_path / "entities.json").exists()


# ------------------------------------------------------------------------- task restart/escalation


async def test_task_restart_and_escalation() -> None:
    metrics = Metrics()
    attempts = 0

    async def flaky() -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("boom")

    with pytest.raises(TaskDiedTooManyTimesError):
        await restart_on_failure(
            "flaky",
            flaky,
            metrics=metrics,
            clock=lambda: 0.0,
            sleep=lambda _delay: asyncio.sleep(0),
        )

    assert attempts == _ESCALATION_THRESHOLD
    assert metrics.task_restarts == _ESCALATION_THRESHOLD


async def test_restart_on_failure_does_not_restart_a_clean_return() -> None:
    metrics = Metrics()
    calls = 0

    async def finishes() -> None:
        nonlocal calls
        calls += 1

    await restart_on_failure("finishes", finishes, metrics=metrics, clock=lambda: 0.0)
    assert calls == 1
    assert metrics.task_restarts == 0


async def test_restart_on_failure_never_restarts_on_ws_authentication_error() -> None:
    # docs/06 §3: "Auth failure -> Immediately. Do not retry." -- restart_on_failure's generic
    # backoff-and-restart policy must not apply to this, the same way it already excludes
    # CancelledError.
    metrics = Metrics()
    attempts = 0

    async def bad_credentials() -> None:
        nonlocal attempts
        attempts += 1
        raise WsAuthenticationError("bad credentials")

    with pytest.raises(WsAuthenticationError):
        await restart_on_failure(
            "ws_reader",
            bad_credentials,
            metrics=metrics,
            clock=lambda: 0.0,
            sleep=lambda _delay: asyncio.sleep(0),
        )
    assert attempts == 1
    assert metrics.task_restarts == 0


async def test_restart_on_failure_never_restarts_on_cancellation() -> None:
    metrics = Metrics()

    async def cancels() -> None:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await restart_on_failure("cancels", cancels, metrics=metrics, clock=lambda: 0.0)
    assert metrics.task_restarts == 0


async def test_a_slow_failure_resets_the_rapid_failure_streak() -> None:
    # Four rapid failures bring the streak to 4 (one short of escalating); a slow one (a large
    # gap between start and end) resets it; four more rapid failures are needed to reach 5. If
    # the reset had not happened, escalation would already have fired on the 5th call overall.
    clock_values = iter(
        [
            0.0,
            0.0,  # call 1 (fast)
            1.0,
            1.0,  # call 2 (fast)
            2.0,
            2.0,  # call 3 (fast)
            3.0,
            3.0,  # call 4 (fast) -- streak is 4
            4.0,
            100.0,  # call 5 (SLOW) -- resets the streak
            101.0,
            101.0,  # call 6 (fast) -- streak 2
            102.0,
            102.0,  # call 7 (fast) -- streak 3
            103.0,
            103.0,  # call 8 (fast) -- streak 4
            104.0,
            104.0,  # call 9 (fast) -- streak 5 -- escalates
        ]
    )
    metrics = Metrics()
    calls = 0

    async def always_fails() -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("boom")

    with pytest.raises(TaskDiedTooManyTimesError):
        await restart_on_failure(
            "slow_then_rapid",
            always_fails,
            metrics=metrics,
            clock=lambda: next(clock_values),
            sleep=lambda _delay: asyncio.sleep(0),
        )
    assert calls == 9


# --------------------------------------------------------------------------------- P-28 monotonic


def test_restart_on_failure_defaults_to_the_monotonic_clock() -> None:
    default = inspect.signature(restart_on_failure).parameters["clock"].default
    assert default is time.monotonic


def test_reload_debouncer_defaults_to_the_monotonic_clock() -> None:
    default = inspect.signature(_ReloadDebouncer.__init__).parameters["clock"].default
    assert default is time.monotonic


# ------------------------------------------------------------------------------ _ReloadDebouncer


async def test_reload_debouncer_coalesces_a_repeat_request_into_one_more_resync() -> None:
    # P-55: a request arriving while a debounce timer is already pending must not queue a second
    # timer -- it collapses into a single extra resync once the first one finishes.
    calls = 0

    async def resync() -> None:
        nonlocal calls
        calls += 1

    debouncer = _ReloadDebouncer(resync=resync, debounce_s=0.02, min_interval_s=0.0)
    debouncer.request()
    await asyncio.sleep(0.005)
    debouncer.request()  # arrives while the first timer is still pending
    await asyncio.sleep(0.005)
    debouncer.request()  # and again -- still collapses into the same "reload again"

    await asyncio.sleep(0.2)
    assert calls == 2
    assert debouncer.reload_count == 2


async def test_reload_debouncer_enforces_the_minimum_interval_between_resyncs() -> None:
    call_times: list[float] = []

    async def resync() -> None:
        call_times.append(time.monotonic())

    debouncer = _ReloadDebouncer(resync=resync, debounce_s=0.01, min_interval_s=0.15)
    debouncer.request()
    await asyncio.sleep(0.05)
    debouncer.request()  # a second, independent request after the first has already completed

    await asyncio.sleep(0.3)
    assert len(call_times) == 2
    assert call_times[1] - call_times[0] >= 0.1  # comfortably below the 0.15 floor would fail this


# --------------------------------------------------------------------- preconditions / guards


def _bare_supervisor(tmp_path: Path) -> Supervisor:
    """A `Supervisor` that has never run `_startup()` -- every dependent is still `None`, which
    is exactly the state its small guard methods (`_ingress_or_raise`, `_on_ws_frame`, ...) exist
    to handle safely rather than crash a callback that fires before startup finishes.
    """
    config = SupervisorConfig(
        sysap_base_url="http://127.0.0.1:1",
        sysap_username="installer",
        sysap_password="secret",
        data_dir=tmp_path,
    )
    return Supervisor(config=config, profiles=REGISTRY, http_session=aiohttp.ClientSession())


async def test_spawn_supervised_raises_outside_of_run(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        with pytest.raises(RuntimeError, match="outside of run"):
            supervisor._spawn_supervised("x", lambda: asyncio.sleep(0))
    finally:
        await supervisor._http_session.close()


async def test_ingress_and_publisher_or_raise_before_startup(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        with pytest.raises(RuntimeError, match="ingress"):
            supervisor._ingress_or_raise()
        with pytest.raises(RuntimeError, match="publisher"):
            supervisor._publisher_or_raise()
    finally:
        await supervisor._http_session.close()


async def test_live_callbacks_are_safe_before_dependents_exist(tmp_path: Path) -> None:
    # docs/02 §7: MQTT subscribes (and can therefore start receiving) before `_ingress`/`_commands`
    # exist -- every live callback must no-op rather than raise if it fires in that narrow window.
    supervisor = _bare_supervisor(tmp_path)
    try:
        supervisor._on_ws_frame({"datapoints": {}})
        supervisor._on_ws_frame({"devicesAdded": ["x"]})  # cold_start_done is False -- no reload
        supervisor._on_mqtt_disconnected()
        supervisor._on_ws_connected()  # self._ws is None -- start_buffering() is skipped
        await supervisor._on_mqtt_reconnected()

        class _FakeMessage:
            topic = "freeathome2mqtt/bridge/request/reload"
            payload = b"{}"

        supervisor._on_mqtt_message(_FakeMessage())  # type: ignore[arg-type]
    finally:
        await supervisor._http_session.close()


async def test_resync_raises_before_startup_completed(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        with pytest.raises(RuntimeError, match="startup completed"):
            await supervisor._resync()
    finally:
        await supervisor._http_session.close()


async def test_fetch_configuration_with_retry_raises_before_rest_exists(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        with pytest.raises(RuntimeError, match="RestClient"):
            await supervisor._fetch_configuration_with_retry()
    finally:
        await supervisor._http_session.close()


async def test_publish_discovery_raises_before_mqtt_exists(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        with pytest.raises(RuntimeError, match="MqttClient"):
            await supervisor._publish_discovery(supervisor._model)  # type: ignore[arg-type]
    finally:
        await supervisor._http_session.close()


async def test_graceful_shutdown_before_startup_only_persists_and_closes(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    await supervisor._graceful_shutdown()
    assert (tmp_path / "entities.json").exists()
    assert supervisor._http_session.closed


# --------------------------------------------------------------------- _spawn_background logging


async def test_spawn_background_logs_a_failed_background_task(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:

        async def _fails() -> None:
            raise RuntimeError("background boom")

        with caplog.at_level("ERROR", logger="freeathome2mqtt.supervisor"):
            supervisor._spawn_background(_fails(), name="doomed")
            await asyncio.sleep(0.05)

        assert any("doomed" in record.message for record in caplog.records)
    finally:
        await supervisor._http_session.close()


async def test_spawn_background_discards_a_cancelled_task_silently(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:

        async def _hangs() -> None:
            await asyncio.sleep(10.0)

        with caplog.at_level("ERROR", logger="freeathome2mqtt.supervisor"):
            supervisor._spawn_background(_hangs(), name="cancel_me")
            await asyncio.sleep(0.01)
            (task,) = supervisor._background_tasks
            task.cancel()
            await asyncio.sleep(0.05)

        assert not any("cancel_me" in record.message for record in caplog.records)
        assert supervisor._background_tasks == set()
    finally:
        await supervisor._http_session.close()


# -------------------------------------------------------------- _fetch_configuration_with_retry


class _FlakyRest:
    """A minimal `RestClient`-shaped double: fails `fail_times` times, then succeeds."""

    def __init__(self, *, fail_times: int, error: Exception) -> None:
        self._remaining = fail_times
        self._error = error
        self.attempts = 0

    async def get_configuration(self) -> dict[str, Any]:
        self.attempts += 1
        if self._remaining > 0:
            self._remaining -= 1
            raise self._error
        return {"devices": {}}


async def test_fetch_configuration_with_retry_retries_and_then_succeeds(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        flaky = _FlakyRest(fail_times=2, error=ServerOverloadedError(502, "/x"))
        supervisor._rest = flaky  # type: ignore[assignment]
        config = await supervisor._fetch_configuration_with_retry()
        assert config == {"devices": {}}
        assert flaky.attempts == 3
    finally:
        await supervisor._http_session.close()


async def test_fetch_configuration_with_retry_never_retries_auth_failures(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        flaky = _FlakyRest(fail_times=99, error=AuthenticationError(401, "/x"))
        supervisor._rest = flaky  # type: ignore[assignment]
        with pytest.raises(AuthenticationError):
            await supervisor._fetch_configuration_with_retry()
        assert flaky.attempts == 1
    finally:
        await supervisor._http_session.close()


# ------------------------------------------------------------- _publish_discovery / _retract_entity


def _bare_entity(
    *, availability_topic: str | None, discovery: tuple[tuple[str, bytes], ...] = ()
) -> Entity:
    return Entity(
        idx=0,
        id=f"{SERIAL}_ch0000",
        profile="switch_actuator",
        name="Switch",
        area=None,
        device_serial=SERIAL,
        channel_id="ch0000",
        attr_names=("state",),
        attr_kinds=(AttrKind.STATE,),
        state_topic=f"{BASE}/switch",
        set_topic=f"{BASE}/switch/set",
        get_topic=f"{BASE}/switch/get",
        availability_topic=availability_topic,
        optimistic=False,
        discovery=discovery,
    )


def _empty_stats() -> CompileStats:
    return CompileStats(
        devices_total=0,
        devices_excluded_by_interface=0,
        channels_total=0,
        channels_orphaned=0,
        channels_unknown_function=0,
        channels_unsupported=0,
        channels_ambiguous_profile=0,
        entities_created=0,
    )


def _bare_model(
    entities: tuple[Entity, ...], *, discovery: tuple[tuple[str, bytes], ...] = ()
) -> Model:
    return Model(
        entities=entities,
        ingress={},
        egress={},
        by_id={e.id: e.idx for e in entities},
        by_topic={},
        discovery=discovery,
        initial_values=tuple((None,) * len(e.attr_names) for e in entities),
        stats=_empty_stats(),
    )


async def test_publish_discovery_publishes_every_configured_topic(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            model = _bare_model((), discovery=(("homeassistant/switch/x/config", b'{"a":1}'),))

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer:
                await observer.subscribe("homeassistant/switch/x/config")
                async with asyncio.timeout(5.0):
                    async_iter = aiter(observer.messages)
                    await supervisor._publish_discovery(model)
                    msg = await anext(async_iter)
                assert msg.payload == b'{"a":1}'
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_retract_entity_skips_absent_availability_and_retracts_discovery(
    tmp_path: Path,
) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            entity = _bare_entity(
                availability_topic=None,
                discovery=(("homeassistant/switch/x/config", b'{"a":1}'),),
            )
            assert supervisor._mqtt is not None

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer:
                await observer.subscribe("homeassistant/switch/x/config")
                await observer.subscribe(f"{BASE}/switch")
                async with asyncio.timeout(5.0):
                    async_iter = aiter(observer.messages)
                    await supervisor._retract_entity(supervisor._mqtt, entity)
                    first = await anext(async_iter)
                    second = await anext(async_iter)
                topics_seen = {str(first.topic), str(second.topic)}
                assert topics_seen == {f"{BASE}/switch", "homeassistant/switch/x/config"}
                assert first.payload == b""
                assert second.payload == b""
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


# ---------------------------------------------------------------------------- bridge/devices


def _bridge_devices_config() -> dict[str, Any]:
    return _configuration(
        {
            SERIAL: {
                "displayName": "Living Room Switch",
                "deviceId": "910C",
                "articleNumber": "6224/xx-500",
                "interface": "TP",
                "unresponsive": False,
                "defect": False,
                # Deliberately no device-level floor/room: ch0000/ch0001 declare their own, and
                # ch0002 must NOT inherit one, or it would not be the orphaned channel this
                # fixture needs it to be.
                "channels": {
                    "ch0000": {
                        "displayName": "Switch",
                        "functionID": "7",  # FID_SWITCH_ACTUATOR -- supported
                        "inputs": {"idp0000": {"pairingID": 1, "value": "0"}},
                        "outputs": {"odp0000": {"pairingID": 256, "value": "1"}},
                        "floor": "01",
                        "room": "01",
                    },
                    "ch0001": {
                        "displayName": "Mystery",
                        "functionID": "fff",  # not a known Function
                        "outputs": {},
                        "floor": "01",
                        "room": "01",
                    },
                    "ch0002": {
                        "displayName": "Orphan",
                        "functionID": "7",
                        "outputs": {"odp0000": {"pairingID": 256, "value": "1"}},
                        # no floor/room -- orphaned
                    },
                },
            }
        }
    )


def _compiled_bridge_devices_model() -> Model:
    return compile_model(_bridge_devices_config(), REGISTRY, CompileOptions(topic_prefix=BASE))


def test_build_bridge_devices_marks_the_matching_channel_supported() -> None:
    model = _compiled_bridge_devices_model()
    devices = _build_bridge_devices(_bridge_devices_config(), model, REGISTRY)
    assert len(devices) == 1
    device = devices[0]
    assert device["serial"] == SERIAL
    assert device["name"] == "Living Room Switch"
    assert device["device_id"] == "910C"
    assert device["area"] is None  # no device-level floor/room in this fixture

    channels = {c["channel_id"]: c for c in device["channels"]}
    assert channels["ch0000"]["supported"] is True
    assert channels["ch0000"]["entity_id"] == f"{SERIAL}_ch0000"
    assert channels["ch0000"]["profile"] == "switch_actuator"
    assert channels["ch0000"]["function"] == "FID_SWITCH_ACTUATOR"
    assert channels["ch0000"]["attributes"] == [
        {"name": "state", "codec": "bool01", "kind": "state"}
    ]
    assert channels["ch0000"]["commands"] == [{"name": "state", "continuous": False}]


def test_build_bridge_devices_resolves_device_level_area() -> None:
    config = _configuration({SERIAL: _switch_device(SERIAL)})
    model = compile_model(config, REGISTRY, CompileOptions(topic_prefix=BASE))
    devices = _build_bridge_devices(config, model, REGISTRY)
    assert devices[0]["area"] == "Living Room"


def test_build_bridge_devices_reports_unknown_function_reason() -> None:
    model = _compiled_bridge_devices_model()
    devices = _build_bridge_devices(_bridge_devices_config(), model, REGISTRY)
    channels = {c["channel_id"]: c for c in devices[0]["channels"]}
    assert channels["ch0001"]["supported"] is False
    assert channels["ch0001"]["function"] is None
    assert channels["ch0001"]["reason"] == "unknown function ID"


def test_build_bridge_devices_reports_orphaned_channel_reason() -> None:
    model = _compiled_bridge_devices_model()
    devices = _build_bridge_devices(_bridge_devices_config(), model, REGISTRY)
    channels = {c["channel_id"]: c for c in devices[0]["channels"]}
    assert channels["ch0002"]["supported"] is False
    assert "orphaned" in channels["ch0002"]["reason"]


def test_unsupported_channels_are_reported() -> None:
    # docs/09 P-59's named test: a channel whose functionID is real and recognised, but that no
    # shipped profile claims, must still surface in bridge/devices with its raw function ID --
    # so a user can open a well-formed issue instead of the device being silently invisible
    # (docs/03 §3.5).
    config = _configuration(
        {
            SERIAL: {
                "displayName": "Domus Window",
                "interface": "TP",
                "floor": "01",
                "room": "01",
                "channels": {
                    "ch0000": {
                        "displayName": "Contact",
                        "functionID": "9d",  # FID_DOMUS_WINDOW_CONTACT -- real, no shipped profile
                        "outputs": {"odp0000": {"pairingID": 53, "value": "1"}},
                        "floor": "01",
                        "room": "01",
                    }
                },
            }
        }
    )
    model = compile_model(config, REGISTRY, CompileOptions(topic_prefix=BASE))
    devices = _build_bridge_devices(config, model, REGISTRY)
    channel = devices[0]["channels"][0]
    assert channel["supported"] is False
    assert channel["function_id"] == "0x9D"
    assert channel["function"] == "FID_DOMUS_WINDOW_CONTACT"
    assert channel["reason"] == "no profile claims this function"


def test_split_devices_payload_fits_in_one_part_below_the_limit() -> None:
    devices = [{"serial": f"S{i}"} for i in range(5)]
    parts = _split_devices_payload(devices, max_size=1_000_000)
    assert len(parts) == 1


def test_split_devices_payload_splits_when_it_would_exceed_the_limit() -> None:
    devices = [{"serial": f"S{i}", "padding": "x" * 100} for i in range(50)]
    small_limit = 500
    parts = _split_devices_payload(devices, max_size=small_limit)
    assert len(parts) > 1
    for part in parts:
        # each part fits, except possibly a single-device part that alone exceeds the limit --
        # not exercised here since no single device is that large.
        assert len(part) <= small_limit


def test_split_devices_payload_never_drops_a_device() -> None:
    devices = [{"serial": f"S{i}"} for i in range(20)]
    parts = _split_devices_payload(devices, max_size=80)
    total = sum(len(orjson.loads(p)) for p in parts)
    assert total == len(devices)


async def test_publish_bridge_devices_publishes_a_single_topic_when_it_fits(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer:
                await observer.subscribe(f"{BASE}/bridge/devices")
                async with asyncio.timeout(5.0):
                    msg = await anext(aiter(observer.messages))
                body = orjson.loads(msg.payload)
                assert isinstance(body, list)
                assert body[0]["serial"] == SERIAL
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_publish_bridge_devices_splits_when_over_the_configured_packet_size(
    tmp_path: Path,
) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        many_devices = {
            f"ABB7F5{i:06X}": _switch_device(f"ABB7F5{i:06X}", name=f"Switch {i}")
            for i in range(20)
        }
        fake.set_configuration(_configuration(many_devices))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client, mqtt_maximum_packet_size=400),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer:
                await observer.subscribe(f"{BASE}/bridge/devices")
                await observer.subscribe(f"{BASE}/bridge/devices/+")
                seen: dict[str, bytes] = {}
                async with asyncio.timeout(5.0):
                    async for message in observer.messages:
                        seen[str(message.topic)] = message.payload
                        if f"{BASE}/bridge/devices" in seen and any("devices/" in t for t in seen):
                            break
                index = orjson.loads(seen[f"{BASE}/bridge/devices"])
                assert "parts" in index
                assert len(index["parts"]) >= 2
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


# ---------------------------------------------------------------------------- _diff_and_apply


async def test_diff_and_apply_seeds_fresh_state_with_no_old_model(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            # Reset to a pristine, never-compiled state to exercise the "no old model" branch
            # directly, rather than only through the (currently unreachable) full startup path.
            supervisor._model = None
            supervisor._state = None
            assert supervisor._mqtt is not None
            assert supervisor._rest is not None

            entity = _bare_entity(availability_topic=None)
            model = _bare_model((entity,))
            await supervisor._diff_and_apply(
                model, mqtt=supervisor._mqtt, rest=supervisor._rest, config={}
            )

            assert supervisor._model is model
            assert supervisor._state is not None
            assert supervisor._state.dirty == set()  # nothing to diff against -- nothing marked
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


# ------------------------------------------------------------------------ _config_refresh_loop


async def test_config_refresh_loop_resyncs_only_when_the_hash_changed(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL, state="0")}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client, config_refresh_interval_s=0.05),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            config_path = "/fhapi/v1/api/rest/configuration"
            reloads_before = supervisor.metrics.config_reloads

            # Tick with nothing changed -- fetched (hash checked) but must not resync.
            await asyncio.sleep(0.15)
            assert supervisor.metrics.config_reloads == reloads_before
            requests_after_idle_ticks = fake.request_count(config_path)
            assert requests_after_idle_ticks >= 2  # the periodic timer really did fire

            fake.set_datapoint(SERIAL, "ch0000", "odp0000", "1")
            await _wait_until(
                lambda: supervisor.metrics.config_reloads > reloads_before, timeout_seconds=5.0
            )
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


# ---------------------------------------------------------------------------------- bridge API


async def test_reload_debounce_and_rate_limit(tmp_path: Path) -> None:
    """docs/04 §5 `reload` reuses `_ReloadDebouncer` (P-55): a burst of requests over the broker
    coalesces into at most one *more* resync than the debouncer's own coalescing already allows
    (`test_reload_debouncer_coalesces_a_repeat_request_into_one_more_resync`) -- never one resync
    per request, which is what "no debouncing at all" would look like.
    """
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            reloads_before = supervisor.metrics.config_reloads
            requests_sent = 5

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as sender:
                for _ in range(requests_sent):
                    await sender.publish(f"{BASE}/bridge/request/reload", b"{}")

            await _wait_until(
                lambda: supervisor.metrics.config_reloads > reloads_before, timeout_seconds=5.0
            )
            await asyncio.sleep(0.3)  # a wrongly-repeating resync would keep firing past here
            reloads = supervisor.metrics.config_reloads - reloads_before
            assert 1 <= reloads <= 2  # one debounce window, plus at most one coalesced repeat
            assert reloads < requests_sent
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_reload_command_requests_a_debounced_resync(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        calls = 0

        def _record() -> None:
            nonlocal calls
            calls += 1

        supervisor._reload_debouncer.request = _record  # type: ignore[method-assign]
        data = await supervisor._handle_reload({})
        assert data == {}
        assert calls == 1
    finally:
        await supervisor._http_session.close()


async def test_restart_command_sets_restart_requested_and_triggers_shutdown(
    tmp_path: Path,
) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        assert supervisor.restart_requested is False
        data = await supervisor._handle_restart({})
        assert data == {}
        assert supervisor.restart_requested is True
        assert supervisor._shutdown_event.is_set()
    finally:
        await supervisor._http_session.close()


async def test_restart_via_mqtt_makes_run_return_with_restart_requested(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            async with aiomqtt.Client("127.0.0.1", port=broker.port) as sender:
                await sender.publish(f"{BASE}/bridge/request/restart", b"{}")
            await asyncio.wait_for(task, timeout=5.0)
            assert supervisor.restart_requested is True
        finally:
            if not task.done():
                await supervisor.stop()
                await asyncio.wait_for(task, timeout=5.0)


# ------------------------------------------------------------------------------- entity/rename


async def test_rename_clears_old_retained_topics(tmp_path: Path) -> None:
    """ADR-010's four-step transaction: clear the old retained topics, persist the alias,
    republish under the new topic, emit `bridge/event`.
    """
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            entity_id = f"{SERIAL}_ch0000"
            old_topic = f"{BASE}/switch"
            new_topic = f"{BASE}/kitchen_light"
            event_topic = f"{BASE}/bridge/event"

            seen: dict[str, bytes] = {}
            async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer:
                await observer.subscribe(f"{BASE}/#")

                async def _collect() -> None:
                    async for message in observer.messages:
                        seen[str(message.topic)] = message.payload

                collector = asyncio.create_task(_collect())
                try:
                    # The initial retained flood for the pre-rename topic must land before the
                    # rename request is sent, or its empty retract could be mistaken for it.
                    await _wait_until(lambda: old_topic in seen)

                    async with aiomqtt.Client("127.0.0.1", port=broker.port) as sender:
                        await sender.publish(
                            f"{BASE}/bridge/request/entity/rename",
                            orjson.dumps({"id": entity_id, "name": "Kitchen Light"}),
                        )

                    # `bridge/event` is published last in the handler's sequence, so by the time
                    # it arrives the retract and the new-topic publish (same connection, ordered)
                    # are guaranteed to have already landed.
                    await _wait_until(lambda: event_topic in seen, timeout_seconds=5.0)
                finally:
                    collector.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await collector

            assert seen[old_topic] == b""
            assert new_topic in seen and seen[new_topic] != b""
            event = orjson.loads(seen[event_topic])
            assert event == {
                "type": "entity_renamed",
                "id": entity_id,
                "from": old_topic,
                "to": new_topic,
            }
            assert supervisor._entities_store.alias_for(entity_id) == "Kitchen Light"
            assert supervisor._model is not None
            assert supervisor._model.entities[supervisor._model.by_id[entity_id]].state_topic == (
                new_topic
            )
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_rename_response_is_an_error_for_an_unknown_entity(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer:
                await observer.subscribe(f"{BASE}/bridge/response/#")
                async with aiomqtt.Client("127.0.0.1", port=broker.port) as sender:
                    await sender.publish(
                        f"{BASE}/bridge/request/entity/rename",
                        orjson.dumps({"id": "no-such-entity", "name": "x"}),
                    )
                async with asyncio.timeout(5.0):
                    async for message in observer.messages:
                        response = orjson.loads(message.payload)
                        break
            assert response["status"] == "error"
            assert "no-such-entity" in response["error"]
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_rename_requires_id_and_name(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        with pytest.raises(BridgeApiError, match="requires a non-empty 'id'"):
            await supervisor._handle_entity_rename({"name": "x"})
        with pytest.raises(BridgeApiError, match="requires a non-empty 'name'"):
            await supervisor._handle_entity_rename({"id": "x"})
    finally:
        await supervisor._http_session.close()


async def test_rename_before_startup_raises(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        with pytest.raises(BridgeApiError, match="not ready"):
            await supervisor._handle_entity_rename({"id": "x", "name": "y"})
    finally:
        await supervisor._http_session.close()


# ------------------------------------------------------------------ entity/options, entity/remove


def _stub_reload_requests(supervisor: Supervisor) -> list[None]:
    calls: list[None] = []
    supervisor._reload_debouncer.request = lambda: calls.append(None)  # type: ignore[method-assign]
    return calls


async def test_entity_options_persists_and_resyncs_on_enabled_change(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        calls = _stub_reload_requests(supervisor)
        data = await supervisor._handle_entity_options(
            {"id": "ABB_ch0001", "options": {"enabled": False}}
        )
        assert data == {"id": "ABB_ch0001", "options": {"enabled": False}}
        assert supervisor._entities_store.options_for("ABB_ch0001") == {"enabled": False}
        assert calls == [None]
    finally:
        await supervisor._http_session.close()


async def test_entity_options_resyncs_for_optimistic_debounce_and_homeassistant_keys(
    tmp_path: Path,
) -> None:
    # optimistic/debounce_ms act on CommandDispatcher, homeassistant on discovery -- both only
    # take effect on the next _rebuild_dependents/build_model_discovery, i.e. a resync, the same
    # as enabled already needed one.
    for key, value in (("optimistic", False), ("debounce_ms", 100), ("homeassistant", {})):
        supervisor = _bare_supervisor(tmp_path)
        try:
            calls = _stub_reload_requests(supervisor)
            await supervisor._handle_entity_options({"id": "ABB_ch0001", "options": {key: value}})
            assert calls == [None], f"{key!r} should have requested a resync"
        finally:
            await supervisor._http_session.close()


async def test_entity_options_does_not_resync_for_an_unrelated_key(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        calls = _stub_reload_requests(supervisor)
        await supervisor._handle_entity_options(
            {"id": "ABB_ch0001", "options": {"some_future_key": 1}}
        )
        assert supervisor._entities_store.options_for("ABB_ch0001") == {"some_future_key": 1}
        assert calls == []
    finally:
        await supervisor._http_session.close()


async def test_entity_options_merges_across_calls(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        _stub_reload_requests(supervisor)
        await supervisor._handle_entity_options(
            {"id": "ABB_ch0001", "options": {"optimistic": False}}
        )
        await supervisor._handle_entity_options(
            {"id": "ABB_ch0001", "options": {"debounce_ms": 50}}
        )
        assert supervisor._entities_store.options_for("ABB_ch0001") == {
            "optimistic": False,
            "debounce_ms": 50,
        }
    finally:
        await supervisor._http_session.close()


async def test_entity_options_requires_id_and_an_options_object(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        with pytest.raises(BridgeApiError, match="requires a non-empty 'id'"):
            await supervisor._handle_entity_options({"options": {}})
        with pytest.raises(BridgeApiError, match="requires an 'options' object"):
            await supervisor._handle_entity_options({"id": "x", "options": "not-a-dict"})
    finally:
        await supervisor._http_session.close()


async def test_entity_remove_marks_disabled_and_requests_resync(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        calls = _stub_reload_requests(supervisor)
        data = await supervisor._handle_entity_remove({"id": "ABB_ch0001"})
        assert data == {"id": "ABB_ch0001"}
        assert supervisor._entities_store.options_for("ABB_ch0001") == {"enabled": False}
        assert calls == [None]
    finally:
        await supervisor._http_session.close()


async def test_entity_remove_requires_id(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        with pytest.raises(BridgeApiError, match="requires a non-empty 'id'"):
            await supervisor._handle_entity_remove({})
    finally:
        await supervisor._http_session.close()


async def test_entity_remove_retracts_the_entity_end_to_end(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            entity_id = f"{SERIAL}_ch0000"
            topic = f"{BASE}/switch"
            reloads_before = supervisor.metrics.config_reloads

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer:
                await observer.subscribe(topic)

                async def _wait_for_empty_payload() -> None:
                    async for message in observer.messages:
                        if message.payload == b"":
                            return

                waiter = asyncio.create_task(_wait_for_empty_payload())
                async with aiomqtt.Client("127.0.0.1", port=broker.port) as sender:
                    await sender.publish(
                        f"{BASE}/bridge/request/entity/remove",
                        orjson.dumps({"id": entity_id}),
                    )
                await asyncio.wait_for(waiter, timeout=5.0)

            await _wait_until(
                lambda: supervisor.metrics.config_reloads > reloads_before, timeout_seconds=5.0
            )
            assert supervisor._model is not None
            assert entity_id not in supervisor._model.by_id
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_entity_options_optimistic_override_takes_effect_end_to_end(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            entity_id = f"{SERIAL}_ch0000"
            reloads_before = supervisor.metrics.config_reloads

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as sender:
                await sender.publish(
                    f"{BASE}/bridge/request/entity/options",
                    orjson.dumps({"id": entity_id, "options": {"optimistic": False}}),
                )
            await _wait_until(
                lambda: supervisor.metrics.config_reloads > reloads_before, timeout_seconds=5.0
            )

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as sender:
                await sender.publish(f"{BASE}/switch/set", orjson.dumps({"state": True}))
            await _wait_until(
                lambda: (
                    fake.request_count(
                        f"/fhapi/v1/api/rest/datapoint/{_UUID}/{SERIAL}.ch0000.idp0000"
                    )
                    >= 1
                )
            )

            assert supervisor._model is not None
            idx = supervisor._model.by_id[entity_id]
            assert supervisor._state is not None
            # The REST write reached the SysAP (checked above), but the optimistic guess (True)
            # must never have been applied -- the seeded initial value (False) is untouched.
            assert supervisor._state.values[idx][0] is False
            assert supervisor._state.unconfirmed[idx] == 0
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_entity_options_homeassistant_override_takes_effect_end_to_end(
    tmp_path: Path,
) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            entity_id = f"{SERIAL}_ch0000"
            reloads_before = supervisor.metrics.config_reloads

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as sender:
                await sender.publish(
                    f"{BASE}/bridge/request/entity/options",
                    orjson.dumps(
                        {"id": entity_id, "options": {"homeassistant": {"device_class": "outlet"}}}
                    ),
                )
            await _wait_until(
                lambda: supervisor.metrics.config_reloads > reloads_before, timeout_seconds=5.0
            )

            assert supervisor._model is not None
            idx = supervisor._model.by_id[entity_id]
            entity = supervisor._model.entities[idx]
            assert len(entity.discovery) == 1
            body = orjson.loads(entity.discovery[0][1])
            assert body["device_class"] == "outlet"
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


# ----------------------------------------------------------- device/refresh, discovery/republish


async def test_device_refresh_requires_serial(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        with pytest.raises(BridgeApiError, match="requires a non-empty 'serial'"):
            await supervisor._handle_device_refresh({})
    finally:
        await supervisor._http_session.close()


async def test_device_refresh_before_startup_raises(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        with pytest.raises(BridgeApiError, match="not ready"):
            await supervisor._handle_device_refresh({"serial": SERIAL})
    finally:
        await supervisor._http_session.close()


async def test_device_refresh_unknown_serial_raises(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            with pytest.raises(BridgeApiError, match="unknown device serial"):
                await supervisor._handle_device_refresh({"serial": "does-not-exist"})
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_device_refresh_known_serial_triggers_a_resync(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            device_path = f"/fhapi/v1/api/rest/device/{fake.sysap_uuid}/{SERIAL}"
            requests_before = fake.request_count(device_path)
            reloads_before = supervisor.metrics.config_reloads

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as sender:
                await sender.publish(
                    f"{BASE}/bridge/request/device/refresh", orjson.dumps({"serial": SERIAL})
                )

            await _wait_until(
                lambda: supervisor.metrics.config_reloads > reloads_before, timeout_seconds=5.0
            )
            assert fake.request_count(device_path) == requests_before + 1
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_discovery_republish_before_startup_raises(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        with pytest.raises(BridgeApiError, match="not ready"):
            await supervisor._handle_discovery_republish({})
    finally:
        await supervisor._http_session.close()


async def test_discovery_republish_succeeds_once_started(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            data = await supervisor._handle_discovery_republish({})
            assert data == {}
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


# -------------------------------------------------------------- Home Assistant discovery lifecycle


async def test_removed_entities_are_retracted(tmp_path: Path) -> None:
    # P-35's cross-restart case: a device removed while the bridge was NOT running must still
    # get its discovery topic retracted on the next start, via discovery.json (docs/07 §4.2) --
    # the in-memory old-model-vs-new-model diff can't see it since there is no old model yet.
    other_serial = "ABB7F500E999"
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(
            _configuration(
                {SERIAL: _switch_device(SERIAL), other_serial: _switch_device(other_serial)}
            )
        )
        config = _config(tmp_path, broker.port, http_client)
        supervisor = Supervisor(config=config, profiles=REGISTRY, http_session=http_client.session)
        task = asyncio.create_task(supervisor.run())
        await _wait_until(lambda: supervisor._cold_start_done)
        assert supervisor._model is not None
        other_idx = supervisor._model.by_id[f"{other_serial}_ch0000"]
        removed_topic = supervisor._model.entities[other_idx].discovery[0][0]
        await supervisor.stop()
        await asyncio.wait_for(task, timeout=5.0)

    # A fresh process: same data_dir (discovery.json survives), but the SysAP no longer has
    # `other_serial` -- exactly what "removed while the bridge was down" looks like.
    async with (
        running_fake_broker(port=broker.port) as broker2,
        running_fake_sysap(FakeSysAp()) as (fake2, http_client2),
    ):
        fake2.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        config2 = _config(tmp_path, broker2.port, http_client2)
        supervisor2 = Supervisor(
            config=config2, profiles=REGISTRY, http_session=http_client2.session
        )
        task2 = asyncio.create_task(supervisor2.run())
        try:
            async with aiomqtt.Client("127.0.0.1", port=broker2.port) as observer:
                await observer.subscribe(removed_topic)
                async with asyncio.timeout(5.0):
                    msg = await anext(aiter(observer.messages))
                assert msg.payload == b""
        finally:
            await supervisor2.stop()
            await asyncio.wait_for(task2, timeout=5.0)


async def test_ha_birth_triggers_republish(tmp_path: Path) -> None:
    # P-36's named test: homeassistant/status: "online" must trigger a discovery republish.
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(
                tmp_path, broker.port, http_client, homeassistant_republish_delay_s=0.05
            ),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            assert supervisor._model is not None
            topic = supervisor._model.discovery[0][0]

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer:
                await observer.subscribe(topic)
                async with asyncio.timeout(5.0):
                    async_iter = aiter(observer.messages)
                    await anext(async_iter)  # the cold-start publish

                    async with aiomqtt.Client("127.0.0.1", port=broker.port) as sender:
                        await sender.publish("homeassistant/status", b"online")

                    msg = await anext(async_iter)  # the birth-triggered republish
                assert msg.payload == supervisor._model.discovery[0][1]
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_ha_birth_republish_is_delayed(tmp_path: Path) -> None:
    # P-37's named test: the republish must wait `republish_delay_s`, not fire instantly.
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client, homeassistant_republish_delay_s=1.0),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            assert supervisor._model is not None
            topic = supervisor._model.discovery[0][0]

            async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer:
                await observer.subscribe(topic)
                async with asyncio.timeout(5.0):
                    async_iter = aiter(observer.messages)
                    await anext(async_iter)  # the cold-start publish

                    async with aiomqtt.Client("127.0.0.1", port=broker.port) as sender:
                        await sender.publish("homeassistant/status", b"online")

                    start = time.monotonic()
                    await anext(async_iter)  # the birth-triggered republish
                    elapsed = time.monotonic() - start
                assert elapsed >= 0.5  # comfortably below the 1.0s delay but not instant
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_initial_publish_is_sequential(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # P-49's named test: the cold-start flood of discovery + state publishes must never overlap
    # (Mosquitto's default in-flight window is 20; `asyncio.gather`-ing 1000s of them would hang).
    concurrent = 0
    max_concurrent = 0
    original_publish = MqttClient.publish

    async def _tracking_publish(self: MqttClient, *args: Any, **kwargs: Any) -> None:
        nonlocal concurrent, max_concurrent
        concurrent += 1
        max_concurrent = max(max_concurrent, concurrent)
        try:
            await original_publish(self, *args, **kwargs)
        finally:
            concurrent -= 1

    monkeypatch.setattr(MqttClient, "publish", _tracking_publish)

    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        devices = {f"ABB7F5{i:06X}": _switch_device(f"ABB7F5{i:06X}") for i in range(20)}
        fake.set_configuration(_configuration(devices))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)

    assert max_concurrent == 1


# ------------------------------------------------------------ log_level, health, bridge_info


async def test_log_level_changes_the_root_logger_level(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    original = logging.getLogger().level
    try:
        data = await supervisor._handle_log_level({"level": "DEBUG"})
        assert data == {"level": "debug"}
        assert logging.getLogger().level == logging.DEBUG
    finally:
        logging.getLogger().setLevel(original)
        await supervisor._http_session.close()


async def test_log_level_requires_a_value(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        with pytest.raises(BridgeApiError, match="requires a non-empty 'level'"):
            await supervisor._handle_log_level({})
    finally:
        await supervisor._http_session.close()


async def test_log_level_rejects_an_unknown_level(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        with pytest.raises(BridgeApiError, match="unknown log level"):
            await supervisor._handle_log_level({"level": "verbose"})
    finally:
        await supervisor._http_session.close()


async def test_health_reports_all_checks_ok_once_started(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            data = await supervisor._handle_health({})
            assert {c["name"]: c["ok"] for c in data["checks"]} == {
                "mqtt_connected": True,
                "sysap_connected": True,
                "model_loaded": True,
            }
            info = data["info"]
            assert info["links"] == {
                "mqtt": "connected",
                "sysap_rest": "ok",
                "sysap_ws": "connected",
            }
            assert info["counts"]["entities"] == 1
            assert info["counts"]["devices"] == 1
            assert info["sysap"]["uuid"] == fake.sysap_uuid
            assert info["config"]["base_topic"] == BASE
            assert info["stats"]["config_reloads"] == 0
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_health_before_startup_reports_everything_down(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        data = await supervisor._handle_health({})
        assert {c["name"]: c["ok"] for c in data["checks"]} == {
            "mqtt_connected": False,
            "sysap_connected": False,
            "model_loaded": False,
        }
        assert data["info"]["counts"] == {}
        assert data["info"]["links"] == {
            "mqtt": "disconnected",
            "sysap_rest": "unknown",
            "sysap_ws": "disconnected",
        }
    finally:
        await supervisor._http_session.close()


async def test_bridge_info_never_contains_sysap_or_mqtt_secrets(tmp_path: Path) -> None:
    """The bridge_info half of `test_no_secrets_in_logs_or_bridge_info` (P-45); the log-output
    half lives alongside cli.py's `configure_logging` wiring.
    """
    sentinel_sysap_password = "sysap-sentinel-3f9a"
    sentinel_mqtt_password = "mqtt-sentinel-7c21"
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(
                tmp_path,
                broker.port,
                http_client,
                sysap_password=sentinel_sysap_password,
                mqtt_password=sentinel_mqtt_password,
            ),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            info = supervisor._build_bridge_info()
            serialized = orjson.dumps(info).decode()
            assert sentinel_sysap_password not in serialized
            assert sentinel_mqtt_password not in serialized
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


# ----------------------------------------------------- WP14: the complete bridge/info stats


async def test_bridge_info_stats_match_documented_shape(tmp_path: Path) -> None:
    """WP14's acceptance test: every key docs/04 §4.2's example shows is present and typed.

    Before WP14, five of them (`ws_frames`, `state_publishes`, `commands`, `command_errors`,
    `latency_ms`) had no counter behind them and were simply absent -- which made docs/05 §9's
    own profiling recipe impossible to follow in production.
    """
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            # Drive one real datapoint change through the whole pipeline, so the assertions
            # below are about counters that actually counted something rather than about
            # keys that merely exist.
            publishes_before = supervisor.metrics.state_publishes
            await fake.push_ws_frame({"datapoints": {f"{SERIAL}/ch0000/odp0000": "1"}})
            await _wait_until(lambda: supervisor.metrics.ws_frames >= 1)
            await _wait_until(lambda: supervisor.metrics.state_publishes > publishes_before)
            info = supervisor._build_bridge_info()
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)

    stats = info["stats"]
    for key in (
        "uptime_s",
        "ws_frames",
        "datapoints_in",
        "unmapped_datapoints",
        "state_publishes",
        "events",
        "commands",
        "command_errors",
        "reconnects_ws",
        "reconnects_mqtt",
        "config_reloads",
        "codec_errors",
        "latency_ms",
    ):
        assert key in stats, f"docs/04 §4.2 documents stats.{key}, and it is missing"
    assert set(stats["latency_ms"]) >= {"p50", "p95", "p99"}
    # docs/04 §4.2's `config` block shows `homeassistant`; it was the one key the code omitted.
    assert "homeassistant" in info["config"]
    # The whole point of the exercise: these are real numbers, not placeholders.
    assert stats["ws_frames"] >= 1
    assert stats["state_publishes"] >= 1


async def test_bridge_info_latency_is_null_before_anything_has_been_published() -> None:
    """An empty histogram reports `null`, not `0`: "no samples yet" and "everything took under a
    millisecond" are different facts and must not render identically.
    """
    assert LatencyHistogram().percentiles() == {"p50": None, "p95": None, "p99": None}


# ------------------------------------------------------------------------ virtualdevice/create


VIRTUAL_SERIAL = "6000AABBCC"


async def test_virtualdevice_create_requires_serial_type_ttl(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        with pytest.raises(BridgeApiError, match="requires a non-empty 'serial'"):
            await supervisor._handle_virtualdevice_create({"type": "x", "ttl": 180})
        with pytest.raises(BridgeApiError, match="requires a non-empty 'type'"):
            await supervisor._handle_virtualdevice_create({"serial": "x", "ttl": 180})
        with pytest.raises(BridgeApiError, match="requires an integer 'ttl'"):
            await supervisor._handle_virtualdevice_create({"serial": "x", "type": "y"})
        with pytest.raises(BridgeApiError, match="requires an integer 'ttl'"):
            await supervisor._handle_virtualdevice_create({"serial": "x", "type": "y", "ttl": True})
    finally:
        await supervisor._http_session.close()


async def test_virtualdevice_create_before_startup_raises(tmp_path: Path) -> None:
    supervisor = _bare_supervisor(tmp_path)
    try:
        with pytest.raises(BridgeApiError, match="not ready"):
            await supervisor._handle_virtualdevice_create(
                {"serial": VIRTUAL_SERIAL, "type": "SwitchingActuator", "ttl": 180}
            )
    finally:
        await supervisor._http_session.close()


async def test_virtual_device_ttl_keepalive(tmp_path: Path) -> None:
    """P-16: a finite-TTL virtual device gets a keepalive re-`PUT`ting at `ttl / 2`."""
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            async with aiomqtt.Client("127.0.0.1", port=broker.port) as sender:
                await sender.publish(
                    f"{BASE}/bridge/request/virtualdevice/create",
                    orjson.dumps(
                        {
                            "serial": VIRTUAL_SERIAL,
                            "type": "SwitchingActuator",
                            "ttl": 1,
                            "displayname": "My Virtual Switch",
                        }
                    ),
                )
            await _wait_until(
                lambda: fake.virtual_device_put_count(VIRTUAL_SERIAL) >= 1, timeout_seconds=5.0
            )
            assert fake.last_virtual_device_put(VIRTUAL_SERIAL) == {
                "type": "SwitchingActuator",
                "properties": {"ttl": "1", "displayname": "My Virtual Switch"},
            }
            # ttl=1 -> keepalive fires every 0.5s; wait long enough for at least one refresh.
            await _wait_until(
                lambda: fake.virtual_device_put_count(VIRTUAL_SERIAL) >= 2, timeout_seconds=5.0
            )
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_virtualdevice_create_with_no_expiry_ttl_starts_no_keepalive(
    tmp_path: Path,
) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            data = await supervisor._handle_virtualdevice_create(
                {"serial": VIRTUAL_SERIAL, "type": "SwitchingActuator", "ttl": 0}
            )
            assert data == {"serial": VIRTUAL_SERIAL}
            assert VIRTUAL_SERIAL not in supervisor._virtual_device_tasks
            await asyncio.sleep(0.2)
            assert fake.virtual_device_put_count(VIRTUAL_SERIAL) == 1
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_virtualdevice_create_recreate_replaces_the_keepalive_not_stacks_it(
    tmp_path: Path,
) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            await supervisor._handle_virtualdevice_create(
                {"serial": VIRTUAL_SERIAL, "type": "SwitchingActuator", "ttl": 300}
            )
            first_task = supervisor._virtual_device_tasks[VIRTUAL_SERIAL]
            await supervisor._handle_virtualdevice_create(
                {"serial": VIRTUAL_SERIAL, "type": "SwitchingActuator", "ttl": 300}
            )
            second_task = supervisor._virtual_device_tasks[VIRTUAL_SERIAL]
            assert first_task is not second_task
            # The loop suppresses CancelledError itself for a clean exit (mirrors
            # mqtt/client.py's _delayed_republish_retained), so it finishes rather than
            # ending up in the "cancelled" task state -- done() is what actually matters here.
            await _wait_until(first_task.done)
            assert len(supervisor._virtual_device_tasks) == 1
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_virtual_device_keepalive_is_stopped_cleanly_on_shutdown(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, broker.port, http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        task = asyncio.create_task(supervisor.run())
        try:
            await _wait_until(lambda: supervisor._cold_start_done)
            await supervisor._handle_virtualdevice_create(
                {"serial": VIRTUAL_SERIAL, "type": "SwitchingActuator", "ttl": 1}
            )
            assert VIRTUAL_SERIAL in supervisor._virtual_device_tasks
        finally:
            await supervisor.stop()
            await asyncio.wait_for(task, timeout=5.0)

        assert supervisor._virtual_device_tasks == {}
        count_at_shutdown = fake.virtual_device_put_count(VIRTUAL_SERIAL)
        await asyncio.sleep(1.5)  # long enough for another keepalive tick, if one were still live
        assert fake.virtual_device_put_count(VIRTUAL_SERIAL) == count_at_shutdown


# ---------------------------------------------------------------------------------- dry_run


async def test_dry_run_compiles_a_model_without_touching_mqtt(tmp_path: Path) -> None:
    async with running_fake_sysap(FakeSysAp()) as (fake, http_client):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        # An unreachable MQTT port -- dry_run must never try to connect, or this would hang/fail.
        supervisor = Supervisor(
            config=_config(tmp_path, free_port(), http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        model = await supervisor.dry_run()
        assert len(model.entities) == 1
        assert supervisor._mqtt is None
        assert supervisor._ws is None
        assert supervisor._model is model


async def test_dry_run_retries_once_with_jid_when_username_is_rejected(tmp_path: Path) -> None:
    async with running_fake_sysap(FakeSysAp()) as (fake, http_client):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        fake.require_username("abc123@busch-jaeger.de")
        supervisor = Supervisor(
            config=_config(tmp_path, free_port(), http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        model = await supervisor.dry_run()
        assert len(model.entities) == 1


async def test_dry_run_raises_when_jid_fallback_also_fails(tmp_path: Path) -> None:
    async with running_fake_sysap(FakeSysAp()) as (fake, http_client):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        fake.require_username("someone-else-entirely")
        supervisor = Supervisor(
            config=_config(tmp_path, free_port(), http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        with pytest.raises(AuthenticationError):
            await supervisor.dry_run()


async def test_dry_run_picks_up_a_persisted_alias(tmp_path: Path) -> None:
    async with running_fake_sysap(FakeSysAp()) as (fake, http_client):
        fake.set_configuration(_configuration({SERIAL: _switch_device(SERIAL)}))
        supervisor = Supervisor(
            config=_config(tmp_path, free_port(), http_client),
            profiles=REGISTRY,
            http_session=http_client.session,
        )
        supervisor._entities_store.set_alias(f"{SERIAL}_ch0000", "kitchen_light")
        await supervisor._entities_store.save()
        model = await supervisor.dry_run()
        idx = model.by_id[f"{SERIAL}_ch0000"]
        assert model.entities[idx].state_topic == f"{BASE}/kitchen_light"
