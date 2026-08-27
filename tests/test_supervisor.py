"""Tests for supervisor.py: startup order, task supervision, resync/reload, shutdown (ADR-001;
docs/02 §3, §7-8; docs/06 §3-4; docs/11 WP8).
"""

from __future__ import annotations

import asyncio
import inspect
import time
from pathlib import Path
from typing import Any

import aiohttp
import aiomqtt
import orjson
import pytest

from fakes.fake_broker import free_port, running_fake_broker
from fakes.fake_sysap import FakeSysAp, running_fake_sysap
from freeathome2mqtt.metrics import Metrics
from freeathome2mqtt.model.compiler import CompileStats, Model
from freeathome2mqtt.model.entity import AttrKind, Entity
from freeathome2mqtt.model.profiles import load_profile_registry
from freeathome2mqtt.supervisor import (
    _ESCALATION_THRESHOLD,
    Supervisor,
    SupervisorConfig,
    TaskDiedTooManyTimesError,
    _ReloadDebouncer,
    restart_on_failure,
)
from freeathome2mqtt.sysap.rest import AuthenticationError, ServerOverloadedError

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
            await supervisor._diff_and_apply(model, mqtt=supervisor._mqtt, rest=supervisor._rest)

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
