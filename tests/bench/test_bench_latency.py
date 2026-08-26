"""bench_latency: WS frame -> MQTT publish latency, p99 (docs/05 §1 P1, P2; §8; docs/11 WP6).

Runs the real pipeline end to end -- fake SysAP WebSocket, `WsReader`, `Ingress`, `StateStore`,
`Publisher`, a real `MqttClient` against an embedded broker -- per docs/10 §7's "measure the whole
path, not a microbenchmark of one function". 1 000 single datapoint changes, each timestamped from
push to observed publish, per docs/05 §8's exact bench_latency scenario.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Callable
from typing import Any

import pytest

from fakes.fake_broker import running_fake_broker
from fakes.fake_sysap import FakeSysAp, running_fake_sysap
from freeathome2mqtt.bus.events import EventPublisher
from freeathome2mqtt.bus.ingress import Ingress
from freeathome2mqtt.bus.publisher import Publisher
from freeathome2mqtt.bus.state import StateStore
from freeathome2mqtt.metrics import Metrics
from freeathome2mqtt.model.codecs import build_codec
from freeathome2mqtt.model.entity import AttrKind, Binding, Entity
from freeathome2mqtt.mqtt.client import MqttClient
from freeathome2mqtt.sysap.ws import WsReader

pytestmark = pytest.mark.bench

SERIAL = "ABB7F500E17A"
BASE = "freeathome2mqtt"
_NUM_ENTITIES = 10
_SAMPLES = 1000
_P1_BUDGET_SECONDS = 0.050
_P2_BUDGET_SECONDS = 0.010


def _entity(idx: int) -> Entity:
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
        availability_topic=None,
        optimistic=False,
        discovery=(),
    )


def _key(entity_idx: int) -> str:
    return f"{SERIAL}/ch{entity_idx:04d}/odp0000"


def _ingress_table() -> dict[str, Binding]:
    # "int", not "bool01": each sample must be a genuine change (rule R4 skips a repeat), and a
    # strictly increasing counter guarantees that regardless of how entities are cycled through.
    int_codec = build_codec("int")
    return {
        _key(i): Binding(
            entity_idx=i, attr_idx=0, decode=int_codec.decode, kind=AttrKind.STATE, attr_bit=1
        )
        for i in range(_NUM_ENTITIES)
    }


async def _wait_until(predicate: Callable[[], bool], *, timeout_seconds: float = 5.0) -> None:
    async with asyncio.timeout(timeout_seconds):
        while not predicate():  # noqa: ASYNC110 -- generic poll, no single event to await
            await asyncio.sleep(0.001)


def _p99(latencies: list[float]) -> float:
    ordered = sorted(latencies)
    return ordered[min(len(ordered) - 1, int(len(ordered) * 0.99))]


async def _connected_mqtt_client(broker: Any) -> tuple[MqttClient, asyncio.Task[None]]:
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


async def _stop_client(client: MqttClient, task: asyncio.Task[None]) -> None:
    await client.stop()
    await asyncio.wait_for(task, timeout=5.0)


async def _cancel_task(task: asyncio.Task[None]) -> None:
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task


def _reader_for(http_client: Any, on_frame: Callable[[Any], None]) -> WsReader:
    url = str(http_client.make_url("/fhapi/v1/api/ws"))
    return WsReader(
        url=url,
        username="installer",
        password="secret",
        session=http_client.session,
        on_frame=on_frame,
    )


async def _stop_reader(reader: WsReader, task: asyncio.Task[None]) -> None:
    await reader.stop()
    await asyncio.wait_for(task, timeout=5.0)


def _wrap_publish_with_timestamps(
    client: MqttClient, event: asyncio.Event, times: list[float]
) -> None:
    """Time each real publish call -- the hot path's actual "publish to MQTT" endpoint."""
    real_publish = client.publish

    async def _timed(topic: str, payload: bytes, *, qos: int = 0, retain: bool = False) -> None:
        await real_publish(topic, payload, qos=qos, retain=retain)
        times.append(time.monotonic())
        event.set()

    client.publish = _timed  # type: ignore[method-assign]


async def _drive_samples(fake: FakeSysAp, event: asyncio.Event, times: list[float]) -> list[float]:
    """Push one datapoint change at a time, waiting for its publish before the next (docs/05 §8:
    "1 000 single datapoint changes" -- isolated, not batched, so each latency sample reflects
    exactly one change's own journey through the pipeline).
    """
    latencies: list[float] = []
    for i in range(_SAMPLES):
        event.clear()
        start = time.monotonic()
        await fake.push_ws_frame({"datapoints": {_key(i % _NUM_ENTITIES): str(i)}})
        await asyncio.wait_for(event.wait(), timeout=2.0)
        latencies.append(times[-1] - start)
    return latencies


async def _measure_single_datapoint_latencies(*, coalesce_ms: int) -> list[float]:
    entities = [_entity(i) for i in range(_NUM_ENTITIES)]
    state = StateStore(entities)
    metrics = Metrics()

    async with contextlib.AsyncExitStack() as stack:
        broker = await stack.enter_async_context(running_fake_broker())
        client, mqtt_task = await _connected_mqtt_client(broker)
        stack.push_async_callback(_stop_client, client, mqtt_task)

        publish_event = asyncio.Event()
        publish_times: list[float] = []
        _wrap_publish_with_timestamps(client, publish_event, publish_times)

        ingress = Ingress(
            entities=entities,
            ingress_table=_ingress_table(),
            state=state,
            events=EventPublisher(mqtt=client),
            metrics=metrics,
        )
        publisher = Publisher(
            entities=entities,
            state=state,
            mqtt=client,
            coalesce_ms=coalesce_ms,
            publish_last_changed=False,
        )
        publisher_task = asyncio.create_task(publisher.run())
        stack.push_async_callback(_cancel_task, publisher_task)

        fake, http_client = await stack.enter_async_context(running_fake_sysap(FakeSysAp()))
        reader = _reader_for(http_client, ingress.process_frame)
        reader_task = asyncio.create_task(reader.run())
        stack.push_async_callback(_stop_reader, reader, reader_task)

        await _wait_until(lambda: reader.reconnect_count >= 1)
        latencies = await _drive_samples(fake, publish_event, publish_times)
    return latencies


async def test_bench_latency_meets_p1_budget_with_default_coalescing() -> None:
    latencies = await _measure_single_datapoint_latencies(coalesce_ms=20)
    assert _p99(latencies) <= _P1_BUDGET_SECONDS


async def test_bench_latency_meets_p2_budget_with_coalescing_disabled() -> None:
    latencies = await _measure_single_datapoint_latencies(coalesce_ms=0)
    assert _p99(latencies) <= _P2_BUDGET_SECONDS
