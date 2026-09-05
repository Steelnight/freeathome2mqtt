"""bench_ingest: sustained ingest without falling behind (docs/05 §1 P3; §8; docs/11 WP6).

docs/05 §8 specifies "60 s of synthetic traffic at 5 000 dp/s". This runs a **5 s** window at the
same 5 000 dp/s target instead: the property under test -- the publisher's coalescing loop keeping
pace with the arrival rate rather than accumulating a backlog -- is a rate property that manifests
within a handful of coalescing windows (20ms each; a 5 s run already contains ~250 of them), and a
literal 60 s per-PR bench test does not buy additional confidence for the cost in CI time. A true
60 s (or longer) continuous-traffic check belongs with the nightly soak suite (docs/10 §8), not
this bench suite -- see the corrected note in docs/05 §8's bench_ingest row.

Traffic arrives as batched WS frames (multiple datapoints per frame), matching how a real SysAP
actually reports bursts of activity (docs/02 §4: "one `orjson.loads` per FRAME, not per datapoint"),
rather than one WS round trip per datapoint.
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

from . import _record

pytestmark = pytest.mark.bench

SERIAL = "ABB7F500E17A"
BASE = "freeathome2mqtt"
_NUM_ENTITIES = 100
_TARGET_RATE_DPS = 5000  # P3
_FRAME_INTERVAL_S = 0.01
_DPS_PER_FRAME = int(_TARGET_RATE_DPS * _FRAME_INTERVAL_S)
_DURATION_S = 5.0  # see module docstring for why this is 5s, not docs/05 §8's literal 60s
_COALESCE_MS = 20
_DRAIN_TAIL_BUDGET_SECONDS = 0.5  # a small, fixed multiple of one coalescing window


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


async def _generate_traffic(fake: FakeSysAp, *, duration_s: float) -> int:
    """Batched frames at the target rate (docs/02 §4: frames carry many datapoints each).

    Paced against a fixed schedule (`next_tick` advances by exactly one interval every time)
    rather than a plain ``sleep(interval)`` after each frame, so per-iteration overhead shortens
    the next sleep instead of silently compounding into a rate below the P3 target.
    """
    deadline = time.monotonic() + duration_s
    next_tick = time.monotonic()
    sent = 0
    counter = 0
    while time.monotonic() < deadline:
        frame = {}
        for _ in range(_DPS_PER_FRAME):
            entity_idx = counter % _NUM_ENTITIES
            frame[_key(entity_idx)] = str(counter)
            counter += 1
        await fake.push_ws_frame({"datapoints": frame})
        sent += len(frame)
        next_tick += _FRAME_INTERVAL_S
        sleep_for = next_tick - time.monotonic()
        if sleep_for > 0:
            await asyncio.sleep(sleep_for)
    return sent


async def _run_sustained_ingest() -> tuple[int, Metrics, StateStore, float]:
    """Returns (sent, metrics, state, drain_tail_seconds)."""
    entities = [_entity(i) for i in range(_NUM_ENTITIES)]
    state = StateStore(entities)
    metrics = Metrics()

    async with contextlib.AsyncExitStack() as stack:
        broker = await stack.enter_async_context(running_fake_broker())
        client, mqtt_task = await _connected_mqtt_client(broker)
        stack.push_async_callback(_stop_client, client, mqtt_task)

        ingress = Ingress(
            entities=entities,
            ingress_table=_ingress_table(),
            state=state,
            events=EventPublisher(mqtt=client, base_topic=BASE),
            metrics=metrics,
        )
        publisher = Publisher(
            entities=entities,
            state=state,
            mqtt=client,
            coalesce_ms=_COALESCE_MS,
            publish_last_changed=False,
        )
        publisher_task = asyncio.create_task(publisher.run())
        stack.push_async_callback(_cancel_task, publisher_task)

        fake, http_client = await stack.enter_async_context(running_fake_sysap(FakeSysAp()))
        reader = _reader_for(http_client, ingress.process_frame)
        reader_task = asyncio.create_task(reader.run())
        stack.push_async_callback(_stop_reader, reader, reader_task)

        await _wait_until(lambda: reader.reconnect_count >= 1)
        sent = await _generate_traffic(fake, duration_s=_DURATION_S)

        await _wait_until(lambda: metrics.datapoints_in >= sent)
        drain_start = time.monotonic()
        await _wait_until(lambda: not state.dirty, timeout_seconds=5.0)
        drain_tail = time.monotonic() - drain_start

    return sent, metrics, state, drain_tail


async def test_bench_ingest_sustains_5000_dps_without_falling_behind() -> None:
    sent, metrics, state, drain_tail = await _run_sustained_ingest()

    achieved_rate = sent / _DURATION_S
    _record.record("tests/bench/test_bench_ingest.py::drain_tail_seconds", drain_tail)
    assert achieved_rate >= _TARGET_RATE_DPS * 0.9  # P3, with generous scheduling slack
    assert metrics.datapoints_in == sent  # no frame loss
    # "never falls behind by more than 1 window": once traffic stops, the publisher catches up
    # almost immediately rather than draining a backlog that grew throughout the run.
    assert drain_tail <= _DRAIN_TAIL_BUDGET_SECONDS
    assert state.dirty == set()
