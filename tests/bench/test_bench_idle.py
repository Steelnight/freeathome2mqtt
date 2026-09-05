"""bench_idle: CPU while essentially nothing is happening (docs/05 §1 P10; §8; docs/12 WP13).

P10 was the second budget in docs/05 §1 that nothing measured. It matters because the bridge's
normal condition is *idle*: a house generates a handful of events a minute, and a bridge that
burns a measurable slice of a core doing nothing is a bridge that shortens the life of the
Raspberry Pi it shares with everything else.

What is measured is `time.process_time()` (user + system CPU for this process) over a window at
docs/05 §8's 0.1 events/s, as a fraction of elapsed wall time. The embedded broker and the fake
SysAP run *in this same process*, so their cost is counted too and the figure is pessimistic
against a real deployment where the broker is elsewhere -- deliberately: a budget that passes with
the harness included cannot be argued down. `_harness_only_cpu_fraction()` is measured alongside
it purely so a failure can say how much of the total was ours.

docs/05 §8 specifies a 10-minute window; this runs a short one, the same documented deviation
`bench_ingest` (WP6) and `bench_resync` (WP8) already took. Idle CPU is a *rate*, so it is visible
in seconds: what a longer window would add is confidence about slow drift, which is the nightly
soak test's job (docs/10 §8), not this one's.
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
_P10_BUDGET_FRACTION = 0.005  # docs/05 §1 P10: 0.5% of one core
_EVENT_INTERVAL_S = 10.0  # docs/05 §8's 0.1 events/s
_WINDOW_S = 10.0
_HARNESS_WINDOW_S = 3.0
_SETTLE_S = 1.0


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


async def _wait_until(predicate: Callable[[], bool], *, timeout_seconds: float = 10.0) -> None:
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


async def _stop_reader(reader: WsReader, task: asyncio.Task[None]) -> None:
    await reader.stop()
    await asyncio.wait_for(task, timeout=5.0)


async def _cpu_fraction_over(window_s: float, *, work: Callable[[], Any] | None = None) -> float:
    """CPU seconds consumed per wall second across `window_s`, optionally driving `work` first."""
    cpu_start = time.process_time()
    wall_start = time.monotonic()
    if work is not None:
        await work()
    remaining = window_s - (time.monotonic() - wall_start)
    if remaining > 0:
        await asyncio.sleep(remaining)
    return (time.process_time() - cpu_start) / (time.monotonic() - wall_start)


async def _measure_idle() -> tuple[float, float, int]:
    """Returns (idle CPU fraction, harness-only CPU fraction, events delivered)."""
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
            events=EventPublisher(mqtt=client),
            metrics=metrics,
        )
        publisher = Publisher(entities=entities, state=state, mqtt=client, coalesce_ms=20)
        publisher_task = asyncio.create_task(publisher.run())
        stack.push_async_callback(_cancel_task, publisher_task)

        fake, http_client = await stack.enter_async_context(running_fake_sysap(FakeSysAp()))
        reader = WsReader(
            url=str(http_client.make_url("/fhapi/v1/api/ws")),
            username="installer",
            password="secret",
            session=http_client.session,
            on_frame=ingress.process_frame,
        )
        reader_task = asyncio.create_task(reader.run())
        stack.push_async_callback(_stop_reader, reader, reader_task)
        await _wait_until(lambda: reader.reconnect_count >= 1)

        # Let connection setup finish before any measurement starts, so handshake cost is not
        # charged to the idle window.
        await asyncio.sleep(_SETTLE_S)
        harness_fraction = await _cpu_fraction_over(_HARNESS_WINDOW_S)

        sent = 0

        async def _trickle() -> None:
            nonlocal sent
            deadline = time.monotonic() + _WINDOW_S
            while time.monotonic() < deadline:
                await fake.push_ws_frame({"datapoints": {_key(0): str(sent)}})
                sent += 1
                await asyncio.sleep(min(_EVENT_INTERVAL_S, max(0.0, deadline - time.monotonic())))

        idle_fraction = await _cpu_fraction_over(_WINDOW_S, work=_trickle)

    return idle_fraction, harness_fraction, sent


async def test_bench_idle_cpu_within_budget() -> None:
    idle_fraction, harness_fraction, sent = await _measure_idle()
    _record.record("tests/bench/test_bench_idle.py::cpu_fraction", idle_fraction)

    assert sent >= 1, "the idle window delivered no events at all; it is not measuring the bridge"
    assert idle_fraction <= _P10_BUDGET_FRACTION, (
        f"idle CPU {idle_fraction:.3%} of one core exceeds P10's "
        f"{_P10_BUDGET_FRACTION:.1%} budget (harness alone measured {harness_fraction:.3%}, so "
        f"{max(0.0, idle_fraction - harness_fraction):.3%} of it is the bridge's own)"
    )
