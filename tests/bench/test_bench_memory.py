"""bench_memory: RSS at 1 000 entities, steady state (docs/05 §1 P9; §6; §8; docs/12 WP13).

P9 was one of two budgets in docs/05 §1 that nothing measured: `tests/test_soak.py` checks RSS
*growth* over the soak window, which is a different property, and never at 1 000 entities. This
closes it, in two halves that measure two different things:

* **The absolute footprint** is measured in a clean child interpreter (`_memory_child.py`). It has
  to be: this pytest process carries the test harness, an embedded `amqtt` broker and (outside
  CI's `--no-cov` bench job) coverage tracing, none of which ship in the container, so an
  in-process number would be measuring the harness as much as the bridge. The child imports the
  real runtime dependencies and builds the real compiled structures docs/05 §6's table enumerates.
* **The absence of a growth trend** is measured here, in-process, against the real pipeline -- fake
  SysAP, `WsReader`, `Ingress`, `StateStore`, `Publisher`, a real `MqttClient` and broker -- because
  a leak only shows up with traffic actually flowing through all of it. The trend is a ratio, so
  the harness's constant overhead cancels out of it.

docs/05 §8 specifies "`tracemalloc` + RSS after 10 min steady state". This runs a short window
instead, the same documented deviation `bench_ingest` (WP6) and `bench_resync` (WP8) already took:
a leak proportional to event count shows up within seconds at 5 000 dp/s, and the 24 h form of the
question is what `tests/test_soak.py` exists to answer nightly. `tracemalloc` is sampled either
side of the window so a failure names the allocation site rather than only the number
(docs/05 §9 step 3).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import subprocess
import sys
import time
import tracemalloc
from collections.abc import Callable
from pathlib import Path
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
_NUM_ENTITIES = 1000  # docs/00 §4's design target
_ATTRS_PER_ENTITY = 6  # -> 6 000 ingress bindings, docs/00 §4's "datapoints (outputs)" target
_P9_BUDGET_KIB = 120 * 1024  # docs/05 §1 P9: 120 MB RSS
_MAX_GROWTH = 0.10  # docs/10 §8's soak threshold, reused for the trend within this window
_WARMUP_S = 2.0
_WINDOW_S = 6.0
_FRAME_INTERVAL_S = 0.01
_DPS_PER_FRAME = 50
_SAMPLE_INTERVAL_S = 0.5
_CHILD = Path(__file__).with_name("_memory_child.py")


def current_rss_kib() -> int:
    """Live RSS from `/proc/self/status`, not `resource.ru_maxrss`: the latter is a monotonic
    *peak* that never comes back down, so it cannot show a plateau (`tests/test_soak.py` makes the
    same choice for the same reason).
    """
    for line in Path("/proc/self/status").read_text(encoding="utf-8").splitlines():
        if line.startswith("VmRSS:"):
            return int(line.split()[1])
    pytest.skip("/proc/self/status carries no VmRSS on this platform")


def _entity(idx: int) -> Entity:
    names = tuple(f"attr{i}" for i in range(_ATTRS_PER_ENTITY))
    return Entity(
        idx=idx,
        id=f"{SERIAL}_ch{idx:04d}",
        profile="test_profile",
        name=f"Entity {idx}",
        area="Area",
        device_serial=SERIAL,
        channel_id=f"ch{idx:04d}",
        attr_names=names,
        attr_kinds=tuple(AttrKind.STATE for _ in names),
        state_topic=f"{BASE}/test{idx}",
        set_topic=f"{BASE}/test{idx}/set",
        get_topic=f"{BASE}/test{idx}/get",
        availability_topic=f"{BASE}/test{idx}/availability",
        optimistic=False,
        discovery=(),
    )


def _key(entity_idx: int, attr_idx: int) -> str:
    return f"{SERIAL}/ch{entity_idx:04d}/odp{attr_idx:04d}"


def _ingress_table() -> dict[str, Binding]:
    int_codec = build_codec("int")
    return {
        _key(entity_idx, attr_idx): Binding(
            entity_idx=entity_idx,
            attr_idx=attr_idx,
            decode=int_codec.decode,
            kind=AttrKind.STATE,
            attr_bit=1 << attr_idx,
        )
        for entity_idx in range(_NUM_ENTITIES)
        for attr_idx in range(_ATTRS_PER_ENTITY)
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


async def _drive_traffic(fake: FakeSysAp, *, duration_s: float, counter: int) -> int:
    """Steady synthetic traffic, every datapoint a genuine change so R4 never short-circuits it."""
    deadline = time.monotonic() + duration_s
    next_tick = time.monotonic()
    while time.monotonic() < deadline:
        frame: dict[str, str] = {}
        for _ in range(_DPS_PER_FRAME):
            entity_idx = (counter // _ATTRS_PER_ENTITY) % _NUM_ENTITIES
            attr_idx = counter % _ATTRS_PER_ENTITY
            frame[_key(entity_idx, attr_idx)] = str(counter)
            counter += 1
        await fake.push_ws_frame({"datapoints": frame})
        next_tick += _FRAME_INTERVAL_S
        sleep_for = next_tick - time.monotonic()
        if sleep_for > 0:
            await asyncio.sleep(sleep_for)
    return counter


async def _steady_state_rss_samples() -> tuple[list[int], list[tracemalloc.StatisticDiff]]:
    """Run the real pipeline under load, sampling RSS across the window after a warmup."""
    entities = [_entity(i) for i in range(_NUM_ENTITIES)]
    state = StateStore(entities)
    samples: list[int] = []

    async with contextlib.AsyncExitStack() as stack:
        broker = await stack.enter_async_context(running_fake_broker())
        client, mqtt_task = await _connected_mqtt_client(broker)
        stack.push_async_callback(_stop_client, client, mqtt_task)

        ingress = Ingress(
            entities=entities,
            ingress_table=_ingress_table(),
            state=state,
            events=EventPublisher(mqtt=client),
            metrics=Metrics(),
        )
        publisher = Publisher(
            entities=entities, state=state, mqtt=client, coalesce_ms=20, publish_last_changed=True
        )
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

        # Warm up first: arena growth while the interpreter reaches steady state is not a leak,
        # and measuring across it would make every run look like one.
        counter = await _drive_traffic(fake, duration_s=_WARMUP_S, counter=0)
        tracemalloc.start()
        before = tracemalloc.take_snapshot()

        deadline = time.monotonic() + _WINDOW_S
        while time.monotonic() < deadline:
            counter = await _drive_traffic(fake, duration_s=_SAMPLE_INTERVAL_S, counter=counter)
            samples.append(current_rss_kib())

        after = tracemalloc.take_snapshot()
        top = after.compare_to(before, "lineno")[:5]
        tracemalloc.stop()

    return samples, top


def test_bench_memory_rss_within_budget() -> None:
    """P9's absolute half, measured in a clean interpreter (see the module docstring)."""
    argv = [sys.executable, str(_CHILD), str(_NUM_ENTITIES), str(_ATTRS_PER_ENTITY)]
    completed = subprocess.run(  # noqa: S603 -- fixed argv built here, our own child script
        argv,
        capture_output=True,
        text=True,
        timeout=180,
        check=True,
    )
    measured = json.loads(completed.stdout)
    _record.record("tests/bench/test_bench_memory.py::rss_kib", measured["rss_kib"])

    assert measured["entities"] == _NUM_ENTITIES
    assert measured["bindings"] == _NUM_ENTITIES * _ATTRS_PER_ENTITY
    assert measured["rss_kib"] <= _P9_BUDGET_KIB, (
        f"RSS {measured['rss_kib'] / 1024:.1f}MB at {_NUM_ENTITIES} entities exceeds P9's "
        f"{_P9_BUDGET_KIB / 1024:.0f}MB budget; baseline before the model was built was "
        f"{measured['rss_kib_baseline'] / 1024:.1f}MB"
    )


async def test_bench_memory_does_not_grow_under_steady_traffic() -> None:
    """P9's trend half: nothing on the hot path grows with *events* (docs/05 §3 R5, §6)."""
    samples, top = await _steady_state_rss_samples()

    assert len(samples) >= 4, "too few samples to establish a trend"
    growth = (samples[-1] - samples[0]) / samples[0]
    sites = "\n".join(str(stat) for stat in top)
    assert growth <= _MAX_GROWTH, (
        f"RSS grew {growth:.1%} across the steady-state window "
        f"({samples[0]}KiB -> {samples[-1]}KiB), which is a leak proportional to events.\n"
        f"Top allocation sites by growth:\n{sites}"
    )
