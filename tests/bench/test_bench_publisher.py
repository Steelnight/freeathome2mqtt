"""bench_burst / bench_dedup: a scene burst collapses to one publish per entity, and repeated
values produce none at all (docs/05 §1 P4, P12; docs/05 §8; docs/11 WP5).

Both are count invariants, not timing budgets, so unlike `bench_compile` these do not use
`pytest-benchmark`'s `benchmark` fixture -- they run the real coalescing loop against StateStore
and the embedded broker and assert the number of publishes it actually produced.
"""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from fakes.fake_broker import running_fake_broker
from freeathome2mqtt.bus.publisher import Publisher
from freeathome2mqtt.bus.state import StateStore
from freeathome2mqtt.model.entity import AttrKind, Entity
from freeathome2mqtt.mqtt.client import MqttClient

pytestmark = pytest.mark.bench

BASE = "freeathome2mqtt"


def _entities(count: int) -> list[Entity]:
    return [
        Entity(
            idx=i,
            id=f"SERIAL_ch{i:04d}",
            profile="switch_actuator",
            name="Test",
            area=None,
            device_serial="SERIAL",
            channel_id=f"ch{i:04d}",
            attr_names=("state", "brightness"),
            attr_kinds=(AttrKind.STATE, AttrKind.STATE),
            state_topic=f"{BASE}/test{i}",
            set_topic=f"{BASE}/test{i}/set",
            get_topic=f"{BASE}/test{i}/get",
            availability_topic=None,
            optimistic=False,
            discovery=(),
        )
        for i in range(count)
    ]


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


async def test_bench_burst_500_datapoints_40_entities_meets_p4_budget() -> None:
    entities = _entities(40)
    state = StateStore(entities)
    async with running_fake_broker() as broker:
        client, task = await _connected_client(broker)
        try:
            publisher = Publisher(
                entities=entities,
                state=state,
                mqtt=client,
                coalesce_ms=50,
                publish_last_changed=False,
            )
            run_task = asyncio.create_task(publisher.run())
            try:
                # One frame's worth of 500 datapoints spread across 40 entities (docs/05 §8's
                # bench_burst scenario), arriving as separate scheduler turns like real WS frames
                # would, not one uninterrupted synchronous burst -- otherwise the test would pass
                # even with the coalescing window removed, since nothing would get a chance to run
                # early regardless.
                for i in range(500):
                    state.apply(i % 40, i % 2, i)
                    if i % 25 == 0:
                        await asyncio.sleep(0)

                await asyncio.sleep(0.2)  # let the 50ms coalescing window close
                assert publisher.publish_count <= 40  # budget P4
                assert publisher.publish_count == 40  # every one of the 40 entities was touched
            finally:
                run_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await run_task
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)


async def test_bench_dedup_10000_repeated_frames_meets_p12_budget() -> None:
    entities = _entities(10)
    state = StateStore(entities)
    for entity_idx in range(10):
        state.seed(entity_idx, 0, True)
        state.seed(entity_idx, 1, 50)

    async with running_fake_broker() as broker:
        client, task = await _connected_client(broker)
        try:
            publisher = Publisher(
                entities=entities,
                state=state,
                mqtt=client,
                coalesce_ms=20,
                publish_last_changed=False,
            )
            run_task = asyncio.create_task(publisher.run())
            try:
                # 10000 frames, every one repeating a value already held -- change detection (R4)
                # must produce zero publishes, not merely few.
                for i in range(10_000):
                    entity_idx = i % 10
                    value = True if i % 2 == 0 else 50
                    state.apply(entity_idx, i % 2, value)
                    if i % 500 == 0:
                        await asyncio.sleep(0)

                await asyncio.sleep(0.1)
                assert publisher.publish_count == 0  # budget P12
            finally:
                run_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await run_task
        finally:
            await client.stop()
            await asyncio.wait_for(task, timeout=5.0)
