"""Tests for bus/ingress.py: WS datapoints -> StateStore/EventPublisher, the hot path (docs/02 §4,
rules R1-R7; docs/11 WP6).
"""

from __future__ import annotations

import ast
import asyncio
import contextlib
import inspect
import textwrap
import time
from collections.abc import Mapping
from typing import Any

import pytest

from fakes.fake_broker import running_fake_broker
from fakes.fake_sysap import FakeSysAp, running_fake_sysap
from freeathome2mqtt.bus.ingress import Ingress
from freeathome2mqtt.bus.publisher import Publisher
from freeathome2mqtt.bus.state import StateStore
from freeathome2mqtt.metrics import Metrics
from freeathome2mqtt.model.codecs import build_codec
from freeathome2mqtt.model.entity import AttrKind, Binding, Entity
from freeathome2mqtt.mqtt.client import MqttClient, MqttClientNotConnectedError
from freeathome2mqtt.sysap.ws import WsReader

SERIAL = "ABB7F500E17A"
BASE = "freeathome2mqtt"


def _entity(idx: int, attr_names: tuple[str, ...]) -> Entity:
    return Entity(
        idx=idx,
        id=f"{SERIAL}_ch{idx:04d}",
        profile="test_profile",
        name="Test",
        area=None,
        device_serial=SERIAL,
        channel_id=f"ch{idx:04d}",
        attr_names=attr_names,
        attr_kinds=tuple(AttrKind.STATE for _ in attr_names),
        state_topic=f"{BASE}/test{idx}",
        set_topic=f"{BASE}/test{idx}/set",
        get_topic=f"{BASE}/test{idx}/get",
        availability_topic=None,
        optimistic=False,
        discovery=(),
    )


def _key(entity_idx: int, dp: str = "odp0000") -> str:
    return f"{SERIAL}/ch{entity_idx:04d}/{dp}"


def _table(
    entity_idx: int, codec_name: str, *, kind: AttrKind = AttrKind.STATE, attr_bit: int = 0
) -> Mapping[str, Binding]:
    codec = build_codec(codec_name)
    binding = Binding(
        entity_idx=entity_idx, attr_idx=0, decode=codec.decode, kind=kind, attr_bit=attr_bit
    )
    return {_key(entity_idx): binding}


class _FakeEvents:
    """A recording double for EventPublisher -- only the call contract matters here; the real
    EventPublisher's own MQTT behaviour is covered by test_events.py.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, object, str]] = []

    async def emit(self, entity: Entity, *, attribute: str, value: object, event: str) -> None:
        self.calls.append((entity.id, attribute, value, event))


class _DisconnectedEvents:
    """Simulates emit() being called while MQTT is down (docs/06 §6's failure matrix)."""

    async def emit(self, entity: Entity, *, attribute: str, value: object, event: str) -> None:
        raise MqttClientNotConnectedError(entity.state_topic)


async def _wait_until(predicate, *, timeout_seconds: float = 5.0, interval: float = 0.005) -> None:
    async with asyncio.timeout(timeout_seconds):
        while not predicate():  # noqa: ASYNC110 -- generic poll, no single event to await
            await asyncio.sleep(interval)


def test_process_frame_updates_state_for_a_mapped_datapoint() -> None:
    entities = [_entity(0, ("state",))]
    state = StateStore(entities)
    ingress = Ingress(
        entities=entities,
        ingress_table=_table(0, "bool01", attr_bit=1),
        state=state,
        events=_FakeEvents(),
        metrics=Metrics(),
    )

    ingress.process_frame({"datapoints": {_key(0): "1"}})

    assert state.values[0][0] is True
    assert state.dirty == {0}


def test_process_frame_counts_unmapped_datapoints_without_touching_state() -> None:
    entities = [_entity(0, ("state",))]
    state = StateStore(entities)
    metrics = Metrics()
    ingress = Ingress(
        entities=entities, ingress_table={}, state=state, events=_FakeEvents(), metrics=metrics
    )

    ingress.process_frame({"datapoints": {_key(0): "1"}})

    assert metrics.unmapped_datapoints == 1
    assert state.dirty == set()


def test_process_frame_ignores_a_frame_without_a_datapoints_key() -> None:
    entities = [_entity(0, ("state",))]
    state = StateStore(entities)
    ingress = Ingress(
        entities=entities, ingress_table={}, state=state, events=_FakeEvents(), metrics=Metrics()
    )

    ingress.process_frame({"devicesAdded": ["DEV1"]})  # must not raise

    assert state.dirty == set()


def test_process_frame_change_detection_skips_a_repeated_value() -> None:
    entities = [_entity(0, ("state",))]
    state = StateStore(entities)
    metrics = Metrics()
    ingress = Ingress(
        entities=entities,
        ingress_table=_table(0, "bool01", attr_bit=1),
        state=state,
        events=_FakeEvents(),
        metrics=metrics,
    )

    ingress.process_frame({"datapoints": {_key(0): "1"}})
    state.take_dirty()
    ingress.process_frame({"datapoints": {_key(0): "1"}})  # same value again

    assert state.dirty == set()  # R4: no re-mark on a repeat
    assert metrics.datapoints_in == 2  # both still counted as received


async def test_process_frame_emits_an_event_for_event_kind_bindings() -> None:
    entities = [_entity(0, ("pressed",))]
    state = StateStore(entities)
    events = _FakeEvents()
    metrics = Metrics()
    ingress = Ingress(
        entities=entities,
        ingress_table=_table(0, "trigger", kind=AttrKind.EVENT),
        state=state,
        events=events,
        metrics=metrics,
    )

    ingress.process_frame({"datapoints": {_key(0): "1"}})
    await _wait_until(lambda: len(events.calls) >= 1)

    assert events.calls == [(f"{SERIAL}_ch0000", "pressed", True, "press")]
    assert metrics.events == 1
    # event-kind attributes bypass StateStore entirely (ADR-005, P-32): no dirty mark.
    assert state.dirty == set()


async def test_process_frame_emits_twice_for_two_identical_presses() -> None:
    # P-32: an event is never deduplicated, unlike a state attribute.
    entities = [_entity(0, ("pressed",))]
    state = StateStore(entities)
    events = _FakeEvents()
    ingress = Ingress(
        entities=entities,
        ingress_table=_table(0, "trigger", kind=AttrKind.EVENT),
        state=state,
        events=events,
        metrics=Metrics(),
    )

    ingress.process_frame({"datapoints": {_key(0): "1"}})
    ingress.process_frame({"datapoints": {_key(0): "1"}})
    await _wait_until(lambda: len(events.calls) >= 2)

    assert len(events.calls) == 2


async def test_process_frame_logs_and_survives_a_disconnected_event_publish(
    caplog: pytest.LogCaptureFixture,
) -> None:
    # docs/06 §6 / CLAUDE.md rule 7: a dropped event while MQTT is down is logged with context,
    # never silently swallowed, and never crashes the fire-and-forget task.
    entities = [_entity(0, ("pressed",))]
    state = StateStore(entities)
    ingress = Ingress(
        entities=entities,
        ingress_table=_table(0, "trigger", kind=AttrKind.EVENT),
        state=state,
        events=_DisconnectedEvents(),
        metrics=Metrics(),
    )

    with caplog.at_level("WARNING"):
        ingress.process_frame({"datapoints": {_key(0): "1"}})
        await _wait_until(lambda: len(ingress._event_tasks) == 0)

    assert "MQTT not connected" in caplog.text
    assert f"{SERIAL}_ch0000" in caplog.text


def test_process_frame_counts_codec_errors_for_malformed_non_empty_datapoints() -> None:
    entities = [_entity(0, ("state",))]
    state = StateStore(entities)
    metrics = Metrics()
    ingress = Ingress(
        entities=entities,
        ingress_table=_table(0, "int", attr_bit=1),
        state=state,
        events=_FakeEvents(),
        metrics=metrics,
    )

    ingress.process_frame({"datapoints": {_key(0): "not-a-number"}})

    assert metrics.codec_errors == 1
    assert state.values[0][0] is None  # decoded to None, no spurious value


def test_process_frame_does_not_count_an_empty_datapoint_as_a_codec_error() -> None:
    # P-04: "" is a legitimate null, not malformed input.
    entities = [_entity(0, ("state",))]
    state = StateStore(entities)
    metrics = Metrics()
    ingress = Ingress(
        entities=entities,
        ingress_table=_table(0, "int", attr_bit=1),
        state=state,
        events=_FakeEvents(),
        metrics=metrics,
    )

    ingress.process_frame({"datapoints": {_key(0): ""}})

    assert metrics.codec_errors == 0


def test_process_frame_clears_the_unconfirmed_bit_via_attr_bit() -> None:
    entities = [_entity(0, ("state",))]
    state = StateStore(entities)
    state.unconfirmed[0] = 0b1
    ingress = Ingress(
        entities=entities,
        ingress_table=_table(0, "bool01", attr_bit=0b1),
        state=state,
        events=_FakeEvents(),
        metrics=Metrics(),
    )

    ingress.process_frame({"datapoints": {_key(0): "1"}})

    assert state.unconfirmed[0] == 0


def _has_await(func: Any) -> bool:
    tree = ast.parse(textwrap.dedent(inspect.getsource(func)))
    return any(isinstance(node, ast.Await) for node in ast.walk(tree))


def _five_entities() -> list[Entity]:
    return [_entity(i, ("state",)) for i in range(5)]


def _five_entity_table() -> dict[str, Binding]:
    bool01 = build_codec("bool01")
    return {
        _key(i): Binding(
            entity_idx=i, attr_idx=0, decode=bool01.decode, kind=AttrKind.STATE, attr_bit=1
        )
        for i in range(5)
    }


async def _wait_reader_connected(reader: WsReader) -> None:
    await _wait_until(lambda: reader.reconnect_count >= 1)


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


def _slow_down_publish(client: MqttClient, *, delay: float = 0.3) -> None:
    """Simulate P-25's "broker artificially slowed" by delaying every real publish call."""
    real_publish = client.publish

    async def _delayed(topic: str, payload: bytes, *, qos: int = 0, retain: bool = False) -> None:
        await asyncio.sleep(delay)
        await real_publish(topic, payload, qos=qos, retain=retain)

    client.publish = _delayed  # type: ignore[method-assign]


async def _push_100_frames_and_time(ingress: Ingress, metrics: Metrics) -> float:
    """Push 100 single-datapoint frames through a real WsReader, timing the push loop itself."""
    async with running_fake_sysap(FakeSysAp()) as (fake, http_client):
        url = str(http_client.make_url("/fhapi/v1/api/ws"))
        reader = WsReader(
            url=url,
            username="installer",
            password="secret",
            session=http_client.session,
            on_frame=ingress.process_frame,
        )
        reader_task = asyncio.create_task(reader.run())
        try:
            await _wait_reader_connected(reader)
            start = time.monotonic()
            for i in range(100):
                value = "1" if i % 2 == 0 else "0"
                await fake.push_ws_frame({"datapoints": {_key(i % 5): value}})
            elapsed = time.monotonic() - start
            # No frame loss (P-25's other half): every pushed datapoint eventually lands,
            # decoupled from how slow the publisher is at draining them.
            await _wait_until(lambda: metrics.datapoints_in >= 100)
            return elapsed
        finally:
            await reader.stop()
            await asyncio.wait_for(reader_task, timeout=5.0)


async def _ingest_elapsed_with_slow_publisher() -> float:
    """The full pipeline, publisher artificially slowed, returning how long ingestion took."""
    entities = _five_entities()
    state = StateStore(entities)
    metrics = Metrics()
    ingress = Ingress(
        entities=entities,
        ingress_table=_five_entity_table(),
        state=state,
        events=_FakeEvents(),
        metrics=metrics,
    )

    async with running_fake_broker() as broker:
        mqtt_client, mqtt_task = await _connected_mqtt_client(broker)
        try:
            _slow_down_publish(mqtt_client)
            publisher = Publisher(entities=entities, state=state, mqtt=mqtt_client, coalesce_ms=20)
            publisher_task = asyncio.create_task(publisher.run())
            try:
                return await _push_100_frames_and_time(ingress, metrics)
            finally:
                publisher_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await publisher_task
        finally:
            await mqtt_client.stop()
            await asyncio.wait_for(mqtt_task, timeout=5.0)


async def test_ws_reader_never_awaits_io() -> None:
    # Static check (P-25, rule R1): the frame-dispatch path awaits nothing but the socket --
    # `_dispatch`/`_dispatch_one` are plain synchronous functions handing off to `Ingress`, which
    # must stay synchronous too, all the way down to `_process_datapoint`.
    assert not _has_await(WsReader._dispatch)
    assert not _has_await(WsReader._dispatch_one)
    assert not _has_await(Ingress.process_frame)
    assert not _has_await(Ingress._process_datapoint)

    # Load test: with MQTT publishing artificially slowed, the reader must keep receiving and
    # dispatching every frame without loss or delay -- it must never block on the publisher,
    # which runs as a fully independent task (docs/02 §3).
    elapsed = await _ingest_elapsed_with_slow_publisher()

    # 100 x 0.3s slow publishes would take 30s if ingestion were blocked on the publisher; a
    # generous 5s bound conclusively shows it never was.
    assert elapsed < 5.0
