"""Tests for bus/ingress.py: WS datapoints -> StateStore/EventPublisher, the hot path (docs/02 §4,
rules R1-R7; docs/11 WP6).
"""

from __future__ import annotations

import ast
import asyncio
import contextlib
import inspect
import logging
import textwrap
import time
from collections.abc import Mapping
from typing import Any
from unittest.mock import AsyncMock

import aiomqtt
import orjson
import pytest

from fakes.fake_broker import running_fake_broker
from fakes.fake_sysap import FakeSysAp, running_fake_sysap
from freeathome2mqtt.bus.events import EventPublisher
from freeathome2mqtt.bus.ingress import Ingress
from freeathome2mqtt.bus.publisher import Publisher
from freeathome2mqtt.bus.raw import RawStatePublisher
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
        self.bridge_events: list[tuple[str, dict[str, object]]] = []

    async def emit(self, entity: Entity, *, attribute: str, value: object, event: str) -> None:
        self.calls.append((entity.id, attribute, value, event))

    async def emit_bridge_event(self, event_type: str, data: dict[str, object]) -> None:
        self.bridge_events.append((event_type, data))


class _DisconnectedEvents:
    """Simulates emit() being called while MQTT is down (docs/06 §6's failure matrix)."""

    async def emit(self, entity: Entity, *, attribute: str, value: object, event: str) -> None:
        raise MqttClientNotConnectedError(entity.state_topic)

    async def emit_bridge_event(self, event_type: str, data: dict[str, object]) -> None:
        raise MqttClientNotConnectedError(event_type)


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


async def test_process_frame_publishes_raw_state_for_a_mapped_raw_topic() -> None:
    entities = [_entity(0, ("state",))]
    state = StateStore(entities)
    mqtt = AsyncMock()
    raw = RawStatePublisher(mqtt=mqtt, topics={_key(0): "freeathome2mqtt/raw/K"})
    ingress = Ingress(
        entities=entities,
        ingress_table=_table(0, "bool01", attr_bit=1),
        state=state,
        events=_FakeEvents(),
        metrics=Metrics(),
        raw=raw,
    )

    ingress.process_frame({"datapoints": {_key(0): "1"}})
    await _wait_until(lambda: mqtt.publish.await_count >= 1)

    mqtt.publish.assert_awaited_once_with("freeathome2mqtt/raw/K", b"1", qos=0, retain=True)
    # The compiled binding still applies too -- raw mode is additive, never a substitute.
    assert state.values[0][0] is True


def test_process_frame_skips_raw_publish_when_the_key_has_no_raw_topic() -> None:
    entities = [_entity(0, ("state",))]
    state = StateStore(entities)
    mqtt = AsyncMock()
    raw = RawStatePublisher(mqtt=mqtt, topics={})
    ingress = Ingress(
        entities=entities,
        ingress_table=_table(0, "bool01", attr_bit=1),
        state=state,
        events=_FakeEvents(),
        metrics=Metrics(),
        raw=raw,
    )

    ingress.process_frame({"datapoints": {_key(0): "1"}})

    mqtt.publish.assert_not_awaited()


def test_process_frame_without_a_raw_publisher_is_unaffected() -> None:
    entities = [_entity(0, ("state",))]
    state = StateStore(entities)
    ingress = Ingress(
        entities=entities,
        ingress_table=_table(0, "bool01", attr_bit=1),
        state=state,
        events=_FakeEvents(),
        metrics=Metrics(),
    )

    ingress.process_frame({"datapoints": {_key(0): "1"}})  # must not raise

    assert state.values[0][0] is True


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


# ------------------------------------------------------------------ WP14: the ws_frames counter


def test_every_frame_counts_towards_ws_frames() -> None:
    """docs/04 §4.2's `ws_frames`. Counted here rather than in `WsReader` because every frame
    body already passes through `process_frame`, so no new plumbing is needed for it.
    """
    metrics = Metrics()
    entities = [_entity(0, ("state",))]
    ingress = Ingress(
        entities=entities,
        ingress_table=_table(0, "int"),
        state=StateStore(entities),
        events=_FakeEvents(),
        metrics=metrics,
    )

    ingress.process_frame({"datapoints": {_key(0): "1"}})
    ingress.process_frame({"datapoints": {_key(0): "2"}})

    assert metrics.ws_frames == 2
    assert metrics.datapoints_in == 2


def test_a_frame_carrying_no_datapoints_still_counts_as_a_frame() -> None:
    """A `devices` or `devicesAdded` frame is a frame. Counting only datapoint-bearing ones would
    make `ws_frames` disagree with what the WebSocket actually delivered.
    """
    metrics = Metrics()
    entities = [_entity(0, ("state",))]
    ingress = Ingress(
        entities=entities,
        ingress_table=_table(0, "int"),
        state=StateStore(entities),
        events=_FakeEvents(),
        metrics=metrics,
    )

    ingress.process_frame({"devicesAdded": ["ABB7F500E17A"]})

    assert metrics.ws_frames == 1
    assert metrics.datapoints_in == 0


# ------------------------------------------------------- WP15: scenesTriggered (docs/01 §5.1)


SCENE_SERIAL = "ABB28000ABCD"


def _scene_frame(serial: str, channel: str, outputs: dict[str, dict[str, object]]) -> dict:
    """docs/01 §5.1's nested shape: sceneSerial -> channels -> outputs -> {value, pairingID}."""
    return {"scenesTriggered": {serial: {"channels": {channel: {"outputs": outputs}}}}}


def _scene_ingress() -> tuple[list[Entity], dict[str, Binding], StateStore, _FakeEvents, Metrics]:
    """One entity whose datapoint is addressed by the *scene's* serial, which is how a scene
    frame's contained outputs are keyed (docs/01 §5.1).
    """
    entities = [
        Entity(
            idx=0,
            id=f"{SCENE_SERIAL}_ch0000",
            profile="test_profile",
            name="Scene",
            area=None,
            device_serial=SCENE_SERIAL,
            channel_id="ch0000",
            attr_names=("state",),
            attr_kinds=(AttrKind.STATE,),
            state_topic=f"{BASE}/scene",
            set_topic=f"{BASE}/scene/set",
            get_topic=f"{BASE}/scene/get",
            availability_topic=None,
            optimistic=False,
            discovery=(),
        )
    ]
    codec = build_codec("bool01")
    table = {
        f"{SCENE_SERIAL}/ch0000/odp0000": Binding(
            entity_idx=0, attr_idx=0, decode=codec.decode, kind=AttrKind.STATE, attr_bit=1
        )
    }
    state = StateStore(entities)
    return entities, table, state, _FakeEvents(), Metrics()


async def test_scene_trigger_applies_contained_output_values_to_state() -> None:
    """docs/01 §5.1: "Also apply the contained output values to state -- a scene trigger is often
    the only notification you get for the channels it drove." Nothing did this before WP15.
    """
    entities, table, state, events, metrics = _scene_ingress()
    ingress = Ingress(
        entities=entities, ingress_table=table, state=state, events=events, metrics=metrics
    )

    ingress.process_frame(
        _scene_frame(SCENE_SERIAL, "ch0000", {"odp0000": {"value": "1", "pairingID": 256}})
    )

    assert state.values[0][0] is True
    assert state.dirty == {0}


async def test_scene_trigger_publishes_a_bridge_event() -> None:
    """The scene channel itself usually matches no profile, so state application alone would
    leave the trigger invisible. One `bridge/event` per scene serial makes it observable
    regardless (docs/04 §4.4).
    """
    entities, table, state, events, metrics = _scene_ingress()
    ingress = Ingress(
        entities=entities, ingress_table=table, state=state, events=events, metrics=metrics
    )

    ingress.process_frame(
        _scene_frame(SCENE_SERIAL, "ch0000", {"odp0000": {"value": "1", "pairingID": 256}})
    )
    await _wait_until(lambda: events.bridge_events != [])

    assert events.bridge_events == [("scene_triggered", {"serial": SCENE_SERIAL})]


async def test_scene_trigger_application_is_idempotent_with_datapoints() -> None:
    """docs/01 §5.1 marks "do the corresponding `datapoints` entries also arrive?" as
    **verify empirically**. The code is correct under either answer because change detection
    (R4) makes the duplicate a no-op -- this is the test that says so (P12).
    """
    entities, table, state, events, metrics = _scene_ingress()
    ingress = Ingress(
        entities=entities, ingress_table=table, state=state, events=events, metrics=metrics
    )
    key = f"{SCENE_SERIAL}/ch0000/odp0000"

    ingress.process_frame(_scene_frame(SCENE_SERIAL, "ch0000", {"odp0000": {"value": "1"}}))
    state.dirty.clear()  # stand in for the publisher having drained it
    ingress.process_frame({"datapoints": {key: "1"}})

    assert state.dirty == set()  # the duplicate produced no further publish at all


async def test_scene_trigger_for_an_unmapped_channel_is_counted_not_an_error() -> None:
    """A scene may drive channels this bridge filtered out; that is expected, not exceptional."""
    entities, table, state, events, metrics = _scene_ingress()
    ingress = Ingress(
        entities=entities, ingress_table=table, state=state, events=events, metrics=metrics
    )

    ingress.process_frame(_scene_frame(SCENE_SERIAL, "ch0099", {"odp0000": {"value": "1"}}))

    assert metrics.unmapped_datapoints == 1
    assert state.dirty == set()


async def test_scene_trigger_tolerates_a_malformed_outputs_block() -> None:
    """WS frames are untrusted input (CLAUDE.md §2 rule 1): a missing `outputs`, a non-dict
    entry, or an absent `value` must be skipped, never raise on the hot path.
    """
    entities, table, state, events, metrics = _scene_ingress()
    ingress = Ingress(
        entities=entities, ingress_table=table, state=state, events=events, metrics=metrics
    )

    ingress.process_frame({"scenesTriggered": {SCENE_SERIAL: {}}})
    ingress.process_frame({"scenesTriggered": {SCENE_SERIAL: {"channels": {"ch0000": {}}}}})
    ingress.process_frame(_scene_frame(SCENE_SERIAL, "ch0000", {"odp0000": {}}))
    ingress.process_frame(_scene_frame(SCENE_SERIAL, "ch0000", {"odp0000": "not-a-dict"}))

    assert state.dirty == set()


def test_scene_frames_do_not_introduce_an_await_on_the_hot_path() -> None:
    """P-25 / rule R1 extended to the new branch: the scene path must hand off, never await."""
    assert not _has_await(Ingress.process_frame)
    assert not _has_await(Ingress._process_scenes)


async def test_scene_trigger_reaches_mqtt_through_the_real_pipeline() -> None:
    """End to end over the fake SysAP and a real broker (docs/10 §3): a scene frame produced by
    the fake becomes a retained state publish and a `bridge/event`.
    """
    entities, table, state, _events, metrics = _scene_ingress()

    async with running_fake_broker() as broker, running_fake_sysap(FakeSysAp()) as (fake, http):
        client = MqttClient(
            host="127.0.0.1",
            port=broker.port,
            base_topic=BASE,
            sysap_serial=SCENE_SERIAL,
            backoff_initial=0.02,
            backoff_cap=0.2,
        )
        mqtt_task = asyncio.create_task(client.run())
        await _wait_until(lambda: client.reconnect_count >= 1)

        ingress = Ingress(
            entities=entities,
            ingress_table=table,
            state=state,
            events=EventPublisher(mqtt=client, base_topic=BASE),
            metrics=metrics,
        )
        publisher = Publisher(entities=entities, state=state, mqtt=client, coalesce_ms=0)
        publisher_task = asyncio.create_task(publisher.run())

        reader = WsReader(
            url=str(http.make_url("/fhapi/v1/api/ws")),
            username="installer",
            password="secret",
            session=http.session,
            on_frame=ingress.process_frame,
        )
        reader_task = asyncio.create_task(reader.run())
        await _wait_until(lambda: reader.reconnect_count >= 1)

        async with aiomqtt.Client("127.0.0.1", port=broker.port) as outsider:
            await outsider.subscribe(f"{BASE}/#")
            await fake.trigger_scene(SCENE_SERIAL, {"ch0000": {"odp0000": ("1", 256)}})

            seen: dict[str, Any] = {}
            async with asyncio.timeout(5.0):
                async for message in outsider.messages:
                    seen[str(message.topic)] = orjson.loads(message.payload)
                    if f"{BASE}/scene" in seen and f"{BASE}/bridge/event" in seen:
                        break

        publisher_task.cancel()
        reader_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await publisher_task
        with contextlib.suppress(asyncio.CancelledError):
            await reader_task
        await reader.stop()
        await client.stop()
        await asyncio.wait_for(mqtt_task, timeout=5.0)

    assert seen[f"{BASE}/scene"]["state"] is True
    assert seen[f"{BASE}/bridge/event"] == {
        "type": "scene_triggered",
        "data": {"serial": SCENE_SERIAL},
    }


async def test_a_scene_bridge_event_dropped_while_disconnected_is_logged_not_raised(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Same rule as an entity event (ADR-005): an edge missed while MQTT is down is gone, and is
    logged with context rather than swallowed or allowed to escape a background task.
    """
    entities, table, state, _events, metrics = _scene_ingress()
    ingress = Ingress(
        entities=entities,
        ingress_table=table,
        state=state,
        events=_DisconnectedEvents(),
        metrics=metrics,
    )

    with caplog.at_level(logging.WARNING):
        ingress.process_frame(_scene_frame(SCENE_SERIAL, "ch0000", {"odp0000": {"value": "1"}}))
        await _wait_until(lambda: "scene_triggered" in caplog.text)

    assert "MQTT not connected" in caplog.text
    # The state half still happened: a broker outage must not cost the value the scene carried.
    assert state.values[0][0] is True


async def test_a_scene_naming_a_non_dict_trigger_is_skipped() -> None:
    """Every level of the nested shape is untrusted (CLAUDE.md §2 rule 1)."""
    entities, table, state, events, metrics = _scene_ingress()
    ingress = Ingress(
        entities=entities, ingress_table=table, state=state, events=events, metrics=metrics
    )

    ingress.process_frame({"scenesTriggered": {SCENE_SERIAL: "not-a-dict"}})
    await _wait_until(lambda: events.bridge_events != [])

    assert state.dirty == set()
    assert events.bridge_events == [("scene_triggered", {"serial": SCENE_SERIAL})]


async def test_a_scene_channel_that_is_not_a_dict_is_skipped() -> None:
    entities, table, state, events, metrics = _scene_ingress()
    ingress = Ingress(
        entities=entities, ingress_table=table, state=state, events=events, metrics=metrics
    )

    ingress.process_frame(
        {"scenesTriggered": {SCENE_SERIAL: {"channels": {"ch0000": "not-a-dict"}}}}
    )

    assert state.dirty == set()
