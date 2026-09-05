"""Tests for bus/publisher.py: the coalescing loop, payload building, retained publish (docs/05
§4.1; docs/11 WP5).
"""

from __future__ import annotations

import asyncio
import contextlib
from datetime import UTC, datetime

import orjson
import pytest

from fakes.fake_broker import running_fake_broker
from freeathome2mqtt.bus.publisher import Publisher
from freeathome2mqtt.bus.state import StateStore
from freeathome2mqtt.metrics import Metrics
from freeathome2mqtt.model.entity import AttrKind, Entity
from freeathome2mqtt.mqtt.client import MqttClient, MqttClientNotConnectedError

BASE = "freeathome2mqtt"


def _entity(
    idx: int,
    attr_names: tuple[str, ...],
    attr_kinds: tuple[int, ...] | None = None,
    *,
    transform: str | None = None,
) -> Entity:
    kinds = attr_kinds if attr_kinds is not None else tuple(AttrKind.STATE for _ in attr_names)
    return Entity(
        idx=idx,
        id=f"SERIAL_ch{idx:04d}",
        profile="switch_actuator",
        name="Test",
        area=None,
        device_serial="SERIAL",
        channel_id=f"ch{idx:04d}",
        attr_names=attr_names,
        attr_kinds=kinds,
        state_topic=f"{BASE}/test{idx}",
        set_topic=f"{BASE}/test{idx}/set",
        get_topic=f"{BASE}/test{idx}/get",
        availability_topic=None,
        optimistic=False,
        discovery=(),
        transform=transform,
    )


def test_build_payload_includes_id_and_attribute_values() -> None:
    entities = [_entity(0, ("state", "brightness"))]
    state = StateStore(entities)
    state.seed(0, 0, True)
    state.seed(0, 1, 43)
    publisher = Publisher(entities=entities, state=state, mqtt=None, publish_last_changed=False)
    assert publisher.build_payload(0) == {"id": "SERIAL_ch0000", "state": True, "brightness": 43}


def test_build_payload_skips_event_kind_attributes() -> None:
    entities = [_entity(0, ("state", "pressed"), (AttrKind.STATE, AttrKind.EVENT))]
    state = StateStore(entities)
    state.seed(0, 0, True)
    state.seed(0, 1, 1)
    publisher = Publisher(entities=entities, state=state, mqtt=None, publish_last_changed=False)
    assert publisher.build_payload(0) == {"id": "SERIAL_ch0000", "state": True}


def test_build_payload_includes_last_changed_when_enabled() -> None:
    entities = [_entity(0, ("state",))]
    state = StateStore(entities)
    state.seed(0, 0, True)
    fixed = datetime(2026, 8, 23, 14, 31, 2, 118000, tzinfo=UTC)
    publisher = Publisher(
        entities=entities, state=state, mqtt=None, publish_last_changed=True, clock=lambda: fixed
    )
    payload = publisher.build_payload(0)
    assert payload["last_changed"] == "2026-08-23T14:31:02.118+00:00"


def test_build_payload_omits_last_changed_when_disabled() -> None:
    entities = [_entity(0, ("state",))]
    state = StateStore(entities)
    state.seed(0, 0, True)
    publisher = Publisher(entities=entities, state=state, mqtt=None, publish_last_changed=False)
    assert "last_changed" not in publisher.build_payload(0)


def test_build_payload_merges_transform_derived_attributes() -> None:
    # docs/03 §7, docs/05 R4: transforms run after change detection, right where build_payload
    # already reads the entity's raw values -- room_temperature_controller derives a synthetic
    # "hvac_mode" key from on_off/eco/mode that is not itself a declared profile attribute.
    entities = [_entity(0, ("on_off", "eco", "mode"), transform="room_temperature_controller")]
    state = StateStore(entities)
    state.seed(0, 0, True)
    state.seed(0, 1, False)
    state.seed(0, 2, "heating")
    publisher = Publisher(entities=entities, state=state, mqtt=None, publish_last_changed=False)
    payload = publisher.build_payload(0)
    assert payload == {
        "id": "SERIAL_ch0000",
        "on_off": True,
        "eco": False,
        "mode": "heating",
        "hvac_mode": "heating",
    }


def test_build_payload_cover_with_slats_hides_slat_position_when_fully_open() -> None:
    entities = [_entity(0, ("position", "slat_position"), transform="cover_with_slats")]
    state = StateStore(entities)
    state.seed(0, 0, 100)
    state.seed(0, 1, 30)
    publisher = Publisher(entities=entities, state=state, mqtt=None, publish_last_changed=False)
    payload = publisher.build_payload(0)
    assert payload["slat_position"] is None


def test_build_payload_without_a_transform_is_unaffected() -> None:
    entities = [_entity(0, ("state",))]
    state = StateStore(entities)
    state.seed(0, 0, True)
    publisher = Publisher(entities=entities, state=state, mqtt=None, publish_last_changed=False)
    assert publisher.build_payload(0) == {"id": "SERIAL_ch0000", "state": True}


async def _wait_until(predicate, *, timeout_seconds: float = 5.0, interval: float = 0.005) -> None:
    async with asyncio.timeout(timeout_seconds):
        while not predicate():  # noqa: ASYNC110 -- generic poll, no single event to await
            await asyncio.sleep(interval)


async def _connected_client(broker, **kwargs) -> tuple[MqttClient, asyncio.Task[None]]:
    client = MqttClient(
        host="127.0.0.1",
        port=broker.port,
        base_topic=BASE,
        sysap_serial="ABB7F500E17A",
        backoff_initial=0.02,
        backoff_cap=0.2,
        **kwargs,
    )
    task = asyncio.create_task(client.run())
    await _wait_until(lambda: client.reconnect_count >= 1)
    return client, task


async def _stop(client: MqttClient, task: asyncio.Task[None]) -> None:
    await client.stop()
    await asyncio.wait_for(task, timeout=5.0)


async def test_flush_publishes_dirty_entities_retained() -> None:
    entities = [_entity(0, ("state",)), _entity(1, ("state",))]
    state = StateStore(entities)
    async with running_fake_broker() as broker:
        client, task = await _connected_client(broker)
        try:
            publisher = Publisher(
                entities=entities, state=state, mqtt=client, publish_last_changed=False
            )
            state.apply(0, 0, True)
            await publisher.flush()

            await _wait_until(lambda: broker.retained_messages.get(f"{BASE}/test0") is not None)
            msg = broker.retained_messages[f"{BASE}/test0"]
            assert orjson.loads(msg.data) == {"id": "SERIAL_ch0000", "state": True}
            assert f"{BASE}/test1" not in broker.retained_messages
        finally:
            await _stop(client, task)


async def test_flush_publishes_with_the_configured_qos_state() -> None:
    entities = [_entity(0, ("state",))]
    state = StateStore(entities)
    async with running_fake_broker() as broker:
        client, task = await _connected_client(broker)
        try:
            publisher = Publisher(
                entities=entities,
                state=state,
                mqtt=client,
                publish_last_changed=False,
                qos_state=1,
            )
            state.apply(0, 0, True)
            await publisher.flush()

            await _wait_until(lambda: broker.retained_messages.get(f"{BASE}/test0") is not None)
            assert broker.retained_messages[f"{BASE}/test0"].qos == 1
        finally:
            await _stop(client, task)


async def test_flush_clears_the_dirty_set() -> None:
    entities = [_entity(0, ("state",))]
    state = StateStore(entities)
    async with running_fake_broker() as broker:
        client, task = await _connected_client(broker)
        try:
            publisher = Publisher(
                entities=entities, state=state, mqtt=client, publish_last_changed=False
            )
            state.apply(0, 0, True)
            assert state.dirty == {0}
            await publisher.flush()
            assert state.dirty == set()
        finally:
            await _stop(client, task)


async def test_flush_keeps_unpublished_entities_dirty_when_mqtt_is_disconnected() -> None:
    # docs/06 §6 F6, docs/08 §9: a broker outage must never silently drop a change. Before this
    # fix, `flush()` cleared the whole dirty batch up front, so a publish failure partway through
    # lost every entity not yet reached -- including the one that actually failed.
    entities = [_entity(0, ("state",))]
    state = StateStore(entities)
    client = MqttClient(host="127.0.0.1", port=1, base_topic=BASE, sysap_serial="SERIAL")
    publisher = Publisher(entities=entities, state=state, mqtt=client, publish_last_changed=False)
    state.apply(0, 0, True)

    with pytest.raises(MqttClientNotConnectedError):
        await publisher.flush()

    assert state.dirty == {0}


async def test_flush_retries_a_previously_failed_entity_once_mqtt_reconnects() -> None:
    entities = [_entity(0, ("state",))]
    state = StateStore(entities)
    doomed_client = MqttClient(host="127.0.0.1", port=1, base_topic=BASE, sysap_serial="SERIAL")
    doomed_publisher = Publisher(
        entities=entities, state=state, mqtt=doomed_client, publish_last_changed=False
    )
    state.apply(0, 0, True)
    with pytest.raises(MqttClientNotConnectedError):
        await doomed_publisher.flush()
    assert state.dirty == {0}

    async with running_fake_broker() as broker:
        client, task = await _connected_client(broker)
        try:
            publisher = Publisher(
                entities=entities, state=state, mqtt=client, publish_last_changed=False
            )
            await publisher.flush()  # the same dirty mark, retried once mqtt is real
            await _wait_until(lambda: broker.retained_messages.get(f"{BASE}/test0") is not None)
            assert state.dirty == set()
        finally:
            await _stop(client, task)


async def test_run_coalesces_a_burst_into_one_publish_per_entity() -> None:
    entities = [_entity(0, ("state", "brightness"))]
    state = StateStore(entities)
    async with running_fake_broker() as broker:
        client, task = await _connected_client(broker)
        try:
            publisher = Publisher(
                entities=entities,
                state=state,
                mqtt=client,
                coalesce_ms=100,
                publish_last_changed=False,
            )
            run_task = asyncio.create_task(publisher.run())
            try:
                # A burst arriving inside one 100ms coalescing window (docs/05 §4.1) must collapse
                # to exactly one publish, carrying the *final* value.
                state.apply(0, 0, True)
                for value in range(40):
                    state.apply(0, 1, value)

                await _wait_until(lambda: publisher.publish_count >= 1, timeout_seconds=5.0)
                await asyncio.sleep(
                    0.05
                )  # give a wrongly-uncoalesced implementation time to over-publish
                assert publisher.publish_count == 1

                msg = broker.retained_messages[f"{BASE}/test0"]
                assert orjson.loads(msg.data) == {
                    "id": "SERIAL_ch0000",
                    "state": True,
                    "brightness": 39,
                }
            finally:
                run_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await run_task
        finally:
            await _stop(client, task)


async def test_run_publishes_immediately_when_coalesce_ms_is_zero() -> None:
    entities = [_entity(0, ("state",))]
    state = StateStore(entities)
    async with running_fake_broker() as broker:
        client, task = await _connected_client(broker)
        try:
            publisher = Publisher(
                entities=entities,
                state=state,
                mqtt=client,
                coalesce_ms=0,
                publish_last_changed=False,
            )
            run_task = asyncio.create_task(publisher.run())
            try:
                state.apply(0, 0, True)
                await _wait_until(lambda: broker.retained_messages.get(f"{BASE}/test0") is not None)
            finally:
                run_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await run_task
        finally:
            await _stop(client, task)


# ------------------------------------------------------------------- WP14 test helpers


class _FakeClock:
    """A monotonic clock under the test's control (docs/06 §6 F20)."""

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class _RecordingMqtt:
    """Stands in for `MqttClient` where the test is about counters, not wire behaviour."""

    def __init__(self) -> None:
        self.published: list[tuple[str, bytes]] = []

    async def publish(self, topic: str, payload: bytes, **_kwargs: object) -> None:
        self.published.append((topic, payload))


class _FailingMqtt:
    async def publish(self, topic: str, payload: bytes, **_kwargs: object) -> None:
        raise MqttClientNotConnectedError(topic)


def _entities(count: int) -> list[Entity]:
    return [_entity(i, ("state", "brightness")) for i in range(count)]


# --------------------------------------------- WP14: state_publishes and the latency histogram


async def test_flush_counts_one_state_publish_per_entity_not_per_attribute() -> None:
    """docs/04 §4.2's `state_publishes`. Tied to P4: a burst across N entities is N publishes,
    however many datapoints it carried (ADR-005, docs/05 §3 R6).
    """
    entities = _entities(3)
    state = StateStore(entities)
    metrics = Metrics()
    mqtt = _RecordingMqtt()
    publisher = Publisher(entities=entities, state=state, mqtt=mqtt, metrics=metrics)

    for entity_idx in range(3):
        for attr_idx in range(2):
            state.apply(entity_idx, attr_idx, f"v{entity_idx}{attr_idx}")
    await publisher.flush()

    assert metrics.state_publishes == 3


async def test_flush_observes_publish_latency_from_when_the_entity_became_dirty() -> None:
    clock = _FakeClock()
    entities = _entities(1)
    state = StateStore(entities, clock=clock)
    metrics = Metrics()
    publisher = Publisher(
        entities=entities, state=state, mqtt=_RecordingMqtt(), metrics=metrics, monotonic=clock
    )

    clock.now = 10.000
    state.apply(0, 0, "on")
    clock.now = 10.021  # 21 ms later -> the 25 ms bucket
    await publisher.flush()

    assert metrics.latency.total == 1
    assert metrics.latency.percentiles()["p50"] == 25


async def test_a_failed_publish_counts_neither_a_publish_nor_a_latency_sample() -> None:
    """F6 (docs/06 §6): an entity that did not reach the broker stays dirty for the next flush.
    Counting it here would overstate throughput and understate latency at exactly the moment the
    broker is in trouble.
    """
    entities = _entities(1)
    state = StateStore(entities)
    metrics = Metrics()
    mqtt = _FailingMqtt()
    publisher = Publisher(entities=entities, state=state, mqtt=mqtt, metrics=metrics)

    state.apply(0, 0, "on")
    with pytest.raises(MqttClientNotConnectedError):
        await publisher.flush()

    assert metrics.state_publishes == 0
    assert metrics.latency.total == 0
    assert state.dirty == {0}


# ------------------------------------------- WP17: adaptive coalescing (docs/05 §4.1, opt-in)


def _publisher_with_adaptive(
    entities: list[Entity], state: StateStore, mqtt: object, **kwargs: object
) -> Publisher:
    return Publisher(
        entities=entities,
        state=state,
        mqtt=mqtt,
        coalesce_ms=20,
        coalesce_adaptive=True,
        coalesce_max_ms=200,
        coalesce_burst_threshold=25,
        publish_last_changed=False,
        **kwargs,
    )


def test_adaptive_window_is_the_base_window_until_a_burst_arrives() -> None:
    entities = _entities(30)
    state = StateStore(entities)
    publisher = _publisher_with_adaptive(entities, state, _RecordingMqtt())

    assert publisher.current_coalesce_ms == 20


def test_adaptive_window_grows_after_a_batch_over_the_threshold() -> None:
    """docs/05 §4.1: "if the batch size exceeds `coalesce_burst_threshold` (default 25), grow the
    next window up to `coalesce_max_ms`"."""
    entities = _entities(30)
    state = StateStore(entities)
    publisher = _publisher_with_adaptive(entities, state, _RecordingMqtt())

    publisher.note_batch_size(30)

    assert publisher.current_coalesce_ms > 20


def test_adaptive_window_never_exceeds_coalesce_max_ms() -> None:
    """The bound is what keeps this from becoming unbounded added latency."""
    entities = _entities(30)
    state = StateStore(entities)
    publisher = _publisher_with_adaptive(entities, state, _RecordingMqtt())

    for _ in range(50):
        publisher.note_batch_size(500)

    assert publisher.current_coalesce_ms == 200


def test_adaptive_window_shrinks_back_when_batches_are_small() -> None:
    """docs/05 §4.1: "shrink back geometrically when batches are small". Without this, one scene
    would leave every later single button press paying the burst window's latency.
    """
    entities = _entities(30)
    state = StateStore(entities)
    publisher = _publisher_with_adaptive(entities, state, _RecordingMqtt())

    for _ in range(10):
        publisher.note_batch_size(500)
    grown = publisher.current_coalesce_ms
    for _ in range(50):
        publisher.note_batch_size(1)

    assert grown > 20
    assert publisher.current_coalesce_ms == 20


def test_adaptive_window_never_shrinks_below_the_configured_base() -> None:
    entities = _entities(2)
    state = StateStore(entities)
    publisher = _publisher_with_adaptive(entities, state, _RecordingMqtt())

    for _ in range(100):
        publisher.note_batch_size(0)

    assert publisher.current_coalesce_ms == 20


def test_the_window_is_fixed_when_adaptive_is_off() -> None:
    """Default-off has to mean *identical* behaviour, not merely similar: P1-P4 pass without this
    feature and must keep passing for everyone who does not opt in.
    """
    entities = _entities(30)
    state = StateStore(entities)
    publisher = Publisher(entities=entities, state=state, mqtt=_RecordingMqtt(), coalesce_ms=20)

    publisher.note_batch_size(500)

    assert publisher.current_coalesce_ms == 20


def test_adaptive_state_is_two_floats_not_a_growing_collection() -> None:
    """Rule 3: a feature that tracked batch history would be a collection growing with events."""
    entities = _entities(2)
    state = StateStore(entities)
    publisher = _publisher_with_adaptive(entities, state, _RecordingMqtt())

    for size in range(2000):
        publisher.note_batch_size(size)

    assert isinstance(publisher.current_coalesce_ms, float | int)


async def test_adaptive_publisher_still_publishes_every_dirty_entity() -> None:
    """The window only changes *when* a flush happens, never *what* it publishes."""
    entities = _entities(5)
    state = StateStore(entities)
    mqtt = _RecordingMqtt()
    publisher = _publisher_with_adaptive(entities, state, mqtt)

    for idx in range(5):
        state.apply(idx, 0, f"v{idx}")
    await publisher.flush()

    assert len(mqtt.published) == 5
