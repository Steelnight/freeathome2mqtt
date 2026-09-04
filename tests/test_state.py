"""Tests for bus/state.py: StateStore -- values, dirty set, unconfirmed marks (ADR-005; docs/03 §2;
docs/11 WP5).
"""

from __future__ import annotations

from freeathome2mqtt.bus.state import StateStore
from freeathome2mqtt.model.entity import AttrKind, Entity


def _entity(idx: int, attr_names: tuple[str, ...]) -> Entity:
    return Entity(
        idx=idx,
        id=f"SERIAL_ch{idx:04d}",
        profile="switch_actuator",
        name="Test",
        area=None,
        device_serial="SERIAL",
        channel_id=f"ch{idx:04d}",
        attr_names=attr_names,
        attr_kinds=tuple(AttrKind.STATE for _ in attr_names),
        state_topic=f"freeathome2mqtt/test{idx}",
        set_topic=f"freeathome2mqtt/test{idx}/set",
        get_topic=f"freeathome2mqtt/test{idx}/get",
        availability_topic=None,
        optimistic=False,
        discovery=(),
    )


def test_values_are_sized_per_entity_from_attr_names() -> None:
    entities = [_entity(0, ("state",)), _entity(1, ("state", "brightness"))]
    store = StateStore(entities)
    assert store.values == [[None], [None, None]]
    assert store.unconfirmed == [0, 0]
    assert store.dirty == set()


def test_seed_sets_a_value_without_marking_dirty() -> None:
    store = StateStore([_entity(0, ("state",))])
    store.seed(0, 0, True)
    assert store.values[0][0] is True
    assert store.dirty == set()
    assert not store.wake.is_set()


def test_apply_stores_and_marks_dirty_on_change() -> None:
    store = StateStore([_entity(0, ("state", "brightness"))])
    changed = store.apply(0, 1, 43)
    assert changed is True
    assert store.values[0][1] == 43
    assert store.dirty == {0}
    assert store.wake.is_set()


def test_apply_is_a_noop_when_the_value_is_unchanged() -> None:
    store = StateStore([_entity(0, ("state",))])
    store.seed(0, 0, True)
    changed = store.apply(0, 0, True)
    assert changed is False
    assert store.dirty == set()
    assert not store.wake.is_set()


def test_apply_deduplicates_multiple_changes_to_the_same_entity() -> None:
    store = StateStore([_entity(0, ("state", "brightness"))])
    store.apply(0, 0, True)
    store.apply(0, 1, 43)
    assert store.dirty == {0}


def test_apply_clears_the_given_unconfirmed_bit_on_change() -> None:
    store = StateStore([_entity(0, ("state", "brightness"))])
    store.unconfirmed[0] = 0b11
    store.apply(0, 1, 43, attr_bit=0b10)
    assert store.unconfirmed[0] == 0b01


def test_apply_without_attr_bit_leaves_unconfirmed_untouched() -> None:
    store = StateStore([_entity(0, ("state",))])
    store.unconfirmed[0] = 0b1
    store.apply(0, 0, True)
    assert store.unconfirmed[0] == 0b1


def test_apply_clears_unconfirmed_bit_even_when_the_value_is_unchanged() -> None:
    # docs/08 §3: an echo that confirms an optimistic guess exactly (no value change) must still
    # clear the mark -- otherwise a correctly-guessed command never gets reconciled off (ADR-012).
    store = StateStore([_entity(0, ("state", "brightness"))])
    store.seed(0, 1, 43)
    store.unconfirmed[0] = 0b10
    changed = store.apply(0, 1, 43, attr_bit=0b10)
    assert changed is False
    assert store.unconfirmed[0] == 0
    assert store.dirty == set()


def test_take_dirty_swaps_and_clears() -> None:
    store = StateStore([_entity(0, ("state",)), _entity(1, ("state",))])
    store.apply(0, 0, True)
    store.apply(1, 0, True)
    batch = store.take_dirty()
    assert batch == {0, 1}
    assert store.dirty == set()


def test_take_dirty_on_an_empty_store_returns_an_empty_set() -> None:
    store = StateStore([_entity(0, ("state",))])
    assert store.take_dirty() == set()


def test_mark_optimistic_stores_sets_unconfirmed_and_marks_dirty() -> None:
    store = StateStore([_entity(0, ("state",))])
    store.mark_optimistic(0, 0, True, attr_bit=0b1)
    assert store.values[0][0] is True
    assert store.unconfirmed[0] == 0b1
    assert store.dirty == {0}
    assert store.wake.is_set()


def test_mark_optimistic_marks_dirty_even_when_the_value_is_unchanged() -> None:
    # A command is a deliberate action; unlike apply()'s R4 gate, it always publishes.
    store = StateStore([_entity(0, ("state",))])
    store.seed(0, 0, True)
    store.mark_optimistic(0, 0, True, attr_bit=0b1)
    assert store.dirty == {0}
    assert store.unconfirmed[0] == 0b1
