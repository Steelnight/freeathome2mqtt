"""``StateStore``: values, the dirty set and unconfirmed-command marks (ADR-005; docs/11 WP5;
docs/12 WP14).

Sized once from the compiled `entities` list and never resized afterward -- a new device requires
a recompile, not a mutation of this store (docs/03 §2, docs/05 §3 R3: no unbounded growth). That
applies to `first_dirty_at` too: it is a `list[float]` indexed by entity, allocated with the rest,
not a dict that grows as entities are touched.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Sequence
from typing import Any

from freeathome2mqtt.model.entity import Entity


class StateStore:
    """Owns `values`, `dirty` and `unconfirmed` -- the only place they live (docs/02 §3)."""

    def __init__(
        self, entities: Sequence[Entity], *, clock: Callable[[], float] = time.monotonic
    ) -> None:
        self.values: list[list[Any]] = [[None] * len(e.attr_names) for e in entities]
        self.unconfirmed: list[int] = [0] * len(entities)
        self.dirty: set[int] = set()
        self.wake = asyncio.Event()
        self._clock = clock
        # When each entity *became* dirty, for the publish-latency histogram (docs/04 §4.2's
        # `latency_ms`, docs/12 WP14). A parallel list, not a side dict: docs/05 §6 names an
        # unpruned per-entity side dict as one of two known unbounded-growth traps, and
        # prescribes exactly this shape ("store it in the entity's slot, not a side dict").
        self.first_dirty_at: list[float] = [0.0] * len(entities)
        # When each entity's value last actually *changed*, for docs/06 §5.3's staleness counter
        # (`availability.stale_after`). Distinct from `first_dirty_at` above, which is about the
        # publish wait: this one survives the publish and is what makes "this sensor has not
        # moved in a week" answerable. Same parallel-list shape, same docs/05 §6 reasoning.
        self.last_changed_at: list[float] = [0.0] * len(entities)

    def seed(self, entity_idx: int, attr_idx: int, value: Any) -> None:
        """Set an initial value without marking dirty (startup/resync snapshot apply).

        This *does* stamp `last_changed_at`: leaving a freshly seeded installation at 0.0 would
        report every entity as stale the moment the bridge started, which is the opposite of what
        docs/06 §5.3's counter is for.
        """
        self.values[entity_idx][attr_idx] = value
        self.last_changed_at[entity_idx] = self._clock()

    def apply(self, entity_idx: int, attr_idx: int, value: Any, *, attr_bit: int = 0) -> bool:
        """Change detection (docs/05 §3 R4): store and mark dirty only if `value` actually changed.

        `attr_bit`, when given, is cleared from the unconfirmed bitmask unconditionally -- even
        when `value` turns out to equal what was already stored. The clear must not be gated by
        change detection: an echo that confirms an optimistic guess *exactly* (docs/08 §3: "value
        already 55 -> clear unconfirmed, no publish") is still the confirmation `bus/reconcile.py`
        is waiting for, and skipping it here would leave that command reconciled off only by
        (harmless but pointless) timeout instead of by its own echo.
        Returns whether the value changed.
        """
        self.unconfirmed[entity_idx] &= ~attr_bit
        slot = self.values[entity_idx]
        if slot[attr_idx] == value:
            return False
        slot[attr_idx] = value
        self.last_changed_at[entity_idx] = self._clock()
        self._mark_dirty(entity_idx)
        return True

    def mark_optimistic(self, entity_idx: int, attr_idx: int, value: Any, *, attr_bit: int) -> None:
        """Optimistic command write (ADR-012): store the guessed value, mark it unconfirmed, and
        mark dirty unconditionally. A command is a deliberate user action, not a hot-path repeat,
        so unlike `apply()` this is never gated by R4 change detection -- docs/02 §5's sequence
        diagram always publishes the optimistic state, even if it happens to match what is already
        held.
        """
        self.values[entity_idx][attr_idx] = value
        self.unconfirmed[entity_idx] |= attr_bit
        self.last_changed_at[entity_idx] = self._clock()
        self._mark_dirty(entity_idx)

    def _mark_dirty(self, entity_idx: int) -> None:
        """Mark dirty and, on the clean -> dirty *edge* only, stamp when the wait began.

        The `not in` test is what keeps this off the per-datapoint cost: a 500-datapoint burst
        across 40 entities takes 40 clock readings, not 500, and a second change to an
        already-dirty entity keeps the earlier (correct) start time rather than resetting it --
        otherwise measured latency would shrink the busier the bridge got.
        """
        if entity_idx not in self.dirty:
            self.first_dirty_at[entity_idx] = self._clock()
            self.dirty.add(entity_idx)
        self.wake.set()

    def stale_entity_count(self, stale_after_s: float) -> int:
        """How many entities have not changed within `stale_after_s` (docs/06 §5.3).

        Informational only, and deliberately never wired to availability: plenty of free@home
        channels legitimately do not change for months (a garage door sensor, a rarely-used
        switch), so marking them unavailable would be wrong. The counter exists so a user can
        spot a genuinely dead sensor; the judgement stays theirs.

        An O(entities) scan, which is fine because the only caller is `bridge/info` -- republished
        at most every 30 s, never on the hot path.
        """
        cutoff = self._clock() - stale_after_s
        return sum(1 for changed_at in self.last_changed_at if changed_at < cutoff)

    def take_dirty(self) -> set[int]:
        """Atomically swap out the dirty set for a fresh one (docs/05 §4.1)."""
        batch, self.dirty = self.dirty, set()
        return batch
