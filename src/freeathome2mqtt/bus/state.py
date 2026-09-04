"""``StateStore``: values, the dirty set and unconfirmed-command marks (ADR-005; docs/11 WP5).

Sized once from the compiled `entities` list and never resized afterward -- a new device requires
a recompile, not a mutation of this store (docs/03 §2, docs/05 §3 R3: no unbounded growth).
"""

from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

from freeathome2mqtt.model.entity import Entity


class StateStore:
    """Owns `values`, `dirty` and `unconfirmed` -- the only place they live (docs/02 §3)."""

    def __init__(self, entities: Sequence[Entity]) -> None:
        self.values: list[list[Any]] = [[None] * len(e.attr_names) for e in entities]
        self.unconfirmed: list[int] = [0] * len(entities)
        self.dirty: set[int] = set()
        self.wake = asyncio.Event()

    def seed(self, entity_idx: int, attr_idx: int, value: Any) -> None:
        """Set an initial value without marking dirty (startup/resync snapshot apply)."""
        self.values[entity_idx][attr_idx] = value

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
        self.dirty.add(entity_idx)
        self.wake.set()
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
        self.dirty.add(entity_idx)
        self.wake.set()

    def take_dirty(self) -> set[int]:
        """Atomically swap out the dirty set for a fresh one (docs/05 §4.1)."""
        batch, self.dirty = self.dirty, set()
        return batch
