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

        `attr_bit`, when given, is cleared from the unconfirmed bitmask on any change to that
        attribute -- the change may be the echo confirming a just-issued command (docs/02 §4).
        Returns whether the value changed.
        """
        slot = self.values[entity_idx]
        if slot[attr_idx] == value:
            return False
        slot[attr_idx] = value
        self.unconfirmed[entity_idx] &= ~attr_bit
        self.dirty.add(entity_idx)
        self.wake.set()
        return True

    def take_dirty(self) -> set[int]:
        """Atomically swap out the dirty set for a fresh one (docs/05 §4.1)."""
        batch, self.dirty = self.dirty, set()
        return batch
