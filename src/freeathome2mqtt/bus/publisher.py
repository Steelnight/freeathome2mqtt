"""Coalescing publish loop, payload building and retained publish (ADR-005; docs/11 WP5).

`run()` is a long-lived task with no resource of its own to close gracefully (docs/02 §8's "flush
the publisher's dirty set" on shutdown is the caller calling `flush()` directly) -- so its exit
condition (docs/05 §3 rule 2) is plain cancellation, the same as any other `while True` task with
nothing left to clean up.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import orjson

from freeathome2mqtt.model.entity import AttrKind, Entity

if TYPE_CHECKING:
    from freeathome2mqtt.bus.state import StateStore
    from freeathome2mqtt.mqtt.client import MqttClient

logger = logging.getLogger(__name__)


def _utcnow() -> datetime:
    return datetime.now(UTC)


class Publisher:
    """Drains the dirty set, builds payloads, publishes retained state (docs/05 §4.1)."""

    def __init__(
        self,
        *,
        entities: Sequence[Entity],
        state: StateStore,
        mqtt: MqttClient,
        coalesce_ms: int = 20,
        publish_last_changed: bool = True,
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        self._entities = entities
        self._state = state
        self._mqtt = mqtt
        self._coalesce_ms = coalesce_ms
        self._publish_last_changed = publish_last_changed
        self._clock = clock
        self.publish_count = 0

    def build_payload(self, entity_idx: int) -> dict[str, Any]:
        """The entity's complete state (docs/04 §2): `id`, every STATE attribute, `last_changed`."""
        entity = self._entities[entity_idx]
        values = self._state.values[entity_idx]
        payload: dict[str, Any] = {"id": entity.id}
        for name, kind, value in zip(entity.attr_names, entity.attr_kinds, values, strict=True):
            if kind == AttrKind.EVENT:
                continue
            payload[name] = value
        if self._publish_last_changed:
            payload["last_changed"] = self._clock().isoformat(timespec="milliseconds")
        return payload

    async def run(self) -> None:
        """The coalescing loop (docs/05 §4.1): wait for a change, gather the burst, publish it."""
        while True:
            await self._state.wake.wait()
            self._state.wake.clear()
            if self._coalesce_ms:
                await asyncio.sleep(self._coalesce_ms / 1000)
            await self.flush()

    async def flush(self) -> None:
        """Publish everything currently dirty, immediately, with no coalescing wait.

        Each index is only discarded from `dirty` *after* its own publish succeeds (docs/06 §6,
        F6): a broker outage must never silently drop a change. Using `StateStore.take_dirty()`'s
        unconditional clear here would lose every entity from the batch that had not yet been
        reached the moment `mqtt.publish` raised -- this way they simply stay dirty for the next
        flush, after reconnect, to retry.
        """
        for idx in sorted(self._state.dirty):
            entity = self._entities[idx]
            payload = orjson.dumps(self.build_payload(idx))
            await self._mqtt.publish(entity.state_topic, payload, qos=0, retain=True)
            self._state.dirty.discard(idx)
            self.publish_count += 1
