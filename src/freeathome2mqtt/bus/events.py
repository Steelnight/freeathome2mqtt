"""The non-coalescing edge path: buttons, scenes, doorbell (ADR-005; docs/04 §2.1; docs/11 WP5;
docs/12 WP15).

`kind: event` attributes bypass `StateStore` entirely -- no dirty set, no change detection, no
retain. Every edge produces exactly one message, including two identical presses in a row
(P-32): a retained button-press would replay on every restart and fire automations spuriously.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

import orjson

from freeathome2mqtt.model.entity import Entity
from freeathome2mqtt.mqtt.client import MqttClient
from freeathome2mqtt.mqtt.topics import bridge_event_topic, entity_event_topic


def _utcnow() -> datetime:
    return datetime.now(UTC)


class EventPublisher:
    """Publishes one non-retained message per edge (docs/04 §2.1)."""

    def __init__(
        self,
        *,
        mqtt: MqttClient,
        base_topic: str,
        clock: Callable[[], datetime] = _utcnow,
    ) -> None:
        self._mqtt = mqtt
        self._clock = clock
        self._base_topic = base_topic

    async def emit(self, entity: Entity, *, attribute: str, value: Any, event: str) -> None:
        topic = entity_event_topic(entity.state_topic)
        payload = {
            "id": entity.id,
            "event": event,
            "attribute": attribute,
            "value": value,
            "timestamp": self._clock().isoformat(timespec="milliseconds"),
        }
        await self._mqtt.publish(topic, orjson.dumps(payload), qos=0, retain=False)

    async def emit_bridge_event(self, event_type: str, data: dict[str, Any]) -> None:
        """One `bridge/event` (docs/04 §4.4), non-retained like every other edge.

        Lives here rather than in `supervisor.py` so that `bus/ingress.py` can report an edge it
        observed -- a triggered scene -- without reaching past its own collaborator into an
        `MqttClient` (CLAUDE.md §2 rule 9: no new indirection where an existing one fits).
        """
        payload = {"type": event_type, "data": data}
        await self._mqtt.publish(
            bridge_event_topic(self._base_topic), orjson.dumps(payload), qos=0, retain=False
        )
