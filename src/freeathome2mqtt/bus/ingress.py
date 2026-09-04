"""WS datapoints -> ``StateStore``; the hot path (docs/02 §4, rules R1-R7; docs/11 WP6).

`Ingress.process_frame` is the `on_frame` callback handed to `sysap.ws.WsReader` -- it must stay
fully synchronous (rule R1): it never `await`s, so nothing it does can ever block the WebSocket
reader on a slow MQTT publish. `kind: event` attributes still need to reach MQTT, so they are
handed off through a tracked fire-and-forget task instead of an inline await (docs/02 §4's own
pseudocode calls `events.emit()` without awaiting it, for the same reason).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from freeathome2mqtt.bus.events import EventPublisher
from freeathome2mqtt.bus.state import StateStore
from freeathome2mqtt.metrics import Metrics
from freeathome2mqtt.model.entity import AttrKind, Binding, Entity
from freeathome2mqtt.mqtt.client import MqttClientNotConnectedError
from freeathome2mqtt.sysap.schema import WsFrameBody

if TYPE_CHECKING:
    from freeathome2mqtt.bus.raw import RawStatePublisher

logger = logging.getLogger(__name__)

_EDGE_EVENT_NAME = "press"
"""docs/04 §2.1's fixed event name -- every `kind: event` profile defined so far (`switch_sensor`,
`trigger`) is a momentary edge; there is no per-attribute discriminator for anything else yet.
"""


class Ingress:
    """Routes each WS datapoint to `StateStore` (state) or `EventPublisher` (event, docs/02 §4)."""

    def __init__(
        self,
        *,
        entities: Sequence[Entity],
        ingress_table: Mapping[str, Binding],
        state: StateStore,
        events: EventPublisher,
        metrics: Metrics,
        raw: RawStatePublisher | None = None,
    ) -> None:
        self._entities = entities
        self._ingress_table = ingress_table
        self._state = state
        self._events = events
        self._metrics = metrics
        self._raw = raw
        # Shared by the event-emit path and (when raw_mode is on) the raw-publish path below --
        # both are the same kind of fire-and-forget background task (docs/02 §4).
        self._event_tasks: set[asyncio.Task[None]] = set()

    def process_frame(self, body: WsFrameBody) -> None:
        """Handle one frame's ``datapoints`` (docs/02 §4 step 2). Awaits nothing (rule R1)."""
        datapoints = body.get("datapoints")
        if not datapoints:
            return
        for key, raw in datapoints.items():
            self._process_datapoint(key, raw)

    def _process_datapoint(self, key: str, raw: str) -> None:
        if self._raw is not None:
            raw_topic = self._raw.topic_for(key)
            if raw_topic is not None:
                self._schedule_raw_publish(self._raw, raw_topic, raw)
        binding = self._ingress_table.get(key)
        if binding is None:
            self._metrics.unmapped_datapoints += 1  # filtered-out channel; expected, cheap
            return
        self._metrics.datapoints_in += 1
        value = binding.decode(raw)
        if value is None and raw != "":
            # Every decoder maps "" to None as a legitimate null (P-04); a non-empty raw that
            # still decoded to None is genuinely malformed input (docs/03 §5).
            self._metrics.codec_errors += 1
        if binding.kind == AttrKind.EVENT:
            self._schedule_event(binding, value)
            return
        self._state.apply(binding.entity_idx, binding.attr_idx, value, attr_bit=binding.attr_bit)

    def _schedule_raw_publish(self, raw_publisher: RawStatePublisher, topic: str, raw: str) -> None:
        task = asyncio.create_task(raw_publisher.publish(topic, raw))
        self._event_tasks.add(task)
        task.add_done_callback(self._event_tasks.discard)

    def _schedule_event(self, binding: Binding, value: Any) -> None:
        entity = self._entities[binding.entity_idx]
        attribute = entity.attr_names[binding.attr_idx]
        self._metrics.events += 1
        task = asyncio.create_task(self._emit_and_log(entity, attribute=attribute, value=value))
        # A bare create_task() result can be garbage-collected mid-flight (a well-known asyncio
        # pitfall); keep a strong reference until it's done, per the stdlib docs' own recipe.
        self._event_tasks.add(task)
        task.add_done_callback(self._event_tasks.discard)

    async def _emit_and_log(self, entity: Entity, *, attribute: str, value: Any) -> None:
        try:
            await self._events.emit(
                entity, attribute=attribute, value=value, event=_EDGE_EVENT_NAME
            )
        except MqttClientNotConnectedError:
            # Events are never retried (ADR-005: the edge is the signal, not the state) -- a drop
            # while disconnected is logged with context, not silently swallowed (CLAUDE.md §2 R7).
            logger.warning("dropped event for %s.%s: MQTT not connected", entity.id, attribute)
