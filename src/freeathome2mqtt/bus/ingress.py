"""WS datapoints -> ``StateStore``; the hot path (docs/02 §4, rules R1-R7; docs/11 WP6;
docs/12 WP15).

`Ingress.process_frame` is the `on_frame` callback handed to `sysap.ws.WsReader` -- it must stay
fully synchronous (rule R1): it never `await`s, so nothing it does can ever block the WebSocket
reader on a slow MQTT publish. `kind: event` attributes still need to reach MQTT, so they are
handed off through a tracked fire-and-forget task instead of an inline await (docs/02 §4's own
pseudocode calls `events.emit()` without awaiting it, for the same reason).

WP15 added ``scenesTriggered`` handling, the last of docs/01 §5.1's six frame keys to be
implemented -- until then a triggered scene was silently dropped, even though docs/01 §5.1
specifies both an event and a state application for it, and docs/05 §7's anti-pattern table names
"handling only `datapoints`" as a thing both reference implementations get wrong.

**One deliberate, bounded exemption from rule R2 lives here.** R2 forbids string formatting
between frame receipt and state update, because the datapoint key arrives ready to use as a dict
key. A scene frame does not: its shape is `sceneSerial -> channels -> outputs`, so the composite
key has to be rebuilt. That is one f-string per scene *output*, and scene triggers are an
occasional burst (docs/00 §4 puts them at 50-200 frames as an event, not a sustained rate), so the
cost is bounded by an event that happens when somebody presses a scene button. The alternative --
a second compile-time ingress table keyed by `(serial, channel, odp)` tuples -- was rejected: a
whole additional lookup structure, sized with the installation, for a rare path does not clear
CLAUDE.md §2 rule 3's "justify a new collection in writing" bar. Recorded in docs/05 §3 beside R2
itself rather than taken silently.
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
        """Handle one frame's ``datapoints`` and ``scenesTriggered`` (docs/02 §4 step 2;
        docs/01 §5.1). Awaits nothing (rule R1).
        """
        # Counted before the early returns below: a `devices`/`devicesAdded` frame carries no
        # datapoints but is still a frame the WebSocket delivered, and `ws_frames` disagreeing
        # with the wire would make the docs/05 §9 "is this ingress or egress?" triage wrong.
        self._metrics.ws_frames += 1
        datapoints = body.get("datapoints")
        if datapoints:
            for key, raw in datapoints.items():
                self._process_datapoint(key, raw)
        scenes = body.get("scenesTriggered")
        if scenes:
            self._process_scenes(scenes)

    def _process_scenes(self, scenes: Mapping[str, Any]) -> None:
        """docs/01 §5.1's `scenesTriggered`: emit the edge *and* apply the values it carries.

        Both halves matter. The state application is often the only notification the bridge gets
        for the channels a scene drove, and it is safe to do unconditionally because change
        detection (R4) makes it a no-op if the matching `datapoints` entries arrive as well --
        which docs/01 §5.1 marks as **verify empirically** and this code does not need to know.
        The `bridge/event` is what makes the trigger observable at all when the scene's own
        channel matched no profile, which is the usual case.
        """
        for scene_serial, trigger in scenes.items():
            self._schedule_bridge_event("scene_triggered", {"serial": scene_serial})
            if not isinstance(trigger, dict):
                continue
            channels = trigger.get("channels")
            if not isinstance(channels, dict):
                continue
            for channel_id, channel in channels.items():
                self._process_scene_channel(scene_serial, channel_id, channel)

    def _process_scene_channel(self, serial: str, channel_id: str, channel: Any) -> None:
        """One channel's outputs from a scene frame, defensively: every level of the nested shape
        is external input (CLAUDE.md §2 rule 1) and a malformed frame must be skipped, not raise
        on the hot path.
        """
        if not isinstance(channel, dict):
            return
        outputs = channel.get("outputs")
        if not isinstance(outputs, dict):
            return
        for datapoint_id, output in outputs.items():
            if not isinstance(output, dict):
                continue
            raw = output.get("value")
            if not isinstance(raw, str):
                continue
            # The R2 exemption the module docstring records: once per scene output, not per
            # datapoint on the steady-state path.
            self._process_datapoint(f"{serial}/{channel_id}/{datapoint_id}", raw)

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

    def _schedule_bridge_event(self, event_type: str, data: dict[str, Any]) -> None:
        task = asyncio.create_task(self._emit_bridge_event_and_log(event_type, data))
        self._event_tasks.add(task)
        task.add_done_callback(self._event_tasks.discard)

    async def _emit_bridge_event_and_log(self, event_type: str, data: dict[str, Any]) -> None:
        try:
            await self._events.emit_bridge_event(event_type, data)
        except MqttClientNotConnectedError:
            # Same rule as an entity event: an edge missed while disconnected is gone, and gets
            # logged with context rather than swallowed (CLAUDE.md §2 rule 7).
            logger.warning("dropped %s bridge event: MQTT not connected", event_type)

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
