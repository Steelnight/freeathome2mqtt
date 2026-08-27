"""Raw datapoint passthrough (`advanced.raw_mode`; docs/04 §7; ADR-011; docs/11 WP11).

The pressure valve for unsupported hardware: a user can drive a channel today and open a profile
PR tomorrow. Every output datapoint's raw wire value is published verbatim and retained under
`<base>/raw/<serial>/<channel>/<datapoint>` (`RawStatePublisher`), and a `.../set` topic writes a
raw string straight through `RestClient.put_datapoint` (`RawCommandHandler`) -- no codec, no
validation, no optimism, no reconciliation, unlike the ordinary `/set` path in `bus/commands.py`.
Off by default because it roughly doubles publish volume and exposes the un-abstracted protocol.

`build_raw_map` is pure (docs/02 §2), run once at compile/resync time exactly like
`homeassistant/discovery.py`'s `build_model_discovery` -- never a live walk of the config dict from
`bus/ingress.py`'s hot path (docs/03 §2's "flat lookup tables only" rule). Only outputs feed the
state-topic map: docs/01 §4.1 -- the WebSocket only ever streams changes to outputs, so a raw state
topic for an input would simply never publish anything.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
from collections.abc import Mapping
from typing import TYPE_CHECKING, Literal

from freeathome2mqtt.mqtt.client import MqttClientNotConnectedError
from freeathome2mqtt.mqtt.topics import raw_state_topic
from freeathome2mqtt.sysap.rest import SysApError

if TYPE_CHECKING:
    import aiomqtt

    from freeathome2mqtt.model.compiler import Model
    from freeathome2mqtt.mqtt.client import MqttClient
    from freeathome2mqtt.sysap.rest import RestClient
    from freeathome2mqtt.sysap.schema import Configuration

logger = logging.getLogger(__name__)

RawMode = Literal[False, "unsupported_only", True]

_TOPIC_PARTS = 4  # "<serial>/<channel>/<datapoint>/set", after the "<base>/raw/" prefix


@dataclasses.dataclass(frozen=True, slots=True)
class RawMap:
    """The two flat lookup tables raw mode needs (docs/03 §2's "no dict-of-dicts on the hot
    path" rule, same as `model.compiler.Model` itself).
    """

    state_topics: Mapping[str, str]
    """``SERIAL/chXXXX/odpYYYY`` -> raw state topic, for `bus.ingress.Ingress`'s hot-path lookup."""
    writable_channels: frozenset[tuple[str, str]]
    """``(serial, channel_id)`` pairs eligible for a raw ``/set`` write under the active mode."""


def build_raw_map(config: Configuration, model: Model, *, mode: RawMode, base_topic: str) -> RawMap:
    """Empty when `mode` is `False` -- callers skip constructing `RawStatePublisher`/
    `RawCommandHandler` entirely rather than holding an always-empty map.
    """
    if mode is False:
        return RawMap(state_topics={}, writable_channels=frozenset())
    supported = {(entity.device_serial, entity.channel_id) for entity in model.entities}
    state_topics: dict[str, str] = {}
    writable: set[tuple[str, str]] = set()
    for serial, device in config.get("devices", {}).items():
        for channel_id, channel in device.get("channels", {}).items():
            if mode == "unsupported_only" and (serial, channel_id) in supported:
                continue
            writable.add((serial, channel_id))
            for datapoint_id in channel.get("outputs", {}):
                key = f"{serial}/{channel_id}/{datapoint_id}"
                state_topics[key] = raw_state_topic(base_topic, serial, channel_id, datapoint_id)
    return RawMap(state_topics=state_topics, writable_channels=frozenset(writable))


class RawStatePublisher:
    """Publishes one output datapoint's raw wire value verbatim, retained (docs/04 §7)."""

    def __init__(self, *, mqtt: MqttClient, topics: Mapping[str, str]) -> None:
        self._mqtt = mqtt
        self._topics = topics

    def topic_for(self, key: str) -> str | None:
        return self._topics.get(key)

    async def publish(self, topic: str, raw: str) -> None:
        try:
            await self._mqtt.publish(topic, raw.encode("utf-8"), qos=0, retain=True)
        except MqttClientNotConnectedError:
            # Same best-effort treatment as bus/events.py's edge path: a diagnostic feature that
            # is off by default does not need its own retry/reconcile machinery (CLAUDE.md §2 R7
            # still applies -- logged with context, never silently dropped).
            logger.warning("dropped raw publish for %s: MQTT not connected", topic)


class RawCommandHandler:
    """`<base>/raw/<serial>/<channel>/<datapoint>/set` -> `RestClient.put_datapoint`, no codec, no
    validation -- the direct wire passthrough the ordinary `/set` path in `bus/commands.py`
    deliberately never is (docs/04 §7).
    """

    def __init__(
        self, *, rest: RestClient, base_topic: str, writable_channels: frozenset[tuple[str, str]]
    ) -> None:
        self._rest = rest
        self._base_topic = base_topic
        self._writable_channels = writable_channels
        self._tasks: set[asyncio.Task[None]] = set()

    def on_message(self, message: aiomqtt.Message) -> None:
        parsed = self._parse_topic(str(message.topic))
        if parsed is None:
            return
        serial, channel_id, datapoint_id = parsed
        if (serial, channel_id) not in self._writable_channels:
            return
        value = message.payload.decode("utf-8", errors="replace")
        task = asyncio.create_task(self._write(serial, channel_id, datapoint_id, value))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _parse_topic(self, topic: str) -> tuple[str, str, str] | None:
        prefix = f"{self._base_topic}/raw/"
        if not topic.startswith(prefix):
            return None
        parts = topic[len(prefix) :].split("/")
        if len(parts) != _TOPIC_PARTS or parts[3] != "set":
            return None
        serial, channel_id, datapoint_id, _ = parts
        return serial, channel_id, datapoint_id

    async def _write(self, serial: str, channel_id: str, datapoint_id: str, value: str) -> None:
        rest_path = f"{serial}.{channel_id}.{datapoint_id}"
        try:
            await self._rest.put_datapoint(rest_path, value)
        except SysApError as exc:
            logger.warning("raw command write failed for %s: %s", rest_path, exc)
