"""Bridge and per-device availability (ADR-008; docs/06 §1, §5; docs/11 WP8).

`BridgeAvailability` publishes ``<base>/bridge/state``: *end-to-end* health, not MQTT connectivity
alone (ADR-008) -- online only when MQTT is connected, the SysAP WebSocket is connected, and the
initial config load has succeeded. Going offline is held for `grace_seconds` before it is actually
published, so a routine reconnect does not flap every entity in Home Assistant; going online is
never delayed, but is also never published automatically -- the caller (`supervisor.py`) decides
the exact moment, since docs/08 §1 requires discovery and state to land on the broker *before*
`bridge/state: online` does.

`device_availability` and `DeviceAvailabilityPublisher` implement docs/06 §5.2's per-device
signal: free@home reports ``unresponsive``/``unresponsiveCounter``/``defect`` on every device in
the configuration snapshot `model.compiler.compile` already walks, so this needs no separate poll
-- just the same snapshot, diffed and published only on change.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import orjson

from freeathome2mqtt.mqtt import topics

if TYPE_CHECKING:
    from freeathome2mqtt.model.entity import Entity
    from freeathome2mqtt.mqtt.client import MqttClient
    from freeathome2mqtt.sysap.schema import Device

logger = logging.getLogger(__name__)


class BridgeAvailability:
    """Publishes ``<base>/bridge/state`` (ADR-008)."""

    def __init__(self, *, mqtt: MqttClient, base_topic: str, grace_seconds: float = 10.0) -> None:
        self._mqtt = mqtt
        self._topic = topics.bridge_state_topic(base_topic)
        self._grace_seconds = grace_seconds
        self._mqtt_connected = False
        self._sysap_connected = False
        self._model_loaded = False
        self._published_online: bool | None = None
        self._grace_task: asyncio.Task[None] | None = None

    @property
    def online(self) -> bool:
        return self._mqtt_connected and self._sysap_connected and self._model_loaded

    @property
    def mqtt_connected(self) -> bool:
        return self._mqtt_connected

    @property
    def sysap_connected(self) -> bool:
        return self._sysap_connected

    @property
    def model_loaded(self) -> bool:
        return self._model_loaded

    def set_mqtt_connected(self, value: bool) -> None:
        self._mqtt_connected = value
        self._on_change()

    def set_sysap_connected(self, value: bool) -> None:
        self._sysap_connected = value
        self._on_change()

    def set_model_loaded(self, value: bool) -> None:
        self._model_loaded = value
        self._on_change()

    async def publish_now(self) -> None:
        """Publish the current `online` value, if it differs from what was last published.

        The caller's explicit hook for the moments docs/08 §1/§4 pin down precisely: right after
        the initial state publish at cold start, and right after publishing resync deltas.
        """
        self._cancel_grace_timer()
        await self._publish(online=self.online)

    async def publish_forced_offline(self) -> None:
        """Shutdown's explicit offline (docs/08 §10) -- the LWT only fires after the broker's
        keepalive timeout, so this is what makes a clean shutdown visible promptly. Bypasses the
        change-only guard deliberately: shutdown always wants this on the wire.
        """
        self._cancel_grace_timer()
        self._published_online = False
        await self._mqtt.publish(
            self._topic, orjson.dumps({"state": "offline"}), qos=1, retain=True
        )

    def _on_change(self) -> None:
        if self.online:
            self._cancel_grace_timer()
        elif self._grace_task is None:
            self._grace_task = asyncio.create_task(self._grace_then_publish_offline())

    def _cancel_grace_timer(self) -> None:
        if self._grace_task is not None:
            self._grace_task.cancel()
            self._grace_task = None

    async def _grace_then_publish_offline(self) -> None:
        # `_on_change` cancels this task the instant `online` turns true, and that cancellation
        # can only land here at the `sleep` -- so reaching the line below always means still not
        # online; there is no live path where the check below could ever be false.
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.sleep(self._grace_seconds)
            self._grace_task = None
            await self._publish(online=False)

    async def _publish(self, *, online: bool) -> None:
        if online == self._published_online:
            return
        self._published_online = online
        state = "online" if online else "offline"
        await self._mqtt.publish(self._topic, orjson.dumps({"state": state}), qos=1, retain=True)


@dataclass(frozen=True, slots=True)
class DeviceAvailability:
    """docs/06 §5.2's payload shape for one entity's per-device availability."""

    state: Literal["online", "offline"]
    reason: Literal["unresponsive", "defect"] | None = None
    unresponsive_counter: int | None = None


def device_availability(device: Device) -> DeviceAvailability:
    """Derive one device's availability straight from its raw config fields (docs/06 §5.2)."""
    counter = device.get("unresponsiveCounter")
    if device.get("defect"):
        return DeviceAvailability(state="offline", reason="defect", unresponsive_counter=counter)
    if device.get("unresponsive"):
        return DeviceAvailability(
            state="offline", reason="unresponsive", unresponsive_counter=counter
        )
    return DeviceAvailability(state="online", unresponsive_counter=counter)


class DeviceAvailabilityPublisher:
    """Publishes per-entity availability (docs/06 §5.2), retained QoS 1, only on change."""

    def __init__(self, *, mqtt: MqttClient) -> None:
        self._mqtt = mqtt
        self._last_published: dict[str, bytes] = {}

    async def publish(self, entities: Sequence[Entity], devices: Mapping[str, Device]) -> None:
        for entity in entities:
            if entity.availability_topic is None:
                continue
            availability = device_availability(devices.get(entity.device_serial, {}))
            payload = orjson.dumps(
                {
                    "state": availability.state,
                    "reason": availability.reason,
                    "unresponsive_counter": availability.unresponsive_counter,
                }
            )
            if self._last_published.get(entity.availability_topic) == payload:
                continue
            self._last_published[entity.availability_topic] = payload
            await self._mqtt.publish(entity.availability_topic, payload, qos=1, retain=True)
