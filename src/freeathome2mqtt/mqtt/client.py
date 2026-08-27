"""``aiomqtt`` wrapper: LWT, narrow subscriptions, republish on reconnect (ADR-006; docs/11 WP5).

Reconnection never gives up (docs/06 §3): on any disconnect this loops forever with exponential
backoff and full jitter, mirroring `sysap/ws.py`'s `WsReader` -- the same shape, because both are
"the one link that must never permanently give up" links docs/06 §3 describes.

Connects with MQTT 3.1.1, not the MQTT 5 docs/04 §8 describes as the eventual default: `identifier`
(a fixed client id, needed for P-43) plus `will` (needed for the LWT) together on an MQTT 5 CONNECT
hangs the client indefinitely -- reproduced in isolation against a bare TCP echo of the connect
packet, so it is a defect in the `paho-mqtt` 2.1.0 / `aiomqtt` 2.5.1 pairing this project currently
pins, not a broker-specific quirk. Revisit once that combination is fixed upstream; MQTT 3.1.1 is
universally supported in the meantime, so this is a deferred optimisation, not a missing feature.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
from collections.abc import Awaitable, Callable
from ssl import SSLContext

import aiomqtt

from freeathome2mqtt.mqtt import topics

logger = logging.getLogger(__name__)

OnMessage = Callable[[aiomqtt.Message], None]


def _backoff_delay(attempt: int, *, initial: float, factor: float, cap: float) -> float:
    """Full jitter (docs/06 §3): ``sleep = random(0, min(cap, initial * factor**(attempt-1)))``.

    Duplicated from `sysap/ws.py`'s identical helper rather than shared, per CLAUDE.md's "three
    similar lines beats a premature abstraction" -- both call sites are three lines of pure math.
    """
    ceiling = min(cap, initial * factor ** (attempt - 1))
    return random.uniform(0, ceiling)  # noqa: S311 -- timing jitter, not a cryptographic use


class MqttClientNotConnectedError(Exception):
    """`publish()` was called while no connection is live."""


class MqttClient:
    """Connects to the broker and keeps the connection alive, reconnecting forever."""

    def __init__(
        self,
        *,
        host: str,
        port: int = 1883,
        base_topic: str,
        sysap_serial: str,
        username: str | None = None,
        password: str | None = None,
        tls_context: SSLContext | None = None,
        keepalive: int = 60,
        homeassistant_discovery_topic: str | None = None,
        backoff_initial: float = 1.0,
        backoff_factor: float = 2.0,
        backoff_cap: float = 60.0,
        retained_republish_delay: float = 2.0,
        on_message: OnMessage | None = None,
        on_reconnected: Callable[[], Awaitable[None]] | None = None,
        on_disconnected: Callable[[], None] | None = None,
    ) -> None:
        self._host = host
        self._port = port
        self._base_topic = base_topic
        self._client_id = f"freeathome2mqtt_{sysap_serial}"
        self._username = username
        self._password = password
        self._tls_context = tls_context
        self._keepalive = keepalive
        self._on_message = on_message
        self._on_reconnected = on_reconnected
        self._on_disconnected = on_disconnected
        self._backoff_initial = backoff_initial
        self._backoff_factor = backoff_factor
        self._backoff_cap = backoff_cap
        self._retained_republish_delay = retained_republish_delay

        self._subscriptions = topics.command_subscriptions(base_topic)
        if homeassistant_discovery_topic is not None:
            self._subscriptions = (
                *self._subscriptions,
                topics.ha_birth_topic(homeassistant_discovery_topic),
            )

        self._will = aiomqtt.Will(
            topic=topics.bridge_state_topic(base_topic),
            payload=b'{"state":"offline"}',
            qos=1,
            retain=True,
        )

        self._last_published: dict[str, bytes] = {}
        self._retained_topics: set[str] = set()

        self._client: aiomqtt.Client | None = None
        self._connection_task: asyncio.Task[None] | None = None
        self._republish_task: asyncio.Task[None] | None = None

        self._stopping = False
        self._stop_event = asyncio.Event()
        self.reconnect_count = 0
        self.backoff_attempt = 0

    @property
    def client_id(self) -> str:
        return self._client_id

    @property
    def subscriptions(self) -> tuple[str, ...]:
        return self._subscriptions

    def last_published(self, topic: str) -> bytes | None:
        return self._last_published.get(topic)

    async def run(self) -> None:
        """Connect-subscribe-dispatch forever, reconnecting with backoff+jitter on any failure."""
        self.backoff_attempt = 0
        while not self._stopping:
            self._connection_task = asyncio.create_task(self._connect_and_serve())
            try:
                await self._connection_task
            except asyncio.CancelledError:
                if not self._stopping:
                    raise
            except aiomqtt.MqttError as exc:
                logger.warning("MQTT connection failed: %s", exc)
            finally:
                self._connection_task = None
            if self._stopping:
                return
            self.backoff_attempt += 1
            await self._sleep_backoff(self.backoff_attempt)

    async def stop(self) -> None:
        """Stop `run()` by cancelling the live connection, once at most one is outstanding."""
        self._stopping = True
        self._stop_event.set()
        if self._connection_task is not None:
            self._connection_task.cancel()

    async def publish(
        self, topic: str, payload: bytes, *, qos: int = 0, retain: bool = False
    ) -> None:
        topics.assert_publishable(topic)
        if self._client is None:
            raise MqttClientNotConnectedError(topic)
        await self._client.publish(topic, payload, qos=qos, retain=retain)
        self._last_published[topic] = payload
        if retain:
            self._retained_topics.add(topic)
        else:
            self._retained_topics.discard(topic)

    async def _sleep_backoff(self, attempt: int) -> None:
        delay = _backoff_delay(
            attempt,
            initial=self._backoff_initial,
            factor=self._backoff_factor,
            cap=self._backoff_cap,
        )
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay)

    async def _connect_and_serve(self) -> None:
        async with aiomqtt.Client(
            self._host,
            port=self._port,
            identifier=self._client_id,
            username=self._username,
            password=self._password,
            protocol=aiomqtt.ProtocolVersion.V311,
            will=self._will,
            keepalive=self._keepalive,
            tls_context=self._tls_context,
        ) as client:
            self._client = client
            self.reconnect_count += 1
            self.backoff_attempt = 0  # reset on every successful (re)connect, not on return
            await self._on_connected(client)
            try:
                async for message in client.messages:
                    if self._on_message is not None:
                        self._on_message(message)
            finally:
                self._cancel_republish_task()
                self._client = None
                if self._on_disconnected is not None:
                    self._on_disconnected()

    async def _on_connected(self, client: aiomqtt.Client) -> None:
        for topic in self._subscriptions:
            await client.subscribe(topic)
        if self._on_reconnected is not None:
            # docs/08 §9: republishing the accumulated dirty batch is an explicit step of the
            # reconnect flow, not something a generic wake-loop can be relied on to do by itself
            # -- `wake` may already be clear from a publish attempt that failed while disconnected.
            await self._on_reconnected()
        self._cancel_republish_task()
        self._republish_task = asyncio.create_task(self._delayed_republish_retained())

    def _cancel_republish_task(self) -> None:
        if self._republish_task is not None:
            self._republish_task.cancel()
            self._republish_task = None

    async def _delayed_republish_retained(self) -> None:
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.sleep(self._retained_republish_delay)
            await self._republish_retained()

    async def _republish_retained(self) -> None:
        """P-42: republish everything we last retain-published, for a broker that forgot it."""
        if self._client is None:
            return
        for topic in list(self._retained_topics):
            payload = self._last_published.get(topic)
            if payload is None:
                continue
            await self._client.publish(topic, payload, qos=0, retain=True)
