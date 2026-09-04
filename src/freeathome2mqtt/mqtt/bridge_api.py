"""``bridge/request/*`` handlers -- the only mutation path for bridge-owned state (docs/02 §6;
docs/04 §5; docs/11 WP9).

`BridgeApi.on_message` mirrors `bus/commands.py`'s `CommandDispatcher.on_message`: parse the topic
only, then hand off to a tracked background task, so a slow handler (a REST round trip, a resync)
can never block message dispatch. The handlers themselves are supplied by the caller
(`supervisor.py`) as a fixed dict keyed by the exact command names docs/04 §5 documents -- the
command table IS the closed, reviewed contract (CLAUDE.md rule 8), not open-ended string dispatch.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any

import orjson

from freeathome2mqtt.mqtt import topics

if TYPE_CHECKING:
    import aiomqtt

    from freeathome2mqtt.mqtt.client import MqttClient

logger = logging.getLogger(__name__)

_TRANSACTION_KEY = "transaction"

Handler = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


class BridgeApiError(Exception):
    """Raised by a handler for a structurally-wrong request; converted to a `status: error`
    response (docs/04 §5) rather than propagating -- anything else is a real bug and is left to
    propagate out of the dispatch task uncaught (CLAUDE.md rule 7: never swallow an exception).
    """


class BridgeApi:
    """Routes `bridge/request/<command>` to a fixed table of async handlers (docs/04 §5)."""

    def __init__(
        self, *, base_topic: str, mqtt: MqttClient, handlers: Mapping[str, Handler]
    ) -> None:
        self._base_topic = base_topic
        self._mqtt = mqtt
        self._handlers = handlers
        self._tasks: set[asyncio.Task[None]] = set()

    def on_message(self, message: aiomqtt.Message) -> None:
        """Parse the topic only; everything else runs in a tracked background task."""
        command = self._parse_topic(str(message.topic))
        if command is None:
            return
        task = asyncio.create_task(self._dispatch(command, message.payload))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _parse_topic(self, topic: str) -> str | None:
        prefix = f"{self._base_topic}/bridge/request/"
        if not topic.startswith(prefix):
            return None
        command = topic[len(prefix) :]
        return command or None

    async def _dispatch(self, command: str, payload: bytes) -> None:
        args, transaction = self._decode(payload)
        handler = self._handlers.get(command)
        if handler is None:
            await self._respond(
                command, transaction, status="error", error=f"unknown command: {command!r}"
            )
            return
        try:
            data = await handler(args)
        except BridgeApiError as exc:
            await self._respond(command, transaction, status="error", error=str(exc))
            return
        await self._respond(command, transaction, status="ok", data=data)

    def _decode(self, payload: bytes) -> tuple[dict[str, Any], str | None]:
        if not payload:
            return {}, None
        try:
            decoded = orjson.loads(payload)
        except orjson.JSONDecodeError:
            return {}, None
        if not isinstance(decoded, dict):
            return {}, None
        transaction = decoded.get(_TRANSACTION_KEY)
        return decoded, transaction if isinstance(transaction, str) else None

    async def _respond(
        self,
        command: str,
        transaction: str | None,
        *,
        status: str,
        data: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        body: dict[str, Any] = {"status": status}
        if data is not None:
            body["data"] = data
        if error is not None:
            body["error"] = error
        if transaction is not None:
            body["transaction"] = transaction
        topic = topics.bridge_response_topic(self._base_topic, command)
        await self._mqtt.publish(topic, orjson.dumps(body), qos=0, retain=False)
