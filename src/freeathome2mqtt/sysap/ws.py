"""``WsReader``: connect, heartbeat, parse, hand off only (docs/01 §5; docs/02 §3; docs/11 WP2).

The single cardinal rule (docs/02 §3, rule R1): this reader must never `await` anything that can
take more than a few milliseconds once connected. It parses each frame and hands the whole
per-SysAP-UUID body to `on_frame` -- a plain, synchronous callable -- so downstream code decides
what to do with each key (`datapoints`, `devicesAdded`, ...), never this module.

Reconnection never gives up (docs/06 §3): on any disconnect -- a clean close, a connection error,
or silence past `idle_timeout` -- this loops forever with exponential backoff and full jitter.

The one exception is a `401`/`403` on the WS handshake itself (docs/06 §3's "Auth failure ->
Immediately. Do not retry.", the WS counterpart of `sysap/rest.py`'s `AuthenticationError`/P-20):
`run()` raises `WsAuthenticationError` straight away instead of entering the backoff loop, and
`connect_once()` offers the same check as a one-shot startup probe that never enters the receive
loop at all -- `supervisor.py` uses it to resolve the docs/01 §1.1 `jid` fallback *before* spawning
the long-lived, never-gives-up `run()` task, since a bad username there would otherwise hang
startup forever waiting for a connection that can never succeed.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import ssl
from collections.abc import Callable
from typing import Any, cast

import aiohttp
import orjson
from aiohttp import WSMsgType

from freeathome2mqtt.backoff import backoff_delay
from freeathome2mqtt.sysap.schema import WsFrameBody

logger = logging.getLogger(__name__)


class WsBufferOverflowError(Exception):
    """The buffer (docs/02 §7) overflowed; the caller must restart the load."""


class WsAuthenticationError(Exception):
    """`401`/`403` on the WS handshake -- bad credentials (docs/06 §3; docs/01 §1.1; P-20's WS
    counterpart). Never retried.
    """


class _WsSilence(Exception):
    """Internal: the current connection went quiet or closed; `run()` reconnects."""


_WS_AUTH_FAILURE_STATUSES = (401, 403)

# A single name here (not an inline tuple literal), same reasoning as sysap/rest.py's own
# _RETRYABLE_CONNECTION_ERRORS: sidesteps Python 3.14's grammar allowing `except A, B:` without
# parentheses -- a bare comma reads exactly like the dead Python 2 idiom.
_CONNECTION_FAILURE_ERRORS = (aiohttp.ClientError, OSError)


class WsReader:
    """Connects to the SysAP's WebSocket and hands off parsed frame bodies (docs/01 §5)."""

    def __init__(
        self,
        *,
        url: str,
        username: str,
        password: str,
        session: aiohttp.ClientSession,
        ssl: ssl.SSLContext | bool = True,
        on_frame: Callable[[WsFrameBody], None] | None = None,
        on_connected: Callable[[], None] | None = None,
        heartbeat: float | None = 30.0,
        idle_timeout: float = 90.0,
        backoff_initial: float = 1.0,
        backoff_factor: float = 2.0,
        backoff_cap: float = 60.0,
        buffer_limit: int = 10_000,
    ) -> None:
        self._url = url
        self._auth_header = aiohttp.encode_basic_auth(username, password)
        self._session = session
        self._ssl = ssl
        self._on_frame = on_frame
        self._on_connected = on_connected
        self._heartbeat = heartbeat
        self._idle_timeout = idle_timeout
        self._backoff_initial = backoff_initial
        self._backoff_factor = backoff_factor
        self._backoff_cap = backoff_cap
        self._buffer_limit = buffer_limit

        self._sysap_uuid: str | None = None
        self._buffering = False
        self._buffer: list[WsFrameBody] = []
        self._ws: aiohttp.ClientWebSocketResponse | None = None
        self._stopping = False
        self._stop_event = asyncio.Event()
        self.reconnect_count = 0
        self.backoff_attempt = 0

    @property
    def sysap_uuid(self) -> str | None:
        return self._sysap_uuid

    @property
    def buffered_frame_count(self) -> int:
        return len(self._buffer)

    def start_buffering(self) -> None:
        """Buffer incoming frame bodies instead of dispatching them (docs/02 §7)."""
        self._buffering = True
        self._buffer = []

    def drain_buffer(self) -> list[WsFrameBody]:
        """Stop buffering; return everything collected, in arrival order, and clear it."""
        frames, self._buffer = self._buffer, []
        self._buffering = False
        return frames

    async def run(self) -> None:
        """Connect-and-dispatch forever, reconnecting with backoff+jitter on any disconnect --
        except a `401`/`403` handshake failure (docs/06 §3), which raises immediately instead.
        """
        self.backoff_attempt = 0
        while not self._stopping:
            try:
                await self._connect_and_dispatch()
            except _WsSilence as exc:
                logger.warning("SysAP WebSocket disconnected: %s", exc)
            except aiohttp.WSServerHandshakeError as exc:
                if exc.status in _WS_AUTH_FAILURE_STATUSES:
                    raise WsAuthenticationError(
                        f"WebSocket handshake failed: HTTP {exc.status}"
                    ) from exc
                logger.warning("SysAP WebSocket connection failed: %s", exc)
            except _CONNECTION_FAILURE_ERRORS as exc:
                logger.warning("SysAP WebSocket connection failed: %s", exc)
            if self._stopping:
                return
            self.backoff_attempt += 1
            await self._sleep_backoff(self.backoff_attempt)

    async def connect_once(self) -> None:
        """One-shot handshake probe for startup (docs/01 §1.1's `jid` fallback): open the WS
        connection and immediately close it, never entering the receive loop `run()` uses for
        real traffic. Raises `WsAuthenticationError` on a `401`/`403`; any other failure (the
        SysAP still booting, a network hiccup, ...) is swallowed here, since retrying those is
        `run()`'s job once actually spawned, not this one-shot check's.
        """
        try:
            async with self._session.ws_connect(
                self._url,
                headers={"Authorization": self._auth_header},
                ssl=self._ssl,
                heartbeat=self._heartbeat,
            ):
                return
        except aiohttp.WSServerHandshakeError as exc:
            if exc.status in _WS_AUTH_FAILURE_STATUSES:
                raise WsAuthenticationError(
                    f"WebSocket handshake failed: HTTP {exc.status}"
                ) from exc
        except _CONNECTION_FAILURE_ERRORS:
            pass

    async def stop(self) -> None:
        """Stop `run()` after the current connection attempt, closing it if live."""
        self._stopping = True
        self._stop_event.set()
        if self._ws is not None and not self._ws.closed:
            await self._ws.close()

    async def _sleep_backoff(self, attempt: int) -> None:
        delay = backoff_delay(
            attempt,
            initial=self._backoff_initial,
            factor=self._backoff_factor,
            cap=self._backoff_cap,
        )
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop_event.wait(), timeout=delay)

    async def _connect_and_dispatch(self) -> None:
        async with self._session.ws_connect(
            self._url,
            headers={"Authorization": self._auth_header},
            ssl=self._ssl,
            heartbeat=self._heartbeat,
        ) as ws:
            self._ws = ws
            self.reconnect_count += 1
            self.backoff_attempt = 0  # reset on every successful (re)connect, not on return
            if self._on_connected is not None:
                # Fired synchronously, before any frame from this connection can reach
                # `_receive_loop` (docs/02 §7): the caller's only safe window to arm buffering
                # again after a reconnect, closing the reconnect-variant of P-22.
                self._on_connected()
            try:
                await self._receive_loop(ws)
            finally:
                self._ws = None

    async def _receive_loop(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while True:
            try:
                msg = await asyncio.wait_for(ws.receive(), timeout=self._idle_timeout)
            except TimeoutError as exc:
                await ws.close()
                raise _WsSilence("idle watchdog timeout") from exc
            if msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.CLOSING):
                raise _WsSilence("connection closed")
            if msg.type == WSMsgType.ERROR:
                raise _WsSilence(f"connection error: {ws.exception()}")
            if msg.type != WSMsgType.TEXT:
                continue
            self._dispatch(msg.data)

    def _dispatch(self, raw: str) -> None:
        try:
            frame = orjson.loads(raw)
        except orjson.JSONDecodeError:
            logger.warning("dropping malformed WS frame (%d bytes)", len(raw))
            return
        if not isinstance(frame, dict):
            logger.warning("dropping WS frame with a non-object top level")
            return
        for uuid, body in frame.items():
            self._dispatch_one(uuid, body)

    def _dispatch_one(self, uuid: str, body: Any) -> None:
        if self._sysap_uuid is None:
            self._sysap_uuid = uuid
        elif uuid != self._sysap_uuid:
            logger.warning("ignoring WS frame keyed by unexpected SysAP UUID %s", uuid)
            return
        if not isinstance(body, dict):
            logger.warning("dropping WS frame body that is not an object")
            return
        # A TypedDict is structurally a plain dict at runtime; schema.py's WsFrameBody is a type
        # hint only ("no validation" per its own docstring), so this cast trusts the shape here.
        body = cast(WsFrameBody, body)
        if self._buffering:
            if len(self._buffer) >= self._buffer_limit:
                raise WsBufferOverflowError(
                    f"WS buffer exceeded {self._buffer_limit} frames while buffering"
                )
            self._buffer.append(body)
            return
        if self._on_frame is not None:
            self._on_frame(body)
