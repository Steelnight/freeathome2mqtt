"""A fake free@home SysAP: a real `aiohttp` server plus a scripting interface (docs/10 §3.1).

Used instead of mocking `aiohttp` so tests exercise real sockets, real WebSocket framing and real
concurrency -- the same reasoning docs/10 §3.4 applies to the broker.
"""

from __future__ import annotations

import asyncio
import contextlib
import copy
from collections import Counter
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import orjson
from aiohttp import WSMsgType, web
from aiohttp.test_utils import TestClient, TestServer

DEFAULT_SYSAP_UUID = "00000000-0000-0000-0000-000000000000"
DEFAULT_SETTINGS_VERSION = "2.6.4"
DEFAULT_SERIAL_NUMBER = "ABB7005500E1"
DEFAULT_SYSAP_NAME = "Fake House"


@dataclass
class _ForcedError:
    status: int
    remaining: int | None  # None = forever


def _empty_configuration() -> dict[str, Any]:
    return {
        "sysapName": "Fake SysAP",
        "sysap": {},
        "users": {},
        "floorplan": {"floors": {}},
        "devices": {},
    }


class FakeSysAp:
    """Serves `/fhapi/v1/api/rest/*` and `/fhapi/v1/api/ws`, scriptable for tests."""

    def __init__(
        self,
        *,
        sysap_uuid: str = DEFAULT_SYSAP_UUID,
        configuration: dict[str, Any] | None = None,
    ) -> None:
        self.sysap_uuid = sysap_uuid
        # Deep-copied so the fake never mutates a fixture dict the caller still holds a reference.
        self._configuration = (
            copy.deepcopy(configuration) if configuration is not None else _empty_configuration()
        )
        self._request_counts: Counter[str] = Counter()
        self._forced_errors: dict[str, _ForcedError] = {}
        self._raw_responses: dict[str, dict[str, Any]] = {}
        self._latency_seconds = 0.0
        self._max_concurrent: int | None = None
        self._current_concurrent = 0
        self._peak_concurrent = 0
        self._write_result = "OK"
        self._ws_clients: list[web.WebSocketResponse] = []
        self._ws_hung = False
        self._settings_version = DEFAULT_SETTINGS_VERSION
        self._serial_number = DEFAULT_SERIAL_NUMBER
        self._settings_users: list[dict[str, str]] = [
            {"name": "installer", "jid": "abc123@busch-jaeger.de"}
        ]

        self.app = web.Application()
        self.app.router.add_get("/settings.json", self._handle_settings)
        self.app.router.add_get("/fhapi/v1/api/rest/configuration", self._handle_configuration)
        self.app.router.add_get(
            "/fhapi/v1/api/rest/datapoint/{sysap}/{address}", self._handle_get_datapoint
        )
        self.app.router.add_put(
            "/fhapi/v1/api/rest/datapoint/{sysap}/{address}", self._handle_put_datapoint
        )
        self.app.router.add_get("/fhapi/v1/api/ws", self._handle_ws)

    # ----------------------------------------------------------------- scripting: configuration

    def set_configuration(self, configuration: dict[str, Any]) -> None:
        """Replace the body served by ``GET /api/rest/configuration``."""
        self._configuration = copy.deepcopy(configuration)

    def set_datapoint(self, serial: str, channel: str, datapoint: str, value: str) -> None:
        """Mutate a datapoint's value in the stored configuration, without pushing a WS frame."""
        channels = self._configuration["devices"][serial]["channels"]
        channel_body = channels[channel]
        for direction in ("inputs", "outputs"):
            datapoints = channel_body.get(direction)
            if isinstance(datapoints, dict) and datapoint in datapoints:
                datapoints[datapoint]["value"] = value
                return
        raise KeyError(f"{serial}/{channel}/{datapoint} is not in the stored configuration")

    # -------------------------------------------------------------- scripting: /settings.json

    def set_settings_version(self, version: str) -> None:
        """The firmware version `GET /settings.json` (unauthenticated, docs/01 §1.1) reports."""
        self._settings_version = version

    def set_serial_number(self, serial: str) -> None:
        self._serial_number = serial

    # ------------------------------------------------------------------- scripting: HTTP quirks

    def set_error(self, path: str, status: int, *, times: int | None = None) -> None:
        """Force requests to `path` to return `status`.

        Forever by default; pass `times` to auto-clear after that many requests (useful for
        simulating a transient overload that then recovers).
        """
        self._forced_errors[path] = _ForcedError(status=status, remaining=times)

    def clear_error(self, path: str) -> None:
        self._forced_errors.pop(path, None)

    def set_raw_response(self, path: str, body: dict[str, Any]) -> None:
        """Return `body` verbatim for `path`, bypassing normal handling -- for testing a
        client's handling of a malformed/unexpected SysAP response shape.
        """
        self._raw_responses[path] = body

    def clear_raw_response(self, path: str) -> None:
        self._raw_responses.pop(path, None)

    def set_latency(self, ms: float) -> None:
        """Add artificial latency to every request."""
        self._latency_seconds = ms / 1000

    def set_max_concurrent(self, n: int | None) -> None:
        """Return 502 for any request arriving while more than `n` are already in flight."""
        self._max_concurrent = n

    def set_write_result(self, result: str) -> None:
        """The `result` field a datapoint PUT reports (docs/01 §2.2); default `"OK"`."""
        self._write_result = result

    def request_count(self, path: str) -> int:
        return self._request_counts[path]

    def peak_concurrency(self) -> int:
        return self._peak_concurrent

    # --------------------------------------------------------------------- scripting: WebSocket

    async def push_ws_frame(self, body: dict[str, Any], *, sysap_uuid: str | None = None) -> None:
        """Push an arbitrary WS frame (any/all of the five keys, docs/01 §5.1) to every client."""
        frame = {sysap_uuid if sysap_uuid is not None else self.sysap_uuid: body}
        await self.push_raw_ws_text(orjson.dumps(frame).decode())

    async def push_raw_ws_text(self, text: str) -> None:
        """Push raw text verbatim -- for testing a client's handling of malformed frames."""
        for ws in list(self._ws_clients):
            if not ws.closed:
                await ws.send_str(text)

    async def push_datapoint_change(
        self, serial: str, channel: str, datapoint: str, value: str
    ) -> None:
        """Convenience: update state and push the corresponding `datapoints` WS frame."""
        self.set_datapoint(serial, channel, datapoint, value)
        await self.push_ws_frame({"datapoints": {f"{serial}/{channel}/{datapoint}": value}})

    async def drop_websocket(self) -> None:
        """Close every live WS connection cleanly (a clean disconnect, not a hang)."""
        for ws in list(self._ws_clients):
            await ws.close()

    def hang_websocket(self) -> None:
        """Stop responding on the WebSocket -- no close, no pong, total silence (P-21)."""
        self._ws_hung = True

    def restore_websocket(self) -> None:
        """Resume responding to pings; new connections behave normally either way."""
        self._ws_hung = False

    # ------------------------------------------------------------------------------- handlers

    @contextlib.asynccontextmanager
    async def _track(self, path: str) -> AsyncIterator[None]:
        self._request_counts[path] += 1
        self._current_concurrent += 1
        self._peak_concurrent = max(self._peak_concurrent, self._current_concurrent)
        try:
            if self._latency_seconds:
                await asyncio.sleep(self._latency_seconds)
            yield
        finally:
            self._current_concurrent -= 1

    def _forced_status(self, path: str) -> int | None:
        forced = self._forced_errors.get(path)
        if forced is not None:
            if forced.remaining is None:
                return forced.status
            if forced.remaining > 0:
                forced.remaining -= 1
                if forced.remaining == 0:
                    del self._forced_errors[path]
                return forced.status
        if self._max_concurrent is not None and self._current_concurrent > self._max_concurrent:
            return 502
        return None

    def _wrap(self, body: dict[str, Any]) -> dict[str, Any]:
        return {self.sysap_uuid: body}

    async def _handle_settings(self, request: web.Request) -> web.Response:
        """Unauthenticated, unversioned by `_track`/`_forced_status` -- this endpoint predates
        `/fhapi/v1` entirely (docs/01 §1.1) and none of the request-count/error scripting is
        meaningful for it.
        """
        return web.json_response(
            {
                "flags": {
                    "version": self._settings_version,
                    "serialNumber": self._serial_number,
                    "name": DEFAULT_SYSAP_NAME,
                },
                "users": self._settings_users,
            }
        )

    async def _handle_configuration(self, request: web.Request) -> web.Response:
        async with self._track(request.path):
            if request.path in self._raw_responses:
                return web.json_response(self._raw_responses[request.path])
            status = self._forced_status(request.path)
            if status is not None:
                return web.Response(status=status)
            return web.json_response(self._wrap(self._configuration))

    def _split_datapoint_address(self, address: str) -> tuple[str, str, str]:
        serial, channel, datapoint = address.split(".")
        return serial, channel, datapoint

    async def _handle_get_datapoint(self, request: web.Request) -> web.Response:
        async with self._track(request.path):
            if request.path in self._raw_responses:
                return web.json_response(self._raw_responses[request.path])
            status = self._forced_status(request.path)
            if status is not None:
                return web.Response(status=status)
            serial, channel, datapoint = self._split_datapoint_address(
                request.match_info["address"]
            )
            channel_body = self._configuration["devices"][serial]["channels"][channel]
            for direction in ("inputs", "outputs"):
                datapoints = channel_body.get(direction)
                if isinstance(datapoints, dict) and datapoint in datapoints:
                    value = datapoints[datapoint]["value"]
                    return web.json_response(self._wrap({"values": [value]}))
            return web.Response(status=404)

    async def _handle_put_datapoint(self, request: web.Request) -> web.Response:
        async with self._track(request.path):
            if request.path in self._raw_responses:
                return web.json_response(self._raw_responses[request.path])
            status = self._forced_status(request.path)
            if status is not None:
                return web.Response(status=status)
            serial, channel, datapoint = self._split_datapoint_address(
                request.match_info["address"]
            )
            value = (await request.text()).strip()
            self.set_datapoint(serial, channel, datapoint, value)
            return web.json_response(self._wrap({"result": self._write_result}))

    async def _handle_ws(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(autoping=False)
        await ws.prepare(request)
        self._ws_clients.append(ws)
        try:
            async for msg in ws:
                if msg.type == WSMsgType.PING and not self._ws_hung:
                    await ws.pong(msg.data)
                elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.ERROR):
                    break
        finally:
            if ws in self._ws_clients:
                self._ws_clients.remove(ws)
        return ws


@contextlib.asynccontextmanager
async def running_fake_sysap(
    fake: FakeSysAp | None = None,
) -> AsyncIterator[tuple[FakeSysAp, TestClient[web.Request, web.Application]]]:
    """Start `fake` (or a fresh one) behind a real `TestServer`; yields `(fake, client)`."""
    fake = fake if fake is not None else FakeSysAp()
    async with TestClient(TestServer(fake.app)) as client:
        yield fake, client
