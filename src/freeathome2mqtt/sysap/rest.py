"""``RestClient``: session, auth, backoff, adaptive limiter (docs/01 §2; docs/06 §3; docs/11 WP2).

One `aiohttp.ClientSession` is shared across every call (connection reuse). Only `502` and
connection-level errors are retried, with exponential backoff and full jitter; every other error
status maps to a specific typed exception and is never retried (docs/01 §2.3, P-20). A `502` also
halves the effective concurrency ceiling (floor 1), recovered by one per subsequent success
(docs/06 §3) -- the mechanism that turns SysAP overload into a slowdown rather than an outage.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import random
import ssl
from pathlib import Path
from typing import Any, Literal

import aiohttp
import orjson

logger = logging.getLogger(__name__)

SslMode = Literal["verify", "ca_file", "off"]


class SysApError(Exception):
    """Base for every error `RestClient` raises."""


class SysApHttpError(SysApError):
    """An HTTP response the SysAP returned that this client will not retry."""

    def __init__(self, status: int, path: str, body: str = "") -> None:
        super().__init__(f"HTTP {status} from {path}: {body}")
        self.status = status
        self.path = path
        self.body = body


class BadRequestError(SysApHttpError):
    """`400` -- malformed request; almost always a profile bug (docs/01 §2.3)."""


class AuthenticationError(SysApHttpError):
    """`401` -- bad credentials. Never retried (P-20)."""


class ForbiddenError(SysApHttpError):
    """`403` -- Local API not enabled, or this user lacks permission."""


class NotFoundError(SysApHttpError):
    """`404` -- unknown device/channel/datapoint; the topology probably changed."""


class ServerOverloadedError(SysApHttpError):
    """`502`, still failing after every retry -- the SysAP stayed overloaded."""


class SysApServerError(SysApHttpError):
    """Any other `5xx`, still failing after every retry."""


class CommandFailedError(SysApError):
    """A `200` response whose ``result`` was not ``"OK"`` (P-12), or a malformed body."""


async def build_ssl_context(mode: SslMode, ca_file: Path | None = None) -> ssl.SSLContext | bool:
    """Build the SSL context for `mode` (docs/01 §8.1): ``verify``, ``ca_file`` or ``off``.

    Blocking cert I/O goes through `loop.run_in_executor` (ADR-001) rather than the event loop.
    """
    if mode == "off":
        logger.warning("SysAP TLS verification disabled; credentials are exposed to the LAN.")
        return False
    loop = asyncio.get_running_loop()
    if mode == "verify":
        return await loop.run_in_executor(None, ssl.create_default_context)
    if mode == "ca_file":
        if ca_file is None:
            raise ValueError("ssl_mode='ca_file' requires ca_file to be set")
        build = functools.partial(ssl.create_default_context, cafile=str(ca_file))
        return await loop.run_in_executor(None, build)
    raise ValueError(f"unknown ssl_mode: {mode!r}")


class AdaptiveLimiter:
    """Concurrency limiter that halves on overload and recovers by one per success (docs/06 §3).

    Not an `asyncio.Semaphore`: its ceiling must shrink and grow at runtime, which `Semaphore`
    does not support safely.
    """

    def __init__(self, max_inflight: int) -> None:
        self._max = max_inflight
        self._limit = max_inflight
        self._in_flight = 0
        self._condition = asyncio.Condition()

    @property
    def limit(self) -> int:
        return self._limit

    async def __aenter__(self) -> AdaptiveLimiter:
        async with self._condition:
            while self._in_flight >= self._limit:
                await self._condition.wait()
            self._in_flight += 1
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        async with self._condition:
            self._in_flight -= 1
            self._condition.notify_all()

    async def record_overload(self) -> None:
        async with self._condition:
            self._limit = max(1, self._limit // 2)

    async def record_success(self) -> None:
        async with self._condition:
            if self._limit < self._max:
                self._limit += 1
                self._condition.notify_all()


def _backoff_delay(attempt: int, *, initial: float, factor: float, cap: float) -> float:
    """Full jitter (docs/06 §3): ``sleep = random(0, min(cap, initial * factor**(attempt-1)))``."""
    ceiling = min(cap, initial * factor ** (attempt - 1))
    return random.uniform(0, ceiling)  # noqa: S311 -- timing jitter, not a cryptographic use


_HTTP_BAD_REQUEST = 400
_HTTP_OVERLOADED = 502
_HTTP_SERVER_ERROR_MIN = 500
_HTTP_SERVER_ERROR_MAX = 600

# A single name here (not an inline tuple literal) sidesteps Python 3.14's grammar allowing
# `except A, B:` without parentheses -- a bare comma reads exactly like the dead Python 2 idiom.
_RETRYABLE_CONNECTION_ERRORS = (aiohttp.ClientConnectionError, TimeoutError)

_STATUS_ERRORS: dict[int, type[SysApHttpError]] = {
    400: BadRequestError,
    401: AuthenticationError,
    403: ForbiddenError,
    404: NotFoundError,
}


class RestClient:
    """The SysAP's REST surface (docs/01 §2), fronted by one shared `aiohttp.ClientSession`."""

    def __init__(
        self,
        *,
        base_url: str,
        username: str,
        password: str,
        session: aiohttp.ClientSession,
        ssl: ssl.SSLContext | bool = True,
        max_inflight: int = 4,
        max_attempts: int = 5,
        backoff_initial: float = 0.5,
        backoff_factor: float = 2.0,
        backoff_cap: float = 30.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        # aiohttp.BasicAuth/the `auth=` kwarg are deprecated in favour of an explicit header.
        self._auth_header = aiohttp.encode_basic_auth(username, password)
        self._session = session
        self._ssl = ssl
        self._limiter = AdaptiveLimiter(max_inflight)
        self._max_attempts = max_attempts
        self._backoff_initial = backoff_initial
        self._backoff_factor = backoff_factor
        self._backoff_cap = backoff_cap
        self._sysap_uuid: str | None = None

    @property
    def sysap_uuid(self) -> str | None:
        return self._sysap_uuid

    @property
    def concurrency_limit(self) -> int:
        return self._limiter.limit

    async def get_configuration(self) -> dict[str, Any]:
        """``GET /api/rest/configuration`` -- the whole installation (docs/01 §2)."""
        body = await self._request("GET", "/fhapi/v1/api/rest/configuration")
        return self._unwrap(body)

    async def get_datapoint(self, address: str) -> str:
        """``GET /api/rest/datapoint/{sysap}/{address}``; `address` is ``serial.channel.dp``."""
        path = f"/fhapi/v1/api/rest/datapoint/{self._require_sysap_uuid()}/{address}"
        payload = self._unwrap(await self._request("GET", path))
        values = payload.get("values")
        if not isinstance(values, list) or not values:
            raise CommandFailedError(f"malformed datapoint response for {address}: {payload!r}")
        return str(values[0])

    async def put_datapoint(self, address: str, value: str) -> None:
        """``PUT /api/rest/datapoint/{sysap}/{address}``; raises unless ``result`` is OK (P-12)."""
        path = f"/fhapi/v1/api/rest/datapoint/{self._require_sysap_uuid()}/{address}"
        payload = self._unwrap(await self._request("PUT", path, data=value))
        result = payload.get("result")
        if not isinstance(result, str) or result.lower() != "ok":
            raise CommandFailedError(f"datapoint write to {address} failed: result={result!r}")

    def _require_sysap_uuid(self) -> str:
        if self._sysap_uuid is None:
            raise SysApError("sysap_uuid is not resolved yet; call get_configuration() first")
        return self._sysap_uuid

    def _unwrap(self, body: dict[str, Any]) -> dict[str, Any]:
        if len(body) != 1:
            raise SysApError(f"expected a single SysAP-UUID-keyed body, got keys={list(body)!r}")
        ((uuid, payload),) = body.items()
        if self._sysap_uuid is None:
            self._sysap_uuid = uuid
        elif uuid != self._sysap_uuid:
            logger.warning("ignoring response keyed by unexpected SysAP UUID %s", uuid)
        if not isinstance(payload, dict):
            raise SysApError(
                f"expected an object payload for SysAP UUID {uuid}, got {type(payload)}"
            )
        return payload

    async def _sleep_backoff(self, attempt: int) -> None:
        await asyncio.sleep(
            _backoff_delay(
                attempt,
                initial=self._backoff_initial,
                factor=self._backoff_factor,
                cap=self._backoff_cap,
            )
        )

    async def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any]:
        url = f"{self._base_url}{path}"
        attempt = 0
        while True:
            attempt += 1
            try:
                async with (
                    self._limiter,
                    self._session.request(
                        method,
                        url,
                        headers={"Authorization": self._auth_header},
                        ssl=self._ssl,
                        **kwargs,
                    ) as response,
                ):
                    retry = await self._handle_response(response, path, attempt)
                    if retry is None:
                        body = await response.read()
            except _RETRYABLE_CONNECTION_ERRORS:
                if attempt >= self._max_attempts:
                    raise
                await self._sleep_backoff(attempt)
                continue
            if retry is not None:
                await self._sleep_backoff(attempt)
                continue
            await self._limiter.record_success()
            return dict(orjson.loads(body))

    async def _handle_response(
        self, response: aiohttp.ClientResponse, path: str, attempt: int
    ) -> bool | None:
        """Return True if the caller should retry, or None if `response` is ready to be read."""
        status = response.status
        if status == _HTTP_OVERLOADED:
            await self._limiter.record_overload()
            if attempt >= self._max_attempts:
                raise ServerOverloadedError(status, path, await response.text())
            return True
        if _HTTP_SERVER_ERROR_MIN <= status < _HTTP_SERVER_ERROR_MAX:
            if attempt >= self._max_attempts:
                raise SysApServerError(status, path, await response.text())
            return True
        error_type = _STATUS_ERRORS.get(status)
        if error_type is not None:
            raise error_type(status, path, await response.text())
        if status >= _HTTP_BAD_REQUEST:
            raise SysApHttpError(status, path, await response.text())
        return None
