"""Logging setup, secret redaction and the rate-limited MQTT sink (docs/04 §4.5; docs/07 §2.1,
§2.2; docs/09 P-44, P-45; docs/11 WP9).

Redaction happens centrally, in one formatter every handler shares -- never per call site (P-45).
The MQTT sink (`MqttLogHandler`) is opt-in (`advanced.log_to_mqtt`, default `false`) and caps
itself at a fixed rate with a dropped-count summary (P-44): a broker problem must not turn into a
log storm published to the very broker that is having the problem.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Coroutine, Iterable
from typing import Any, Literal

import orjson

_REDACTED = "***redacted***"
_DEFAULT_MQTT_RATE_LIMIT_PER_S = 20
_WINDOW_SECONDS = 1.0

_LOG_LEVELS: dict[str, int] = {
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
}


def _level_or_raise(name: str) -> int:
    level = _LOG_LEVELS.get(name.lower())
    if level is None:
        raise ValueError(f"unknown log level: {name!r}; must be one of {sorted(_LOG_LEVELS)}")
    return level


def _redact(text: str, secrets: Iterable[str | None]) -> str:
    for secret in secrets:
        if secret:
            text = text.replace(secret, _REDACTED)
    return text


def _record_message(record: logging.LogRecord, *, secrets: Iterable[str | None]) -> str:
    """`record.getMessage()`, plus any traceback, with every secret scrubbed -- shared by the
    text/json stream formatter below and `MqttLogHandler`, so both sinks redact identically
    (P-45) instead of each reimplementing it slightly differently.
    """
    message = record.getMessage()
    if record.exc_info:
        message = f"{message}\n{logging.Formatter().formatException(record.exc_info)}"
    return _redact(message, secrets)


class _RedactingFormatter(logging.Formatter):
    """Renders `text` or `json` (docs/07 §2 `advanced.log_format`) with every configured secret
    scrubbed from the message -- redaction happens centrally here, never at the call site (P-45).
    """

    def __init__(
        self, *, log_format: Literal["text", "json"], secrets: Iterable[str | None]
    ) -> None:
        super().__init__()
        self._json = log_format == "json"
        self._secrets = list(secrets)

    def format(self, record: logging.LogRecord) -> str:
        message = _record_message(record, secrets=self._secrets)
        if self._json:
            return orjson.dumps(
                {
                    "ts": self.formatTime(record),
                    "level": record.levelname.lower(),
                    "logger": record.name,
                    "message": message,
                }
            ).decode()
        return f"{self.formatTime(record)} {record.levelname} {record.name}: {message}"


def configure_logging(
    *,
    level: str = "info",
    log_format: Literal["text", "json"] = "text",
    secrets: Iterable[str | None] = (),
    logger: logging.Logger | None = None,
    stream: Any = None,
) -> logging.Logger:
    """One handler, one redacting formatter (docs/07 §2 `advanced.log_level`/`log_format`).

    `logger` defaults to the root logger for production use; tests pass a throwaway named logger
    (and a `stream`) so configuring logging in one test can never leak handlers into another.
    """
    target = logger if logger is not None else logging.getLogger()
    target.setLevel(_level_or_raise(level))
    handler = logging.StreamHandler(stream)
    handler.setFormatter(_RedactingFormatter(log_format=log_format, secrets=secrets))
    target.handlers[:] = [handler]
    return target


def set_level(name: str, *, logger: logging.Logger | None = None) -> None:
    """`bridge/request/log_level` (docs/04 §5): change verbosity at runtime, no restart."""
    target = logger if logger is not None else logging.getLogger()
    target.setLevel(_level_or_raise(name))


def log_once(seen: set[Any], key: Any, log_fn: Callable[[], None]) -> None:
    """Call `log_fn()` only the first time `key` is seen for a given `seen` set.

    `seen` is owned and scoped by the caller (e.g. one compile() call, one Supervisor instance) --
    never a module-level global (CLAUDE.md rule 6) -- so its size is bounded by the caller's own
    key space, not by process lifetime.
    """
    if key in seen:
        return
    seen.add(key)
    log_fn()


class MqttLogHandler(logging.Handler):
    """Publishes `bridge/logging` (docs/04 §4.5), capped at `rate_limit_per_s` messages per
    rolling one-second window with a dropped-count summary (P-44).

    `publish` is a bound callable (typically `lambda payload: mqtt.publish(topic, payload)`)
    rather than a whole `MqttClient` -- this module sits below `mqtt/` in the dependency graph and
    must not import it (docs/02 §2's `sysap/` <-> `mqtt/` separation extends the same way here).
    """

    def __init__(
        self,
        *,
        publish: Callable[[bytes], Coroutine[Any, Any, None]],
        secrets: Iterable[str | None] = (),
        rate_limit_per_s: int = _DEFAULT_MQTT_RATE_LIMIT_PER_S,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        super().__init__()
        self._publish = publish
        self._secrets = list(secrets)
        self._rate_limit = rate_limit_per_s
        self._clock = clock
        self._window_start = clock()
        self._count_in_window = 0
        self._dropped = 0
        self._tasks: set[asyncio.Task[None]] = set()

    def emit(self, record: logging.LogRecord) -> None:
        now = self._clock()
        if now - self._window_start >= _WINDOW_SECONDS:
            self._roll_window(now)
        if self._count_in_window >= self._rate_limit:
            self._dropped += 1
            return
        self._count_in_window += 1
        self._schedule_publish(
            {
                "level": record.levelname.lower(),
                "message": _record_message(record, secrets=self._secrets),
                "logger": record.name,
            }
        )

    def _roll_window(self, now: float) -> None:
        dropped = self._dropped
        self._window_start = now
        self._count_in_window = 0
        self._dropped = 0
        if dropped:
            self._schedule_publish(
                {
                    "level": "warning",
                    "message": f"dropped {dropped} log messages (rate limit)",
                    "logger": __name__,
                }
            )

    def _schedule_publish(self, body: dict[str, Any]) -> None:
        payload = orjson.dumps(body)
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return  # no loop yet (e.g. pre-startup logging) -- nothing sane to schedule onto
        task: asyncio.Task[None] = loop.create_task(self._publish(payload))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def flush_pending(self) -> None:
        """Wait for every publish this handler has scheduled so far to finish."""
        pending = list(self._tasks)
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
