"""Tests for log.py: redaction, level control, log_once, the rate-limited MQTT sink (docs/04 §4.5;
docs/07 §2; docs/09 P-44, P-45; docs/11 WP9).
"""

from __future__ import annotations

import io
import logging

import orjson
import pytest

from freeathome2mqtt.log import MqttLogHandler, configure_logging, log_once, set_level

SENTINEL_PASSWORD = "hunter2-sentinel"


def _make_record(msg: str, *args: object, level: int = logging.WARNING) -> logging.LogRecord:
    return logging.LogRecord("test.logger", level, __file__, 1, msg, args, None)


# ----------------------------------------------------------------------------- configure_logging


def test_configure_logging_redacts_a_configured_secret() -> None:
    stream = io.StringIO()
    logger = logging.getLogger("test.log.redact")
    configure_logging(level="info", secrets=[SENTINEL_PASSWORD], logger=logger, stream=stream)
    logger.info("connecting with password=%s", SENTINEL_PASSWORD)
    output = stream.getvalue()
    assert SENTINEL_PASSWORD not in output
    assert "***redacted***" in output


def test_configure_logging_ignores_falsy_secrets() -> None:
    stream = io.StringIO()
    logger = logging.getLogger("test.log.falsy")
    configure_logging(level="info", secrets=[None, ""], logger=logger, stream=stream)
    logger.info("nothing to redact here")
    assert "nothing to redact here" in stream.getvalue()


def test_configure_logging_sets_the_requested_level() -> None:
    logger = logging.getLogger("test.log.level")
    configure_logging(level="debug", logger=logger)
    assert logger.level == logging.DEBUG


def test_configure_logging_rejects_an_unknown_level() -> None:
    with pytest.raises(ValueError, match="unknown log level"):
        configure_logging(level="verbose", logger=logging.getLogger("test.log.bad"))


def test_configure_logging_json_format_is_valid_json_and_redacted() -> None:
    stream = io.StringIO()
    logger = logging.getLogger("test.log.json")
    configure_logging(
        level="info", log_format="json", secrets=[SENTINEL_PASSWORD], logger=logger, stream=stream
    )
    logger.info("token=%s", SENTINEL_PASSWORD)
    line = stream.getvalue().strip().splitlines()[-1]
    body = orjson.loads(line)
    assert body["level"] == "info"
    assert body["logger"] == "test.log.json"
    assert SENTINEL_PASSWORD not in body["message"]
    assert "***redacted***" in body["message"]


def test_configure_logging_appends_a_traceback_for_exception_records() -> None:
    stream = io.StringIO()
    logger = logging.getLogger("test.log.exc")
    configure_logging(level="info", logger=logger, stream=stream)
    try:
        raise ValueError("boom")
    except ValueError:
        logger.exception("something failed")
    output = stream.getvalue()
    assert "something failed" in output
    assert "ValueError: boom" in output


def test_configure_logging_replaces_previously_installed_handlers() -> None:
    logger = logging.getLogger("test.log.replace")
    configure_logging(level="info", logger=logger, stream=io.StringIO())
    configure_logging(level="info", logger=logger, stream=io.StringIO())
    assert len(logger.handlers) == 1


# --------------------------------------------------------------------------------------- set_level


def test_set_level_changes_an_already_configured_logger() -> None:
    logger = logging.getLogger("test.log.set_level")
    configure_logging(level="info", logger=logger)
    set_level("debug", logger=logger)
    assert logger.level == logging.DEBUG


def test_set_level_rejects_an_unknown_level() -> None:
    with pytest.raises(ValueError, match="unknown log level"):
        set_level("verbose", logger=logging.getLogger("test.log.set_level.bad"))


# -------------------------------------------------------------------------------------- log_once


def test_log_once_runs_the_callback_only_the_first_time_for_a_key() -> None:
    seen: set[str] = set()
    calls: list[str] = []
    log_once(seen, "k1", lambda: calls.append("a"))
    log_once(seen, "k1", lambda: calls.append("b"))
    log_once(seen, "k2", lambda: calls.append("c"))
    assert calls == ["a", "c"]


# ------------------------------------------------------------------------------- MqttLogHandler


class _RecordingPublish:
    def __init__(self) -> None:
        self.calls: list[bytes] = []

    async def __call__(self, payload: bytes) -> None:
        self.calls.append(payload)


async def test_mqtt_log_sink_is_rate_limited_with_a_dropped_count_summary() -> None:
    clock_time = [0.0]
    recorder = _RecordingPublish()
    handler = MqttLogHandler(publish=recorder, rate_limit_per_s=3, clock=lambda: clock_time[0])
    logger = logging.getLogger("test.log.mqtt_sink")
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    try:
        for i in range(5):
            logger.warning("message %d", i)
        await handler.flush_pending()
        assert len(recorder.calls) == 3

        clock_time[0] = 1.1
        logger.warning("after window roll")
        await handler.flush_pending()

        bodies = [orjson.loads(c) for c in recorder.calls]
        assert len(bodies) == 5
        assert bodies[3]["message"] == "dropped 2 log messages (rate limit)"
        assert bodies[3]["level"] == "warning"
        assert bodies[4]["message"] == "after window roll"
    finally:
        logger.removeHandler(handler)


async def test_mqtt_log_sink_redacts_secrets() -> None:
    recorder = _RecordingPublish()
    handler = MqttLogHandler(publish=recorder, secrets=[SENTINEL_PASSWORD])
    logger = logging.getLogger("test.log.mqtt_sink.redact")
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    try:
        logger.warning("password=%s", SENTINEL_PASSWORD)
        await handler.flush_pending()
        body = orjson.loads(recorder.calls[0])
        assert SENTINEL_PASSWORD not in body["message"]
        assert "***redacted***" in body["message"]
    finally:
        logger.removeHandler(handler)


async def test_mqtt_log_sink_does_not_reset_the_window_without_a_rollover() -> None:
    clock_time = [0.0]
    recorder = _RecordingPublish()
    handler = MqttLogHandler(publish=recorder, rate_limit_per_s=2, clock=lambda: clock_time[0])
    logger = logging.getLogger("test.log.mqtt_sink.window")
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    try:
        logger.warning("a")
        clock_time[0] = 0.5
        logger.warning("b")
        clock_time[0] = 0.9
        logger.warning("c")  # still inside the same 1s window -> dropped, not published
        await handler.flush_pending()
        assert len(recorder.calls) == 2
    finally:
        logger.removeHandler(handler)


async def test_mqtt_log_sink_window_rollover_without_drops_publishes_no_summary() -> None:
    clock_time = [0.0]
    recorder = _RecordingPublish()
    handler = MqttLogHandler(publish=recorder, rate_limit_per_s=5, clock=lambda: clock_time[0])
    logger = logging.getLogger("test.log.mqtt_sink.no_drop_rollover")
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    try:
        logger.warning("a")
        clock_time[0] = 1.5  # a new window, but nothing was ever dropped in the old one
        logger.warning("b")
        await handler.flush_pending()
        bodies = [orjson.loads(c) for c in recorder.calls]
        assert [b["message"] for b in bodies] == ["a", "b"]
    finally:
        logger.removeHandler(handler)


async def test_mqtt_log_sink_flush_pending_is_a_noop_with_nothing_scheduled() -> None:
    handler = MqttLogHandler(publish=_RecordingPublish())
    await handler.flush_pending()  # must not raise with an empty task set


def test_mqtt_log_sink_emit_without_a_running_loop_is_silently_a_noop() -> None:
    """`emit()` must never raise just because it happens to run before any event loop exists --
    logging can be called from arbitrary synchronous code, including at import time.
    """
    recorder = _RecordingPublish()
    handler = MqttLogHandler(publish=recorder)
    handler.emit(_make_record("no loop yet"))
    assert recorder.calls == []
