"""Tests for metrics_server.py: the optional Prometheus text-exposition endpoint
(`advanced.metrics`; docs/00 §5; docs/11 WP12).
"""

from __future__ import annotations

import asyncio

import aiohttp
import pytest

from fakes.fake_broker import free_port
from freeathome2mqtt.metrics import Metrics
from freeathome2mqtt.metrics_server import MetricsServer, render_prometheus_text


async def _wait_until(predicate, *, timeout_seconds: float = 5.0, interval: float = 0.01) -> None:
    async with asyncio.timeout(timeout_seconds):
        while not predicate():  # noqa: ASYNC110 -- generic poll, no single event to await
            await asyncio.sleep(interval)


# ------------------------------------------------------------------------ render_prometheus_text


def test_render_prometheus_text_includes_every_counter() -> None:
    metrics = Metrics(
        datapoints_in=42,
        unmapped_datapoints=3,
        events=7,
        codec_errors=1,
        task_restarts=0,
        config_reloads=2,
    )
    text = render_prometheus_text(metrics).decode("utf-8")
    assert "freeathome2mqtt_datapoints_in 42" in text
    assert "freeathome2mqtt_unmapped_datapoints 3" in text
    assert "freeathome2mqtt_events 7" in text
    assert "freeathome2mqtt_codec_errors 1" in text
    assert "freeathome2mqtt_task_restarts 0" in text
    assert "freeathome2mqtt_config_reloads 2" in text


def test_render_prometheus_text_has_help_and_type_lines() -> None:
    text = render_prometheus_text(Metrics()).decode("utf-8")
    assert "# HELP freeathome2mqtt_events" in text
    assert "# TYPE freeathome2mqtt_events counter" in text


def test_render_prometheus_text_is_valid_utf8_plain_text() -> None:
    body = render_prometheus_text(Metrics())
    body.decode("utf-8")  # must not raise
    assert body.endswith(b"\n")


# --------------------------------------------------------------------------------- MetricsServer


async def test_metrics_server_serves_metrics_on_its_configured_port() -> None:
    metrics = Metrics(events=5)
    server = MetricsServer(metrics=metrics, host="127.0.0.1", port=free_port())
    task = asyncio.create_task(server.run())
    try:
        await _wait_until(lambda: server.is_listening)
        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{server.port}/metrics") as response,
        ):
            assert response.status == 200
            assert response.headers["Content-Type"].startswith("text/plain")
            body = await response.text()
            assert "freeathome2mqtt_events 5" in body
    finally:
        await server.stop()
        await asyncio.wait_for(task, timeout=5.0)


async def test_metrics_server_reflects_live_counter_updates() -> None:
    metrics = Metrics()
    server = MetricsServer(metrics=metrics, host="127.0.0.1", port=free_port())
    task = asyncio.create_task(server.run())
    try:
        await _wait_until(lambda: server.is_listening)
        metrics.datapoints_in = 99
        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{server.port}/metrics") as response,
        ):
            body = await response.text()
            assert "freeathome2mqtt_datapoints_in 99" in body
    finally:
        await server.stop()
        await asyncio.wait_for(task, timeout=5.0)


async def test_metrics_server_stop_before_start_does_not_raise() -> None:
    server = MetricsServer(metrics=Metrics(), host="127.0.0.1", port=free_port())
    await server.stop()  # must not raise -- run() was never awaited


async def test_metrics_server_run_returns_once_stopped() -> None:
    server = MetricsServer(metrics=Metrics(), host="127.0.0.1", port=free_port())
    task = asyncio.create_task(server.run())
    await _wait_until(lambda: server.is_listening)
    await server.stop()
    await asyncio.wait_for(task, timeout=5.0)
    assert task.done()
    assert not server.is_listening


@pytest.mark.parametrize("path", ["/", "/nope"])
async def test_metrics_server_only_serves_metrics_path(path: str) -> None:
    server = MetricsServer(metrics=Metrics(), host="127.0.0.1", port=free_port())
    task = asyncio.create_task(server.run())
    try:
        await _wait_until(lambda: server.is_listening)
        async with (
            aiohttp.ClientSession() as session,
            session.get(f"http://127.0.0.1:{server.port}{path}") as response,
        ):
            assert response.status == 404
    finally:
        await server.stop()
        await asyncio.wait_for(task, timeout=5.0)
