"""The optional Prometheus text-exposition endpoint (`advanced.metrics`; docs/00 §5; docs/11
WP12).

`render_prometheus_text` is a pure formatter -- a hand-rolled one, not the `prometheus_client`
library, matching ADR "minimal dependency surface" (docs/00 §5): `metrics.Metrics` is already a
small, fixed set of counters, so a dependency just to print `# HELP`/`# TYPE`/value triples is not
worth adding. `MetricsServer` owns the HTTP endpoint's lifecycle the same way `mqtt/client.py`'s
`MqttClient` owns its connection: `run()` is the long-lived coroutine `supervisor.py`'s
`_spawn_supervised` expects, `stop()` unblocks it for a clean shutdown.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

from aiohttp import web

if TYPE_CHECKING:
    from freeathome2mqtt.metrics import Metrics

_COUNTER_FIELDS: tuple[str, ...] = (
    "datapoints_in",
    "unmapped_datapoints",
    "events",
    "codec_errors",
    "task_restarts",
    "config_reloads",
    # docs/12 WP14: the four docs/04 §4.2 `stats` counters that had no source before it.
    "ws_frames",
    "state_publishes",
    "commands",
    "command_errors",
)

_LATENCY_METRIC = "freeathome2mqtt_publish_latency_ms"


def _render_latency_histogram(metrics: Metrics) -> list[str]:
    """The publish-latency histogram in Prometheus's own histogram convention (docs/12 WP14).

    Buckets are **cumulative** and the series ends with `le="+Inf"`, which is not a stylistic
    choice: a scraper fed non-cumulative buckets computes silently wrong quantiles rather than
    erroring, so getting this shape right is the difference between a useful panel and a
    plausible-looking lie.
    """
    histogram = metrics.latency
    lines = [
        f"# HELP {_LATENCY_METRIC} WS frame to MQTT publish latency, milliseconds.",
        f"# TYPE {_LATENCY_METRIC} histogram",
    ]
    for bound, cumulative in histogram.cumulative_buckets():
        label = "+Inf" if bound is None else str(bound)
        lines.append(f'{_LATENCY_METRIC}_bucket{{le="{label}"}} {cumulative}')
    lines.append(f"{_LATENCY_METRIC}_sum {histogram.total_ms:.3f}")
    lines.append(f"{_LATENCY_METRIC}_count {histogram.total}")
    return lines


def render_prometheus_text(metrics: Metrics) -> bytes:
    """Every counter in `metrics.Metrics` plus the latency histogram, Prometheus text exposition
    format (one `# HELP`/`# TYPE`/value triple each; the counters are all monotonically
    increasing, docs/04 §4.2's `stats` names).
    """
    lines: list[str] = []
    for field in _COUNTER_FIELDS:
        name = f"freeathome2mqtt_{field}"
        lines.append(f"# HELP {name} {field.replace('_', ' ')}.")
        lines.append(f"# TYPE {name} counter")
        lines.append(f"{name} {getattr(metrics, field)}")
    lines.extend(_render_latency_histogram(metrics))
    return ("\n".join(lines) + "\n").encode("utf-8")


class MetricsServer:
    """Serves `GET /metrics` on `host:port` for as long as `run()` is awaited."""

    def __init__(
        self,
        *,
        metrics: Metrics,
        host: str = "0.0.0.0",  # noqa: S104 -- meant to be scraped from outside the container
        port: int,
    ) -> None:
        self._metrics = metrics
        self._host = host
        self._port = port
        self._runner: web.AppRunner | None = None
        self._stop_event = asyncio.Event()

    @property
    def port(self) -> int:
        return self._port

    @property
    def is_listening(self) -> bool:
        return self._runner is not None

    async def run(self) -> None:
        """The long-lived task shape `supervisor.py`'s `_spawn_supervised` expects (docs/02 §3):
        serve until `stop()` sets the event, then clean up before returning.
        """
        app = web.Application()
        app.router.add_get("/metrics", self._handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, self._host, self._port)
        await site.start()
        self._runner = runner
        try:
            await self._stop_event.wait()
        finally:
            await runner.cleanup()
            self._runner = None

    async def stop(self) -> None:
        self._stop_event.set()

    async def _handle(self, _request: web.Request) -> web.Response:
        return web.Response(body=render_prometheus_text(self._metrics), content_type="text/plain")
