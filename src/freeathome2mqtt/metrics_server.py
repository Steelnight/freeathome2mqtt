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
)


def render_prometheus_text(metrics: Metrics) -> bytes:
    """Every counter in `metrics.Metrics`, Prometheus text exposition format (one `# HELP`/
    `# TYPE`/value triple each, all typed `counter` -- every field here is monotonically
    increasing, docs/06 §4.2's `stats` names).
    """
    lines: list[str] = []
    for field in _COUNTER_FIELDS:
        name = f"freeathome2mqtt_{field}"
        lines.append(f"# HELP {name} {field.replace('_', ' ')}.")
        lines.append(f"# TYPE {name} counter")
        lines.append(f"{name} {getattr(metrics, field)}")
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
