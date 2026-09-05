"""Counters and the latency histogram exposed via ``bridge/info`` and the Prometheus endpoint
(docs/04 §4.2; docs/05 §9; docs/11 WP6; docs/12 WP14).

WP14 completed the docs/04 §4.2 ``stats`` object. Before it, five documented keys -- ``ws_frames``,
``state_publishes``, ``commands``, ``command_errors`` and ``latency_ms`` -- had no counter behind
them anywhere, which mattered beyond a missing field: docs/05 §9 step 4 tells an operator to
"instrument counters rather than guessing ... usually localise the problem to ingress, egress or
the broker without a profiler at all", and the counters that recipe names did not exist. Every
counter here also reaches Prometheus for free, since `metrics_server.py` renders whatever this
module holds.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass, field

LATENCY_BUCKET_BOUNDS_MS: tuple[int, ...] = (1, 2, 5, 10, 15, 20, 25, 30, 40, 50, 75, 100, 200, 500)
"""Upper bounds, in milliseconds, of the fixed latency buckets.

Deliberately dense between 10 ms and 50 ms: that is where P1's 50 ms budget and the default 20 ms
coalescing window both sit, so it is the region where a reported percentile has to be able to tell
"fine" from "about to breach". Coarse either side of it, because a 200 ms and a 500 ms publish are
equally bad news and do not need distinguishing.
"""

MAX_BUCKET_MS = LATENCY_BUCKET_BOUNDS_MS[-1]
"""The last finite bound. A percentile landing past it is reported *as* it, per the ordinary
convention for a bucketed histogram -- and `LatencyHistogram.over_max_count` is published beside
the percentiles so that understatement is never silent: an operator sees both "p99 is 500 ms" and
"and 17 publishes were worse than that".
"""


@dataclass(slots=True)
class LatencyHistogram:
    """WS-frame-to-MQTT-publish latency in fixed buckets (docs/04 §4.2's ``latency_ms``).

    Fixed-bucket, not a reservoir of samples: CLAUDE.md §2 rule 3 and docs/05 §3 R5 both forbid a
    collection that grows with the number of events, and latency samples arrive at the publish
    rate. `counts` is allocated once at construction and never resized -- a million observations
    leave this object exactly the size it started, which
    `test_latency_histogram_memory_is_constant` pins down.
    """

    counts: list[int] = field(default_factory=lambda: [0] * (len(LATENCY_BUCKET_BOUNDS_MS) + 1))
    total: int = 0
    total_ms: float = 0.0

    def observe(self, seconds: float) -> None:
        """Record one latency. Negative input is dropped rather than bucketed: it can only come
        from a clock that moved backwards (docs/06 §6 F20), and skewing the percentiles toward
        zero would misreport exactly when something is wrong.
        """
        if seconds < 0:
            return
        milliseconds = seconds * 1000
        index = bisect.bisect_left(LATENCY_BUCKET_BOUNDS_MS, milliseconds)
        self.counts[index] += 1
        self.total += 1
        self.total_ms += milliseconds

    def percentiles(self) -> dict[str, int | None]:
        """``{"p50": ..., "p95": ..., "p99": ...}`` as bucket upper bounds in milliseconds.

        Each value is the bound of the bucket the quantile falls into, so it is an upper estimate
        rather than an interpolated figure -- which is the honest reading of a bucketed histogram
        and enough to answer the only question asked of it ("are we inside budget?"). `None`
        before any sample has been observed.

        The returned values are always ordered p50 <= p95 <= p99, which is why a quantile past the
        last bound reports `MAX_BUCKET_MS` rather than a sentinel: a sentinel that sorted below
        the other two would make the object self-contradictory. `over_max_count` carries what that
        clamp hides.
        """
        if self.total == 0:
            return {"p50": None, "p95": None, "p99": None}
        wanted = (("p50", 0.50), ("p95", 0.95), ("p99", 0.99))
        return {name: self._quantile(quantile) for name, quantile in wanted}

    @property
    def over_max_count(self) -> int:
        """Observations worse than the last bound -- what a clamped p99 does not show."""
        return self.counts[-1]

    def cumulative_buckets(self) -> list[tuple[int | None, int]]:
        """``(upper bound ms, count <= that bound)`` pairs for Prometheus's cumulative
        ``_bucket{le=...}`` convention. The final pair's bound is `None`, meaning `+Inf`.
        """
        cumulative = 0
        pairs: list[tuple[int | None, int]] = []
        for bound, count in zip(LATENCY_BUCKET_BOUNDS_MS, self.counts, strict=False):
            cumulative += count
            pairs.append((bound, cumulative))
        pairs.append((None, cumulative + self.counts[-1]))
        return pairs

    def _quantile(self, quantile: float) -> int:
        target = quantile * self.total
        cumulative = 0
        for bound, count in zip(LATENCY_BUCKET_BOUNDS_MS, self.counts, strict=False):
            cumulative += count
            if cumulative >= target:
                return bound
        return MAX_BUCKET_MS


@dataclass(slots=True)
class Metrics:
    """Plain mutable counters -- one process-wide instance, passed explicitly (CLAUDE.md rule 6)."""

    datapoints_in: int = 0
    unmapped_datapoints: int = 0
    events: int = 0
    codec_errors: int = 0
    task_restarts: int = 0
    config_reloads: int = 0
    ws_frames: int = 0
    state_publishes: int = 0
    commands: int = 0
    command_errors: int = 0
    latency: LatencyHistogram = field(default_factory=LatencyHistogram)
