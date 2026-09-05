"""Records an async benchmark's measured time in the shape `tools/compare_bench.py` reads
(docs/05 §8; docs/10 §7; docs/12 WP13).

Why this exists: only `test_bench_compile` can use pytest-benchmark's `benchmark` fixture -- it
alone is synchronous, CPU-bound work over pure data structures. Every other module in this
directory benchmarks an async path against the fake SysAP and an embedded broker, where the
fixture's synchronous `benchmark()` call does not apply, so each has always timed itself with
`time.perf_counter()` and asserted its own absolute docs/05 §1 budget. That left them outside the
*relative* regression gate: a change that stayed inside an absolute budget while making a path 3x
slower passed unnoticed. `record()` closes that by writing the same JSON shape pytest-benchmark
emits, which `tools/compare_bench.py` then reads alongside `bench/results.json`.

The accumulator below is module-level mutable state, which CLAUDE.md §2 rule 6 discourages -- it
is deliberate and confined to test support: pytest collects one report per *session* across many
modules, so the alternative is threading a collector through every bench test's signature for no
gain. `tests/bench/conftest.py` owns the lifecycle (reset at session start, write at session end),
so the state has exactly one owner, which is what the rule is actually protecting.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

_PATH_ENV = "FAH2MQTT_BENCH_ASYNC_JSON"
_DEFAULT_PATH = Path("bench/results-async.json")

_measurements: dict[str, float] = {}


def record(name: str, value: float) -> None:
    """Record one benchmark measurement. A repeated name overwrites, so a parametrised or retried
    test contributes one entry rather than silently accumulating several.

    `name` states its own unit (`...::p99_seconds`, `...::rss_kib`), because the recorded
    quantities are not all times and `tools/compare_bench.py` compares them unit-neutrally.

    **What is deliberately not recorded here: exact count invariants.** P4 (a 500-datapoint burst
    produces <= 40 publishes), P5 (60 `/set` produce <= 6 SysAP writes) and P12 (repeated values
    produce 0 publishes) are already asserted as exact counts against docs/05 §1. A relative
    "did this grow 25%?" gate adds nothing on top of "is this still exactly 2?" -- it would only
    add a second, weaker way for the same test to fail. Continuous quantities (latency, elapsed
    time, RSS, CPU) are the ones where a relative gate catches what an absolute budget with
    headroom cannot.
    """
    _measurements[name] = value


def reset() -> None:
    """Drop everything recorded so far (session start, so a stale run cannot leak into a report)."""
    _measurements.clear()


def report_path() -> Path:
    """Where the report goes: `$FAH2MQTT_BENCH_ASYNC_JSON`, else `bench/results-async.json`."""
    override = os.environ.get(_PATH_ENV)
    return Path(override) if override else _DEFAULT_PATH


def write_report() -> Path | None:
    """Write every recorded measurement, or return `None` if nothing was recorded -- a fast-suite
    run collects no benchmarks at all, and must not leave an empty report behind for CI to
    compare against.
    """
    if not _measurements:
        return None
    path = report_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "benchmarks": [
            {"fullname": name, "name": name, "stats": {"mean": seconds}}
            for name, seconds in sorted(_measurements.items())
        ]
    }
    path.write_text(json.dumps(report, indent=4) + "\n", encoding="utf-8")
    return path
