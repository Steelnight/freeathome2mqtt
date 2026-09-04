"""Compares a pytest-benchmark JSON report against a committed baseline and fails if any
benchmark's mean time regressed beyond a threshold (docs/10 §7; docs/11 WP12).

Only benchmarks that go through pytest-benchmark's ``benchmark`` fixture appear in either file --
today that is ``test_bench_compile`` alone. The rest of ``tests/bench/`` exercises async I/O
against the fake SysAP/broker, where the fixture's synchronous ``benchmark()`` call doesn't apply;
those assert directly against their own docs/05 §1 budget with manual timing instead (still run by
CI's ``bench`` job, just outside this relative-regression check -- docs/10 §7 says so). A
benchmark present on only one side is reported informationally, not treated as a regression: there
is nothing to compare it against yet.

Run via ``uv run python -m freeathome2mqtt.tools.compare_bench --baseline bench/baseline.json
--results bench/results.json``.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

_DEFAULT_THRESHOLD = 0.25


def _stats_by_name(report: dict[str, Any]) -> dict[str, float]:
    return {b["fullname"]: b["stats"]["mean"] for b in report["benchmarks"]}


def find_regressions(
    baseline: dict[str, Any], results: dict[str, Any], *, threshold: float = _DEFAULT_THRESHOLD
) -> list[str]:
    """One message per benchmark whose mean grew by more than `threshold` (a fraction, e.g. 0.25
    for 25%) versus `baseline`. A benchmark missing from either side is skipped, not flagged.
    """
    baseline_means = _stats_by_name(baseline)
    result_means = _stats_by_name(results)
    regressions: list[str] = []
    for name, new_mean in result_means.items():
        old_mean = baseline_means.get(name)
        if old_mean is None or old_mean <= 0:
            continue
        growth = (new_mean - old_mean) / old_mean
        if growth > threshold:
            regressions.append(
                f"{name}: mean regressed {growth:.1%} "
                f"({old_mean * 1000:.2f}ms -> {new_mean * 1000:.2f}ms), "
                f"exceeds the {threshold:.0%} gate"
            )
    return regressions


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point (used by CI's ``bench`` job, main branch only -- docs/10 §9)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--threshold", type=float, default=_DEFAULT_THRESHOLD)
    args = parser.parse_args(argv)

    baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    results = json.loads(args.results.read_text(encoding="utf-8"))
    regressions = find_regressions(baseline, results, threshold=args.threshold)
    if regressions:
        for message in regressions:
            print(message, file=sys.stderr)
        return 1
    print(f"no benchmark regressed beyond {args.threshold:.0%} vs. {args.baseline}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
