"""Compares benchmark JSON reports against a committed baseline and fails if any benchmark's mean
time regressed beyond a threshold (docs/10 §7; docs/11 WP12; docs/12 WP13).

Two reports feed this, because the bench suite measures two different kinds of thing:

* ``bench/results.json`` -- written by pytest-benchmark's ``benchmark`` fixture, which only
  ``test_bench_compile`` can use: it alone is synchronous, CPU-bound work over pure data
  structures.
* ``bench/results-async.json`` -- written by ``tests/bench/_record.py``, the hand-rolled recorder
  the async benchmarks use. They exercise real socket I/O against the fake SysAP and an embedded
  broker, where the fixture's synchronous ``benchmark()`` call does not apply, so each measures
  itself and records the result in this same shape (docs/12 WP13 closed the gap docs/10 §7 had
  named as a tracked improvement: before it, only ``test_bench_compile`` was gated). Each entry
  names its own unit, because not all of them are times -- see ``_record.py`` for which
  quantities are gated relatively and which stay purely absolute.

``--results`` may therefore be repeated, and every file given is read: passing several and
comparing only the last would silently drop a whole class of regression. A benchmark present on
only one side is reported informationally, not treated as a regression -- there is nothing to
compare it against yet.

Run via ``uv run python -m freeathome2mqtt.tools.compare_bench --baseline bench/baseline.json
--results bench/results.json --results bench/results-async.json``.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable, Sequence
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
                # Unit-neutral on purpose: pytest-benchmark's entries are seconds, but the
                # recorded async ones also carry an RSS in KiB and a dimensionless CPU fraction
                # (docs/12 WP13). Each name states its own unit; printing "ms" for all of them
                # would be wrong for two of the three.
                f"{name}: regressed {growth:.1%} "
                f"({old_mean:.6g} -> {new_mean:.6g}), exceeds the {threshold:.0%} gate"
            )
    return regressions


def merge_reports(reports: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """One report carrying every input's benchmarks, so several results files gate as a whole."""
    merged: list[Any] = []
    for report in reports:
        merged.extend(report["benchmarks"])
    return {"benchmarks": merged}


def _load_reports(paths: Sequence[Path]) -> tuple[list[dict[str, Any]], list[str]]:
    """Parse each report, collecting a message per unreadable path rather than raising: CI writes
    these files in separate steps, and "the recorder produced nothing" has to fail the gate with
    an actionable message, not a bare traceback (CLAUDE.md §2 rule 7).
    """
    reports: list[dict[str, Any]] = []
    errors: list[str] = []
    for path in paths:
        try:
            reports.append(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError) as exc:
            errors.append(f"{path}: could not be read as a benchmark report ({exc})")
    return reports, errors


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point (used by CI's ``bench`` job, main branch only -- docs/10 §9)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True, action="append")
    parser.add_argument("--threshold", type=float, default=_DEFAULT_THRESHOLD)
    args = parser.parse_args(argv)

    reports, errors = _load_reports([args.baseline, *args.results])
    if errors:
        for message in errors:
            print(message, file=sys.stderr)
        return 1
    baseline, results = reports[0], merge_reports(reports[1:])
    regressions = find_regressions(baseline, results, threshold=args.threshold)
    if regressions:
        for message in regressions:
            print(message, file=sys.stderr)
        return 1

    gated = _stats_by_name(baseline).keys() & _stats_by_name(results).keys()
    ungated = len(_stats_by_name(results)) - len(gated)
    print(
        f"no benchmark regressed beyond {args.threshold:.0%} vs. {args.baseline} "
        f"({len(gated)} gated across {len(args.results)} report(s))"
    )
    if ungated:
        # Deliberately visible: some measurements are recorded for the record but excluded from
        # the baseline because they are too noisy for a relative gate to be signal (docs/12 WP13
        # measured which). "Recorded but not gated" must not read the same as "all clear".
        print(f"{ungated} recorded benchmark(s) have no baseline entry and are not gated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
