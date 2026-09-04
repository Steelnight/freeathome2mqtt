"""tools/compare_bench.py: the CI regression gate for the one benchmark that goes through
pytest-benchmark's `benchmark` fixture (docs/10 §7; docs/11 WP12).
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from freeathome2mqtt.tools.compare_bench import find_regressions, main


def _report(entries: dict[str, float]) -> dict[str, object]:
    return {
        "benchmarks": [
            {"fullname": name, "stats": {"mean": mean}} for name, mean in entries.items()
        ]
    }


def test_no_regressions_when_results_match_baseline() -> None:
    baseline = _report({"a": 1.0, "b": 2.0})
    results = _report({"a": 1.0, "b": 2.0})
    assert find_regressions(baseline, results) == []


def test_no_regressions_when_results_are_faster() -> None:
    baseline = _report({"a": 1.0})
    results = _report({"a": 0.5})
    assert find_regressions(baseline, results) == []


def test_regression_reported_when_mean_grows_beyond_threshold() -> None:
    baseline = _report({"a": 1.0})
    results = _report({"a": 1.30})
    regressions = find_regressions(baseline, results, threshold=0.25)
    assert len(regressions) == 1
    assert "a" in regressions[0]


def test_growth_within_threshold_is_not_a_regression() -> None:
    baseline = _report({"a": 1.0})
    results = _report({"a": 1.20})
    assert find_regressions(baseline, results, threshold=0.25) == []


def test_benchmark_missing_from_baseline_is_not_a_regression() -> None:
    baseline = _report({"a": 1.0})
    results = _report({"a": 1.0, "new_bench": 5.0})
    assert find_regressions(baseline, results) == []


def test_benchmark_missing_from_results_is_not_a_regression() -> None:
    baseline = _report({"a": 1.0, "gone": 2.0})
    results = _report({"a": 1.0})
    assert find_regressions(baseline, results) == []


def test_zero_or_negative_baseline_mean_is_skipped_not_a_division_error() -> None:
    baseline = _report({"a": 0.0})
    results = _report({"a": 1.0})
    assert find_regressions(baseline, results) == []


def test_main_exits_zero_and_prints_summary_when_clean(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    baseline_path = tmp_path / "baseline.json"
    results_path = tmp_path / "results.json"
    baseline_path.write_text(json.dumps(_report({"a": 1.0})), encoding="utf-8")
    results_path.write_text(json.dumps(_report({"a": 1.0})), encoding="utf-8")

    exit_code = main(["--baseline", str(baseline_path), "--results", str(results_path)])

    assert exit_code == 0
    assert "no benchmark regressed" in capsys.readouterr().out


def test_main_exits_one_and_prints_regressions_to_stderr(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    baseline_path = tmp_path / "baseline.json"
    results_path = tmp_path / "results.json"
    baseline_path.write_text(json.dumps(_report({"a": 1.0})), encoding="utf-8")
    results_path.write_text(json.dumps(_report({"a": 2.0})), encoding="utf-8")

    exit_code = main(["--baseline", str(baseline_path), "--results", str(results_path)])

    assert exit_code == 1
    assert "a" in capsys.readouterr().err


def test_main_honours_custom_threshold(tmp_path: Path) -> None:
    baseline_path = tmp_path / "baseline.json"
    results_path = tmp_path / "results.json"
    baseline_path.write_text(json.dumps(_report({"a": 1.0})), encoding="utf-8")
    results_path.write_text(json.dumps(_report({"a": 1.10})), encoding="utf-8")

    assert (
        main(
            [
                "--baseline",
                str(baseline_path),
                "--results",
                str(results_path),
                "--threshold",
                "0.05",
            ]
        )
        == 1
    )
    assert (
        main(
            ["--baseline", str(baseline_path), "--results", str(results_path), "--threshold", "0.5"]
        )
        == 0
    )
