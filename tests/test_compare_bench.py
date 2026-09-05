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


# --------------------------------------------------------------- WP13: the async bench entries


def test_main_reads_every_results_file_not_just_the_last(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """The async benchmarks are recorded into their own file, so the gate reads more than one
    report (docs/12 WP13). The regression is put in the *first* file deliberately: a plain
    last-wins `--results` would silently ignore it and pass.
    """
    baseline_path = tmp_path / "baseline.json"
    first = tmp_path / "results.json"
    second = tmp_path / "results-async.json"
    baseline_path.write_text(json.dumps(_report({"sync": 1.0, "async": 2.0})), encoding="utf-8")
    first.write_text(json.dumps(_report({"sync": 2.0})), encoding="utf-8")
    second.write_text(json.dumps(_report({"async": 2.0})), encoding="utf-8")

    exit_code = main(
        ["--baseline", str(baseline_path), "--results", str(first), "--results", str(second)]
    )

    assert exit_code == 1
    assert "sync" in capsys.readouterr().err


def test_main_is_clean_when_every_merged_results_file_is_within_budget(tmp_path: Path) -> None:
    baseline_path = tmp_path / "baseline.json"
    first = tmp_path / "results.json"
    second = tmp_path / "results-async.json"
    baseline_path.write_text(json.dumps(_report({"sync": 1.0, "async": 2.0})), encoding="utf-8")
    first.write_text(json.dumps(_report({"sync": 1.0})), encoding="utf-8")
    second.write_text(json.dumps(_report({"async": 2.0})), encoding="utf-8")

    assert (
        main(["--baseline", str(baseline_path), "--results", str(first), "--results", str(second)])
        == 0
    )


def test_compare_bench_detects_async_regression(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """WP13's acceptance test: an async benchmark 40% slower than its baseline fails the gate.

    This is the assertion that proves the recorded async entries are actually *gated* and not
    merely written to a file nobody compares (docs/12 WP13; docs/10 §7).
    """
    baseline_path = tmp_path / "baseline.json"
    sync_path = tmp_path / "results.json"
    async_path = tmp_path / "results-async.json"
    baseline_path.write_text(
        json.dumps(_report({"test_bench_compile": 1.0, "test_bench_latency": 2.0})),
        encoding="utf-8",
    )
    sync_path.write_text(json.dumps(_report({"test_bench_compile": 1.0})), encoding="utf-8")
    async_path.write_text(json.dumps(_report({"test_bench_latency": 2.8})), encoding="utf-8")

    exit_code = main(
        [
            "--baseline",
            str(baseline_path),
            "--results",
            str(sync_path),
            "--results",
            str(async_path),
        ]
    )

    assert exit_code == 1
    assert "test_bench_latency" in capsys.readouterr().err


def test_main_reports_a_missing_results_file_rather_than_crashing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """CI runs the async recorder and the pytest-benchmark run as separate steps; if one produced
    no file the gate must say so, not raise `FileNotFoundError` (CLAUDE.md §2 rule 7).
    """
    baseline_path = tmp_path / "baseline.json"
    baseline_path.write_text(json.dumps(_report({"a": 1.0})), encoding="utf-8")

    exit_code = main(["--baseline", str(baseline_path), "--results", str(tmp_path / "absent.json")])

    assert exit_code == 1
    assert "absent.json" in capsys.readouterr().err


def test_main_reports_how_many_results_have_no_baseline_entry(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Recorded-but-not-gated is a real state (docs/12 WP13: three async measurements are too
    noisy for a 25% relative gate and are deliberately absent from the baseline). It has to be
    *visible* in the CI log rather than silently indistinguishable from "all clear".
    """
    baseline_path = tmp_path / "baseline.json"
    results_path = tmp_path / "results.json"
    baseline_path.write_text(json.dumps(_report({"gated": 1.0})), encoding="utf-8")
    results_path.write_text(
        json.dumps(_report({"gated": 1.0, "ungated_a": 2.0, "ungated_b": 3.0})), encoding="utf-8"
    )

    exit_code = main(["--baseline", str(baseline_path), "--results", str(results_path)])

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "2" in out and "no baseline entry" in out
