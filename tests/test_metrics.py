"""Tests for metrics.py: the counters exposed via ``bridge/info`` (docs/04 §4.2; docs/11 WP6;
docs/12 WP14)."""

from __future__ import annotations

import sys

import pytest
from hypothesis import given
from hypothesis import strategies as st

from freeathome2mqtt.metrics import MAX_BUCKET_MS, LatencyHistogram, Metrics


def test_all_counters_start_at_zero() -> None:
    metrics = Metrics()
    assert metrics.datapoints_in == 0
    assert metrics.unmapped_datapoints == 0
    assert metrics.events == 0
    assert metrics.codec_errors == 0
    assert metrics.task_restarts == 0
    assert metrics.config_reloads == 0


def test_counters_are_independently_mutable() -> None:
    metrics = Metrics()
    metrics.datapoints_in += 5
    metrics.unmapped_datapoints += 1
    metrics.events += 2
    metrics.codec_errors += 3
    metrics.task_restarts += 4
    metrics.config_reloads += 6
    assert (
        metrics.datapoints_in,
        metrics.unmapped_datapoints,
        metrics.events,
        metrics.codec_errors,
        metrics.task_restarts,
        metrics.config_reloads,
    ) == (5, 1, 2, 3, 4, 6)


# ------------------------------------------------------- WP14: the docs/04 §4.2 stats counters


def test_wp14_counters_start_at_zero() -> None:
    metrics = Metrics()
    assert metrics.ws_frames == 0
    assert metrics.state_publishes == 0
    assert metrics.commands == 0
    assert metrics.command_errors == 0


def test_latency_histogram_starts_empty() -> None:
    histogram = LatencyHistogram()
    assert histogram.total == 0
    assert histogram.percentiles() == {"p50": None, "p95": None, "p99": None}


@pytest.mark.parametrize(
    ("seconds", "expected_p50"),
    [
        (0.0004, 1),  # below the first bound lands in the first bucket
        (0.0015, 2),
        (0.004, 5),
        (0.021, 25),
        (0.049, 50),
    ],
)
def test_observation_lands_in_the_bucket_whose_bound_contains_it(
    seconds: float, expected_p50: int
) -> None:
    histogram = LatencyHistogram()
    histogram.observe(seconds)
    assert histogram.percentiles()["p50"] == expected_p50


def test_observations_beyond_the_last_bound_are_clamped_but_counted_separately() -> None:
    """A p99 past the last bound reports that bound (so percentiles stay ordered), and
    `over_max_count` is what stops the clamp from hiding anything.
    """
    histogram = LatencyHistogram()
    histogram.observe(9.0)  # 9 000 ms, far past the last bound
    assert histogram.percentiles()["p99"] == MAX_BUCKET_MS
    assert histogram.over_max_count == 1


def test_percentiles_are_ordered() -> None:
    histogram = LatencyHistogram()
    for i in range(1000):
        histogram.observe(i / 1000)
    percentiles = histogram.percentiles()
    assert percentiles["p50"] <= percentiles["p95"] <= percentiles["p99"]


def test_latency_histogram_memory_is_constant() -> None:
    """Rule 3 (CLAUDE.md §2) / docs/05 §3 R5 as an executable invariant: no collection on this
    path may grow with the number of *events*. A million observations must leave the histogram
    exactly the size it started -- an implementation that kept the samples would fail here.
    """
    histogram = LatencyHistogram()
    histogram.observe(0.001)
    size_before = sys.getsizeof(histogram.counts)
    buckets_before = len(histogram.counts)

    for i in range(1_000_000):
        histogram.observe((i % 500) / 1000)

    assert sys.getsizeof(histogram.counts) == size_before
    assert len(histogram.counts) == buckets_before
    assert histogram.total == 1_000_001  # it did observe them all; it just did not store them


@given(st.lists(st.floats(min_value=0.0, max_value=5.0, allow_nan=False), min_size=1, max_size=200))
def test_percentiles_are_monotonic_for_any_sample_set(samples: list[float]) -> None:
    histogram = LatencyHistogram()
    for value in samples:
        histogram.observe(value)
    percentiles = histogram.percentiles()
    assert percentiles["p50"] <= percentiles["p95"] <= percentiles["p99"]
    assert histogram.total == len(samples)


@given(st.lists(st.floats(min_value=0.0, max_value=5.0, allow_nan=False), min_size=1, max_size=200))
def test_cumulative_buckets_end_at_the_total(samples: list[float]) -> None:
    histogram = LatencyHistogram()
    for value in samples:
        histogram.observe(value)
    cumulative = histogram.cumulative_buckets()
    assert [count for _bound, count in cumulative] == sorted(count for _b, count in cumulative)
    assert cumulative[-1][0] is None  # the +Inf bucket
    assert cumulative[-1][1] == histogram.total


def test_negative_observations_are_ignored_rather_than_corrupting_the_histogram() -> None:
    """A clock that went backwards (F20's failure mode) must not produce a negative latency
    sample; dropping it keeps the percentiles honest instead of skewing them toward zero.
    """
    histogram = LatencyHistogram()
    histogram.observe(-1.0)
    assert histogram.total == 0
