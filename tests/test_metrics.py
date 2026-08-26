"""Tests for metrics.py: the counters exposed via ``bridge/info`` (docs/04 §4.2; docs/11 WP6)."""

from __future__ import annotations

from freeathome2mqtt.metrics import Metrics


def test_all_counters_start_at_zero() -> None:
    metrics = Metrics()
    assert metrics.datapoints_in == 0
    assert metrics.unmapped_datapoints == 0
    assert metrics.events == 0
    assert metrics.codec_errors == 0


def test_counters_are_independently_mutable() -> None:
    metrics = Metrics()
    metrics.datapoints_in += 5
    metrics.unmapped_datapoints += 1
    metrics.events += 2
    metrics.codec_errors += 3
    assert (
        metrics.datapoints_in,
        metrics.unmapped_datapoints,
        metrics.events,
        metrics.codec_errors,
    ) == (5, 1, 2, 3)
