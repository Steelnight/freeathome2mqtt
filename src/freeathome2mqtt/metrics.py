"""Counters and histograms exposed via ``bridge/info`` and an optional Prometheus endpoint.

Grows incrementally as later work packages add counters -- only the ones `bus/ingress.py` touches
exist yet (docs/04 §4.2's ``stats`` names, so bridge_api.py can report them verbatim later).
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class Metrics:
    """Plain mutable counters -- one process-wide instance, passed explicitly (CLAUDE.md rule 6)."""

    datapoints_in: int = 0
    unmapped_datapoints: int = 0
    events: int = 0
    codec_errors: int = 0
