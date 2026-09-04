"""Tests for backoff.py: the single full-jitter delay policy (docs/06 §3).

Four modules (`sysap/rest.py`, `sysap/ws.py`, `mqtt/client.py`, `supervisor.py`) each retry on a
different link, but docs/06 §3 defines *one* policy for all of them. These tests pin the shape of
that one implementation so a policy change stays a one-line edit rather than four.
"""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from freeathome2mqtt.backoff import backoff_delay


def test_delay_never_exceeds_the_uncapped_ceiling() -> None:
    """Full jitter (docs/06 §3): `sleep = random(0, min(cap, initial * factor**(attempt-1)))`."""
    for attempt in range(1, 6):
        ceiling = min(60.0, 1.0 * 2.0 ** (attempt - 1))
        for _ in range(50):
            delay = backoff_delay(attempt, initial=1.0, factor=2.0, cap=60.0)
            assert 0.0 <= delay <= ceiling


def test_delay_is_capped_once_the_ceiling_is_reached() -> None:
    """A high attempt number must never produce a delay above `cap`."""
    for _ in range(100):
        assert 0.0 <= backoff_delay(30, initial=1.0, factor=2.0, cap=60.0) <= 60.0


def test_first_attempt_uses_the_initial_delay_as_its_ceiling() -> None:
    """`attempt=1` means `factor**0 == 1`, so the ceiling is exactly `initial`."""
    for _ in range(100):
        assert 0.0 <= backoff_delay(1, initial=0.5, factor=2.0, cap=30.0) <= 0.5


def test_jitter_actually_varies() -> None:
    """docs/06 §3: jitter is mandatory -- a constant delay would let instances lock-step."""
    seen = {backoff_delay(5, initial=1.0, factor=2.0, cap=60.0) for _ in range(200)}
    assert len(seen) > 1


@given(
    attempt=st.integers(min_value=1, max_value=64),
    initial=st.floats(min_value=0.01, max_value=10.0),
    factor=st.floats(min_value=1.0, max_value=4.0),
    cap=st.floats(min_value=0.01, max_value=600.0),
)
def test_delay_is_always_within_zero_and_cap(
    attempt: int, initial: float, factor: float, cap: float
) -> None:
    """The stated invariant, property-checked (docs/10 §5): never negative, never above `cap`."""
    delay = backoff_delay(attempt, initial=initial, factor=factor, cap=cap)
    assert 0.0 <= delay <= cap


@pytest.mark.parametrize("attempt", [0, -1])
def test_non_positive_attempt_is_rejected(attempt: int) -> None:
    """Attempts are 1-based; a 0 or negative attempt is a caller bug, not a silent tiny delay."""
    with pytest.raises(ValueError, match="attempt"):
        backoff_delay(attempt, initial=1.0, factor=2.0, cap=60.0)
