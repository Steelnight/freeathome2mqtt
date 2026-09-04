"""The one full-jitter backoff delay, shared by every retrying link (docs/06 §3).

docs/06 §3 defines a *single* policy -- ``sleep = random(0, min(cap, initial *
factor**(attempt-1)))`` -- and then tabulates per-link `initial`/`factor`/`cap` values for the
SysAP WebSocket, SysAP REST, MQTT and the supervisor's task-restart shim. Those four callers
differ only in the constants they pass, so the policy itself lives here once: a change to how the
delay is computed (equal jitter, decorrelated jitter, a different ceiling) is a one-line edit
rather than four edits that can silently drift apart.

Full jitter, not equal jitter, is deliberate: docs/06 §3 requires that several bridges (or a
bridge plus the Home Assistant integration) reconnecting against a SysAP that has just rebooted
must not do so in lockstep and hold it down.
"""

from __future__ import annotations

import random


def backoff_delay(attempt: int, *, initial: float, factor: float, cap: float) -> float:
    """Full jitter (docs/06 §3): ``sleep = random(0, min(cap, initial * factor**(attempt-1)))``.

    `attempt` is 1-based, so the first retry's ceiling is exactly `initial`. A non-positive
    `attempt` is a caller bug -- raised rather than silently yielding a near-zero delay, which
    would turn a retry loop into the hot spin docs/06 §3 forbids (CLAUDE.md rule 2).
    """
    if attempt < 1:
        raise ValueError(f"attempt must be 1-based and positive, got {attempt}")
    ceiling = min(cap, initial * factor ** (attempt - 1))
    return random.uniform(0, ceiling)  # noqa: S311 -- timing jitter, not a cryptographic use
