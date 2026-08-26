"""Unconfirmed-command reconciliation via a single targeted read (ADR-012; docs/11 WP7).

`Reconciler` is the "safety net" half of optimistic command writes: `bus/commands.py` guesses the
new state and publishes it immediately, then arms a timer here. If the SysAP echoes the change
over the WebSocket first, `StateStore.apply()` clears the unconfirmed mark and the timer's own
check finds nothing to do. If it doesn't, this issues one targeted `GET` and publishes the truth --
which may be a rollback (docs/08 §3).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable, Mapping
from typing import Any

from freeathome2mqtt.bus.state import StateStore
from freeathome2mqtt.model.entity import Binding
from freeathome2mqtt.sysap.rest import RestClient, SysApError

logger = logging.getLogger(__name__)

_DEFAULT_MIN_INTERVAL_S = 5.0
_DEFAULT_DELAY_S = 3.0


class RateLimiter:
    """Per-entity read cooldown, shared between `/get` and reconciliation (docs/04 §3.4, P-52).

    A loop publishing `/get` for every entity -- or a burst of unconfirmed commands -- must not be
    able to flood the SysAP with reads; both paths share one instance so the budget is the same
    regardless of which one triggered a read.
    """

    def __init__(
        self,
        *,
        min_interval_s: float = _DEFAULT_MIN_INTERVAL_S,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._min_interval = min_interval_s
        self._clock = clock
        self._last: dict[int, float] = {}

    def allow(self, entity_idx: int) -> bool:
        """Return whether a read for `entity_idx` is allowed now, consuming the budget if so."""
        now = self._clock()
        last = self._last.get(entity_idx)
        if last is not None and now - last < self._min_interval:
            return False
        self._last[entity_idx] = now
        return True


class Reconciler:
    """Reads the truth for one attribute when its optimistic value goes unconfirmed."""

    def __init__(
        self,
        *,
        state: StateStore,
        rest: RestClient,
        ingress_table: Mapping[str, Binding],
        rate_limiter: RateLimiter,
        delay_s: float = _DEFAULT_DELAY_S,
    ) -> None:
        self._state = state
        self._rest = rest
        self._rate_limiter = rate_limiter
        self._delay_s = delay_s
        # Reconciliation reads the same datapoint the attribute's own Binding decodes -- the REST
        # address is the ingress table's own key, dot-separated instead of slash-separated
        # ("SERIAL/chXXXX/odpXXXX" -> "SERIAL.chXXXX.odpXXXX"), so no separate Model field is
        # needed to know where to read from.
        self._reads: dict[tuple[int, int], tuple[str, Callable[[str], Any]]] = {
            (binding.entity_idx, binding.attr_idx): (key.replace("/", "."), binding.decode)
            for key, binding in ingress_table.items()
        }
        self._timers: dict[tuple[int, int], asyncio.Task[None]] = {}

    def schedule(self, entity_idx: int, attr_idx: int) -> None:
        """Arm (or re-arm) the reconciliation timer for one attribute (ADR-012's default 3 s)."""
        key = (entity_idx, attr_idx)
        existing = self._timers.get(key)
        if existing is not None:
            existing.cancel()
        self._timers[key] = asyncio.create_task(self._wait_then_reconcile(entity_idx, attr_idx))

    async def _wait_then_reconcile(self, entity_idx: int, attr_idx: int) -> None:
        await asyncio.sleep(self._delay_s)
        self._timers.pop((entity_idx, attr_idx), None)
        if not (self._state.unconfirmed[entity_idx] & (1 << attr_idx)):
            return  # confirmed by the WS echo already (docs/08 §3) -- nothing to do
        await self.reconcile_now(entity_idx, attr_idx)

    async def reconcile_now(self, entity_idx: int, attr_idx: int) -> None:
        """One targeted read, right now -- used for the timer and F12's immediate case.

        Bounded by the shared rate limiter: if the budget was just used (by a `/get` or another
        reconciliation), this is a silent no-op rather than a second SysAP read (P-52). ADR-012
        already frames reconciliation as "at most one read per unconfirmed command", so a skip
        here is not retried -- the mark simply waits for the next real change to correct it.
        """
        if not self._rate_limiter.allow(entity_idx):
            logger.info(
                "skipping reconciliation for entity %d attr %d: rate limited", entity_idx, attr_idx
            )
            return
        await self.read_one(entity_idx, attr_idx)

    async def read_one(self, entity_idx: int, attr_idx: int) -> None:
        """The actual read-decode-apply, with no rate-limit check of its own.

        `/get` "refresh all attributes" (docs/04 §3.4) is budgeted once *per request*, not once
        per attribute it happens to touch -- so its caller checks the shared rate limiter itself,
        a single time, then calls this directly for every attribute the request targets.
        """
        address, decode = self._reads[(entity_idx, attr_idx)]
        try:
            raw = await self._rest.get_datapoint(address)
        except SysApError as exc:
            logger.warning(
                "reconciliation read failed for entity %d attr %d (%s): %s",
                entity_idx,
                attr_idx,
                address,
                exc,
            )
            return
        # 1 << attr_idx is the same attr_bit convention Binding/StateStore use everywhere else
        # (model/compiler.py's _build_attributes) -- reading the truth always resolves the
        # uncertainty, whether it confirms the guess or rolls it back, so the mark clears
        # either way.
        self._state.apply(entity_idx, attr_idx, decode(raw), attr_bit=1 << attr_idx)
