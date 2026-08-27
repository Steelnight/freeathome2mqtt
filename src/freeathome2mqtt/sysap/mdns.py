"""Optional zeroconf discovery of the SysAP on the local network (docs/01 §1; docs/07 §3
``--discover``; docs/11 WP9).

The SysAP advertises itself as an `_http._tcp.local.` mDNS service named `free@home*`
(docs/07 §3). `discover_sysaps` browses for `timeout_s`, resolving every matching service it
sees, and returns whatever it found once the window closes -- mDNS has no notion of "done", only
"nothing new arrived for a while", so a fixed listen window is the only sane stopping condition
(CLAUDE.md rule 2).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from zeroconf import ServiceStateChange, Zeroconf
from zeroconf.asyncio import AsyncServiceBrowser, AsyncServiceInfo, AsyncZeroconf

logger = logging.getLogger(__name__)

_SERVICE_TYPE = "_http._tcp.local."
_DEFAULT_NAME_PREFIX = "free@home"
_DEFAULT_TIMEOUT_S = 5.0
_RESOLVE_TIMEOUT_MS = 3000


@dataclass(frozen=True, slots=True)
class SysApCandidate:
    """One mDNS-discovered SysAP: enough to build a `sysap.host` URL from."""

    name: str
    host: str
    port: int


def _candidate_from_info(info: AsyncServiceInfo) -> SysApCandidate | None:
    """Pure by design (CLAUDE.md rule 9) so it's testable without a real mDNS responder --
    `discover_sysaps` is the only caller that needs the network.
    """
    addresses = info.parsed_addresses()
    if not addresses:
        return None
    name = info.name.removesuffix(f".{_SERVICE_TYPE}")
    return SysApCandidate(name=name, host=addresses[0], port=info.port or 80)


async def discover_sysaps(
    *, timeout_s: float = _DEFAULT_TIMEOUT_S, name_prefix: str = _DEFAULT_NAME_PREFIX
) -> list[SysApCandidate]:
    """Scan `_http._tcp.local.` for `name_prefix*` services for `timeout_s` seconds."""
    found: dict[str, SysApCandidate] = {}
    tasks: set[asyncio.Task[None]] = set()

    async def _resolve(zc: Zeroconf, service_type: str, name: str) -> None:
        info = AsyncServiceInfo(service_type, name)
        if await info.async_request(zc, _RESOLVE_TIMEOUT_MS):
            candidate = _candidate_from_info(info)
            if candidate is not None:
                found[name] = candidate

    def _on_change(
        zeroconf: Zeroconf, service_type: str, name: str, state_change: ServiceStateChange
    ) -> None:
        # zeroconf's `Signal.fire()` calls handlers with keyword arguments matching these exact
        # parameter names -- `zeroconf`, not `zc` -- so the names here are load-bearing.
        if state_change is not ServiceStateChange.Added or not name.startswith(name_prefix):
            return
        task = asyncio.ensure_future(_resolve(zeroconf, service_type, name))
        tasks.add(task)
        task.add_done_callback(tasks.discard)

    async with AsyncZeroconf() as azc:
        browser = AsyncServiceBrowser(azc.zeroconf, _SERVICE_TYPE, handlers=[_on_change])
        try:
            await asyncio.sleep(timeout_s)
        finally:
            await browser.async_cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    return sorted(found.values(), key=lambda c: c.name)
