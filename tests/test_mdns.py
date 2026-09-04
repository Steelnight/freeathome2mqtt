"""Tests for sysap/mdns.py: mDNS discovery of SysAPs (docs/07 §3 ``--discover``; docs/11 WP9).

Registers a real service via `AsyncZeroconf` and discovers it with the real network stack
(loopback multicast), per docs/10 §2's preference for real dependencies over mocks -- mocking
zeroconf's protocol internals would hide exactly the kind of wiring bug (docs/mdns.py's handler
keyword-argument names are load-bearing) a real round trip catches.
"""

from __future__ import annotations

import contextlib
import socket
from collections.abc import AsyncIterator

import pytest
from zeroconf import ServiceInfo
from zeroconf.asyncio import AsyncServiceInfo, AsyncZeroconf

from freeathome2mqtt.sysap.mdns import SysApCandidate, _candidate_from_info, discover_sysaps

_SERVICE_TYPE = "_http._tcp.local."


@contextlib.asynccontextmanager
async def _registered_service(name: str, *, port: int = 80) -> AsyncIterator[None]:
    info = ServiceInfo(
        _SERVICE_TYPE,
        f"{name}.{_SERVICE_TYPE}",
        addresses=[socket.inet_aton("127.0.0.1")],
        port=port,
        properties={},
        server=f"{name.replace(' ', '-').replace('@', '-')}.local.",
    )
    azc = AsyncZeroconf()
    await azc.async_register_service(info)
    try:
        yield
    finally:
        with contextlib.suppress(Exception):
            await azc.async_unregister_service(info)
        await azc.async_close()


async def test_discover_sysaps_finds_a_registered_service() -> None:
    async with _registered_service("free@home Test SysAP", port=8080):
        candidates = await discover_sysaps(timeout_s=1.0)
    assert candidates == [SysApCandidate(name="free@home Test SysAP", host="127.0.0.1", port=8080)]


async def test_discover_sysaps_ignores_services_not_matching_the_prefix() -> None:
    async with _registered_service("some other http service"):
        candidates = await discover_sysaps(timeout_s=1.0)
    assert candidates == []


async def test_discover_sysaps_respects_a_custom_name_prefix() -> None:
    async with _registered_service("custom-prefix-device"):
        candidates = await discover_sysaps(timeout_s=1.0, name_prefix="custom-prefix")
    assert [c.name for c in candidates] == ["custom-prefix-device"]


async def test_discover_sysaps_returns_an_empty_list_when_nothing_matches() -> None:
    candidates = await discover_sysaps(timeout_s=0.5, name_prefix="nobody-advertises-this")
    assert candidates == []


def test_candidate_from_info_returns_none_without_any_addresses() -> None:
    info = AsyncServiceInfo(_SERVICE_TYPE, f"free@home unresolved.{_SERVICE_TYPE}")
    assert _candidate_from_info(info) is None


@pytest.mark.parametrize(
    ("service_name", "expected_name"),
    [
        (f"free@home Living Room.{_SERVICE_TYPE}", "free@home Living Room"),
        (f"free@home.{_SERVICE_TYPE}", "free@home"),
    ],
)
def test_candidate_from_info_strips_the_service_type_suffix(
    service_name: str, expected_name: str
) -> None:
    info = AsyncServiceInfo(_SERVICE_TYPE, service_name)
    object.__setattr__(info, "addresses", [socket.inet_aton("10.0.0.5")])
    info.port = 443
    candidate = _candidate_from_info(info)
    assert candidate == SysApCandidate(name=expected_name, host="10.0.0.5", port=443)
