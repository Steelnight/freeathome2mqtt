"""Unauthenticated ``/settings.json`` pre-flight: version gate, serial resolution (docs/01 §1.1).

Not under ``/fhapi/v1`` and does not take auth (docs/01 §1.1) -- used before any credentials are
known, to refuse to start against unsupported firmware with a clear message rather than a
confusing 401/404 later, and to resolve the installation's serial for the MQTT client id (ADR-002).
"""

from __future__ import annotations

import re
from dataclasses import dataclass

import aiohttp

MINIMUM_VERSION: tuple[int, int, int] = (2, 6, 0)

_VERSION_RE = re.compile(r"(\d+)\.(\d+)\.(\d+)")


class UnsupportedFirmwareError(Exception):
    """The SysAP's firmware is below `MINIMUM_VERSION` (docs/01 §1)."""


@dataclass(frozen=True, slots=True)
class SysApUser:
    """One entry of settings.json's ``users`` list (docs/01 §1.1)."""

    name: str
    jid: str


@dataclass(frozen=True, slots=True)
class SysApSettings:
    """The parsed body of the unauthenticated ``GET /settings.json`` (docs/01 §1.1)."""

    version: str
    serial_number: str
    name: str
    users: tuple[SysApUser, ...]


def parse_version(raw: str) -> tuple[int, int, int]:
    """Parse a ``MAJOR.MINOR.PATCH`` firmware version string.

    Raises ``ValueError`` for a malformed string: settings.json is unauthenticated, unvalidated
    input, so this is a trust boundary (CLAUDE.md rule 5), not an assertion.
    """
    match = _VERSION_RE.fullmatch(raw.strip())
    if match is None:
        raise ValueError(f"malformed firmware version: {raw!r}")
    return (int(match.group(1)), int(match.group(2)), int(match.group(3)))


def check_version_supported(raw: str) -> None:
    """Raise `UnsupportedFirmwareError` if `raw` is below `MINIMUM_VERSION` (docs/01 §1)."""
    if parse_version(raw) < MINIMUM_VERSION:
        minimum = ".".join(str(part) for part in MINIMUM_VERSION)
        raise UnsupportedFirmwareError(
            f"SysAP firmware {raw} is below the minimum supported {minimum}"
        )


def find_jid(users: tuple[SysApUser, ...], username: str) -> str | None:
    """Look up a user's ``jid`` by display name, for the Basic-auth fallback (docs/01 §1.1)."""
    for user in users:
        if user.name == username:
            return user.jid
    return None


def _parse_users(body: dict[str, object]) -> tuple[SysApUser, ...]:
    raw_users = body.get("users")
    if not isinstance(raw_users, list):
        return ()
    users = []
    for entry in raw_users:
        if not isinstance(entry, dict):
            continue
        name, jid = entry.get("name"), entry.get("jid")
        if isinstance(name, str) and isinstance(jid, str):
            users.append(SysApUser(name=name, jid=jid))
    return tuple(users)


def parse_settings(body: dict[str, object]) -> SysApSettings:
    """Parse the JSON body of ``GET /settings.json`` (docs/01 §1.1)."""
    flags = body.get("flags")
    if not isinstance(flags, dict):
        raise ValueError("settings.json response is missing 'flags'")
    version, serial_number, name = (
        flags.get("version"),
        flags.get("serialNumber"),
        flags.get("name"),
    )
    if (
        not isinstance(version, str)
        or not isinstance(serial_number, str)
        or not isinstance(name, str)
    ):
        raise ValueError("settings.json 'flags' is missing version/serialNumber/name")
    return SysApSettings(
        version=version, serial_number=serial_number, name=name, users=_parse_users(body)
    )


async def fetch_settings(session: aiohttp.ClientSession, base_url: str) -> SysApSettings:
    """``GET /settings.json`` with no auth (docs/01 §1.1) and parse the response."""
    async with session.get(f"{base_url}/settings.json") as response:
        response.raise_for_status()
        body = await response.json(content_type=None)
    return parse_settings(body)
