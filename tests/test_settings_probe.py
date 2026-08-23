"""Tests for sysap/settings_probe.py: version gate, serial, jid fallback (docs/01 §1.1)."""

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from freeathome2mqtt.sysap.settings_probe import (
    SysApUser,
    UnsupportedFirmwareError,
    check_version_supported,
    fetch_settings,
    find_jid,
    parse_settings,
    parse_version,
)

FLAGS_BODY = {
    "flags": {
        "version": "2.6.4",
        "hardwareVersion": "1.0",
        "serialNumber": "ABB7005500E1",
        "name": "My Home",
    },
    "users": [{"name": "installer", "jid": "abc123@busch-jaeger.de"}],
}


def test_parse_version_accepts_well_formed_string() -> None:
    assert parse_version("2.6.4") == (2, 6, 4)


@pytest.mark.parametrize("raw", ["2.6", "not-a-version", "", "2.6.4-beta"])
def test_parse_version_rejects_malformed_string(raw: str) -> None:
    with pytest.raises(ValueError):
        parse_version(raw)


def test_version_gate_rejects_pre_2_6_0() -> None:
    with pytest.raises(UnsupportedFirmwareError):
        check_version_supported("2.5.9")


def test_version_gate_accepts_2_6_0() -> None:
    check_version_supported("2.6.0")


def test_version_gate_accepts_newer_versions() -> None:
    check_version_supported("2.7.1")


def test_find_jid_returns_matching_user() -> None:
    users = (SysApUser(name="installer", jid="abc123@busch-jaeger.de"),)
    assert find_jid(users, "installer") == "abc123@busch-jaeger.de"


def test_find_jid_returns_none_when_absent() -> None:
    users = (SysApUser(name="installer", jid="abc123@busch-jaeger.de"),)
    assert find_jid(users, "someone_else") is None


def test_parse_settings_extracts_version_serial_and_users() -> None:
    settings = parse_settings(FLAGS_BODY)
    assert settings.version == "2.6.4"
    assert settings.serial_number == "ABB7005500E1"
    assert settings.name == "My Home"
    assert settings.users == (SysApUser(name="installer", jid="abc123@busch-jaeger.de"),)


def test_parse_settings_rejects_missing_flags() -> None:
    with pytest.raises(ValueError):
        parse_settings({})


def test_parse_settings_rejects_incomplete_flags() -> None:
    with pytest.raises(ValueError):
        parse_settings({"flags": {"version": "2.6.4"}})


def test_parse_settings_tolerates_missing_users() -> None:
    body = {"flags": FLAGS_BODY["flags"]}
    settings = parse_settings(body)
    assert settings.users == ()


def test_parse_settings_skips_malformed_user_entries() -> None:
    body = {
        "flags": FLAGS_BODY["flags"],
        "users": [
            "not-a-dict",
            {"name": "installer"},  # missing jid
            {"name": "installer", "jid": "abc123@busch-jaeger.de"},
        ],
    }
    settings = parse_settings(body)
    assert settings.users == (SysApUser(name="installer", jid="abc123@busch-jaeger.de"),)


async def test_fetch_settings_round_trips_over_real_http() -> None:
    async def handler(_request: web.Request) -> web.Response:
        return web.json_response(FLAGS_BODY)

    app = web.Application()
    app.router.add_get("/settings.json", handler)
    async with TestClient(TestServer(app)) as client:
        base_url = str(client.make_url("")).rstrip("/")
        settings = await fetch_settings(client.session, base_url)

    assert settings.version == "2.6.4"
    assert settings.serial_number == "ABB7005500E1"
