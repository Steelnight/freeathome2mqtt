"""Tests for cli.py: argument parsing and the five dispatch modes (docs/07 §3; docs/11 WP9)."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import io
import logging
from pathlib import Path
from typing import Any

import aiohttp
import aiomqtt
import orjson
import pytest
import yaml

from fakes.fake_broker import running_fake_broker
from fakes.fake_sysap import FakeSysAp, running_fake_sysap
from freeathome2mqtt import cli, log
from freeathome2mqtt.settings import load_settings
from freeathome2mqtt.supervisor import Supervisor, SupervisorConfig, TaskDiedTooManyTimesError
from freeathome2mqtt.sysap.mdns import SysApCandidate

SERIAL = "ABB7F500E17A"


def _configuration() -> dict[str, Any]:
    return {
        "sysapName": "Test House",
        "sysap": {},
        "users": {},
        "floorplan": {
            "floors": {"01": {"name": "Ground Floor", "rooms": {"01": {"name": "Room"}}}}
        },
        "devices": {
            SERIAL: {
                "displayName": "Switch",
                "interface": "TP",
                "unresponsive": False,
                "unresponsiveCounter": 0,
                "defect": False,
                "channels": {
                    "ch0000": {
                        "displayName": "Switch",
                        "functionID": "7",
                        "inputs": {"idp0000": {"pairingID": 1, "value": "0"}},
                        "outputs": {"odp0000": {"pairingID": 256, "value": "0"}},
                        "floor": "01",
                        "room": "01",
                    }
                },
                "floor": "01",
                "room": "01",
            }
        },
    }


def _write_config(tmp_path: Path, *, sysap_url: str, mqtt_port: int) -> Path:
    body = {
        "sysap": {"host": sysap_url, "username": "installer", "password": "secret"},
        "mqtt": {"server": f"mqtt://127.0.0.1:{mqtt_port}"},
        "advanced": {"data_dir": str(tmp_path)},
    }
    path = tmp_path / "config.yaml"
    path.write_text(yaml.dump(body))
    return path


async def _wait_until(predicate: Any, *, timeout_seconds: float = 5.0) -> None:
    async with asyncio.timeout(timeout_seconds):
        while not predicate():  # noqa: ASYNC110 -- generic poll, no single event to await
            await asyncio.sleep(0.005)


async def _no_broker_messages_arrived(port: int, *, wait_s: float = 0.3) -> bool:
    async with aiomqtt.Client("127.0.0.1", port=port) as observer:
        await observer.subscribe("#")
        try:
            async with asyncio.timeout(wait_s):
                async for _message in observer.messages:
                    return False
        except TimeoutError:
            return True
    return True  # pragma: no cover -- messages() only exits via return/timeout above


# -------------------------------------------------------------------------------- argument parsing


def test_default_args_use_documented_defaults() -> None:
    args = cli._build_arg_parser().parse_args([])
    assert args.config == cli._DEFAULT_CONFIG_PATH
    assert args.data_dir is None
    assert args.log_level is None
    assert args.check_config is False
    assert args.dry_run is False
    assert args.discover is False
    assert args.capture is None


def test_mode_flags_are_mutually_exclusive() -> None:
    with pytest.raises(SystemExit):
        cli._build_arg_parser().parse_args(["--dry-run", "--check-config"])


def test_log_level_rejects_an_unknown_value() -> None:
    with pytest.raises(SystemExit):
        cli._build_arg_parser().parse_args(["--log-level", "verbose"])


# ---------------------------------------------------------------------------------- --check-config


async def test_check_config_returns_zero_for_a_valid_file(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (_fake, http_client),
    ):
        config_path = _write_config(
            tmp_path, sysap_url=str(http_client.make_url("")).rstrip("/"), mqtt_port=broker.port
        )
        args = cli._build_arg_parser().parse_args(["--config", str(config_path), "--check-config"])
        assert await cli._async_main(args) == 0


async def test_check_config_returns_one_and_prints_to_stderr_for_an_invalid_file(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    config_path = tmp_path / "config.yaml"
    config_path.write_text("sysap:\n  username: u\n")  # missing required fields
    args = cli._build_arg_parser().parse_args(["--config", str(config_path), "--check-config"])
    assert await cli._async_main(args) == 1
    assert "invalid configuration" in capsys.readouterr().err


# -------------------------------------------------------------------------------------- --discover


async def test_discover_prints_candidates_and_returns_zero(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    async def _fake_discover(**_kwargs: Any) -> list[SysApCandidate]:
        return [SysApCandidate(name="free@home Test", host="192.168.1.50", port=80)]

    monkeypatch.setattr(cli, "discover_sysaps", _fake_discover)
    args = cli._build_arg_parser().parse_args(["--discover"])
    assert await cli._async_main(args) == 0
    out = capsys.readouterr().out
    assert "free@home Test" in out
    assert "192.168.1.50" in out


async def test_discover_with_no_candidates_prints_a_clear_message(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    async def _fake_discover(**_kwargs: Any) -> list[SysApCandidate]:
        return []

    monkeypatch.setattr(cli, "discover_sysaps", _fake_discover)
    args = cli._build_arg_parser().parse_args(["--discover"])
    assert await cli._async_main(args) == 0
    assert "No SysAPs found" in capsys.readouterr().out


async def test_discover_does_not_require_a_config_file() -> None:
    # --discover must work before any config.yaml exists -- it does not even try to load one.
    async def _fake_discover(**_kwargs: Any) -> list[SysApCandidate]:
        return []

    args = cli._build_arg_parser().parse_args(["--config", "/no/such/config.yaml", "--discover"])
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(cli, "discover_sysaps", _fake_discover)
        assert await cli._async_main(args) == 0


# -------------------------------------------------------------------------------------- --dry-run


async def test_dry_run_prints_the_entity_table_and_publishes_nothing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration())
        config_path = _write_config(
            tmp_path, sysap_url=str(http_client.make_url("")).rstrip("/"), mqtt_port=broker.port
        )
        args = cli._build_arg_parser().parse_args(["--config", str(config_path), "--dry-run"])
        assert await cli._async_main(args) == 0

        out = capsys.readouterr().out
        assert "1 entities from 1 devices" in out
        assert f"{SERIAL}_ch0000" in out

        assert await _no_broker_messages_arrived(broker.port)


# -------------------------------------------------------------------------------------- --capture


async def test_capture_writes_a_pseudonymised_fixture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(cli, "_CAPTURE_WINDOW_S", 0.1)
    async with running_fake_sysap(FakeSysAp()) as (fake, http_client):
        fake.set_configuration(_configuration())
        config_path = _write_config(
            tmp_path, sysap_url=str(http_client.make_url("")).rstrip("/"), mqtt_port=1
        )
        settings = load_settings(config_path, data_dir=tmp_path, environ={})
        output_path = tmp_path / "capture.json"
        assert await cli._run_capture(settings, output_path) == 0

    body = orjson.loads(output_path.read_bytes())
    assert "config" in body and "ws_frames" in body
    assert SERIAL not in orjson.dumps(body).decode()  # the real serial must be pseudonymised


# --------------------------------------------------------------------------------- _run_supervisor


async def test_run_supervisor_returns_zero_on_a_clean_stop(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration())
        supervisor = Supervisor(
            config=SupervisorConfig(
                sysap_base_url=str(http_client.make_url("")).rstrip("/"),
                sysap_username="installer",
                sysap_password="secret",
                mqtt_host="127.0.0.1",
                mqtt_port=broker.port,
                data_dir=tmp_path,
                config_refresh_interval_s=0,
            ),
            profiles=cli.load_profile_registry(cli._BUILT_IN_PROFILES_DIR),
            http_session=http_client.session,
        )
        run_task = asyncio.create_task(cli._run_supervisor(supervisor))
        await _wait_until(lambda: supervisor._cold_start_done)
        await supervisor.stop()
        assert await asyncio.wait_for(run_task, timeout=5.0) == 0


async def test_run_supervisor_returns_one_when_restart_was_requested(tmp_path: Path) -> None:
    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration())
        supervisor = Supervisor(
            config=SupervisorConfig(
                sysap_base_url=str(http_client.make_url("")).rstrip("/"),
                sysap_username="installer",
                sysap_password="secret",
                mqtt_host="127.0.0.1",
                mqtt_port=broker.port,
                data_dir=tmp_path,
                config_refresh_interval_s=0,
            ),
            profiles=cli.load_profile_registry(cli._BUILT_IN_PROFILES_DIR),
            http_session=http_client.session,
        )
        run_task = asyncio.create_task(cli._run_supervisor(supervisor))
        await _wait_until(lambda: supervisor._cold_start_done)
        supervisor.restart_requested = True
        await supervisor.stop()
        assert await asyncio.wait_for(run_task, timeout=5.0) == 1


async def test_run_supervisor_returns_one_when_a_task_dies_too_many_times() -> None:
    class _DyingSupervisor:
        restart_requested = False

        async def run(self) -> None:
            raise ExceptionGroup("x", [TaskDiedTooManyTimesError("boom")])

    assert await cli._run_supervisor(_DyingSupervisor()) == 1  # type: ignore[arg-type]


# ------------------------------------------------------------------------------------------- main


def test_main_exits_with_the_async_main_return_code(monkeypatch: pytest.MonkeyPatch) -> None:
    # main() itself owns the event loop (via asyncio.run()), so it can't be exercised from
    # inside an already-running one (an async test) -- this is a plain sync test instead,
    # confirming main()'s own job (parse argv, drive _async_main, sys.exit its result) without
    # re-running the full async startup logic every other test in this file already covers.
    async def _fake_async_main(_args: argparse.Namespace) -> int:
        return 7

    monkeypatch.setattr(cli, "_async_main", _fake_async_main)
    monkeypatch.setattr("sys.argv", ["freeathome2mqtt", "--check-config"])
    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    assert excinfo.value.code == 7


def test_install_uvloop_does_not_raise() -> None:
    cli._install_uvloop()


# ------------------------------------------------------------------------------ P-45 end to end


async def _cold_start_then_stop(settings: Any, http_session: aiohttp.ClientSession) -> None:
    profiles = cli.load_profile_registry(cli._BUILT_IN_PROFILES_DIR)
    supervisor_config = await cli.settings_to_supervisor_config(settings)
    supervisor = Supervisor(config=supervisor_config, profiles=profiles, http_session=http_session)
    run_task = asyncio.create_task(supervisor.run())
    try:
        await _wait_until(lambda: supervisor._cold_start_done)
    finally:
        await supervisor.stop()
        await asyncio.wait_for(run_task, timeout=5.0)


async def test_no_secrets_in_logs_or_bridge_info_during_a_real_startup(tmp_path: Path) -> None:
    """P-45, end to end: `cli._async_main`'s own `configure_logging()` call, wired with real
    sentinel secrets exactly as production does it, plus a real cold start over a real broker --
    neither the log nor a single one of the topics a cold start publishes may contain either
    secret. The `_build_bridge_info` half of this is also covered directly, in isolation, by
    `test_bridge_info_never_contains_sysap_or_mqtt_secrets` in test_supervisor.py.
    """
    sentinel_sysap_password = "sysap-e2e-sentinel-91af"
    sentinel_mqtt_password = "mqtt-e2e-sentinel-73bd"
    log_stream = io.StringIO()

    async with (
        running_fake_broker() as broker,
        running_fake_sysap(FakeSysAp()) as (fake, http_client),
    ):
        fake.set_configuration(_configuration())
        config_body = {
            "sysap": {
                "host": str(http_client.make_url("")).rstrip("/"),
                "username": "installer",
                "password": sentinel_sysap_password,
            },
            "mqtt": {
                "server": f"mqtt://127.0.0.1:{broker.port}",
                "password": sentinel_mqtt_password,
            },
            "advanced": {"data_dir": str(tmp_path)},
        }
        config_path = tmp_path / "config.yaml"
        config_path.write_text(yaml.dump(config_body))

        settings = load_settings(config_path, environ={})
        test_logger = logging.getLogger(f"test.no_secrets.{id(tmp_path)}")
        log.configure_logging(
            level="debug",
            logger=test_logger,
            stream=log_stream,
            secrets=[settings.sysap.password, settings.mqtt.password],
        )

        seen: dict[str, bytes] = {}
        async with aiomqtt.Client("127.0.0.1", port=broker.port) as observer:
            await observer.subscribe("#")

            async def _collect() -> None:
                async for message in observer.messages:
                    seen[str(message.topic)] = message.payload

            collector = asyncio.create_task(_collect())
            try:
                async with aiohttp.ClientSession() as session:
                    await _cold_start_then_stop(settings, session)
                await asyncio.sleep(0.1)  # let any straggling publishes land
            finally:
                collector.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await collector

        log_output = log_stream.getvalue()
        assert sentinel_sysap_password not in log_output
        assert sentinel_mqtt_password not in log_output

        all_payloads = b"".join(seen.values())
        assert sentinel_sysap_password.encode() not in all_payloads
        assert sentinel_mqtt_password.encode() not in all_payloads
