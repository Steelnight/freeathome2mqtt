"""Argument parsing, config discovery and ``uvloop`` install (docs/07 §3; docs/11 WP9).

`main()` is the process entry point (`python -m freeathome2mqtt`, the `freeathome2mqtt` console
script). It resolves `config.yaml` through `settings.py`, then dispatches to one of five modes:
`--check-config` (validate and exit), `--discover` (mDNS scan, needs no config file at all),
`--capture PATH` (record a pseudonymised fixture), `--dry-run` (connect, fetch, compile, print --
publish nothing, via `Supervisor.dry_run()`, which never constructs an `MqttClient`), or the
default: run the bridge until a signal or `bridge/request/restart` asks it to stop.

Not wired here, by design: `advanced.log_to_mqtt`. `configure_logging()` runs before any
`MqttClient` exists (secrets must be redacted from the log from the very first line), and
`Supervisor` does not yet expose a hook to attach a handler once its `MqttClient` connects and
detach it before shutdown. `log.MqttLogHandler` is fully implemented and tested standalone
(`tests/test_log.py`) -- this is the one real, named integration gap left for that hook to land in.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

import aiohttp

try:
    import uvloop
except ImportError:  # pragma: no cover -- exercised only on platforms without uvloop wheels
    uvloop = None  # type: ignore[assignment]

from freeathome2mqtt import log
from freeathome2mqtt.model.profiles import load_profile_registry
from freeathome2mqtt.settings import (
    Settings,
    SettingsError,
    build_sysap_ssl,
    load_settings,
    settings_to_supervisor_config,
)
from freeathome2mqtt.supervisor import Supervisor, TaskDiedTooManyTimesError
from freeathome2mqtt.sysap.mdns import discover_sysaps
from freeathome2mqtt.sysap.rest import RestClient, SysApError
from freeathome2mqtt.sysap.settings_probe import check_version_supported, fetch_settings
from freeathome2mqtt.sysap.ws import WsAuthenticationError, WsReader
from freeathome2mqtt.tools.capture import capture

if TYPE_CHECKING:
    from freeathome2mqtt.model.compiler import Model

logger = logging.getLogger(__name__)

_DEFAULT_CONFIG_PATH = Path("/config.yaml")
_BUILT_IN_PROFILES_DIR = Path(__file__).resolve().parent / "profiles"
_LOG_LEVEL_CHOICES = ("error", "warning", "info", "debug")
_CAPTURE_WINDOW_S = 30.0
_STOP_SIGNALS = (signal.SIGTERM, signal.SIGINT)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="freeathome2mqtt",
        description="A bridge between an ABB/Busch-Jaeger free@home SysAP and MQTT.",
    )
    parser.add_argument(
        "--config", type=Path, default=_DEFAULT_CONFIG_PATH, help="path to config.yaml"
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=None,
        help="override advanced.data_dir; also resolves !secret before config.yaml is parsed",
    )
    parser.add_argument(
        "--log-level", choices=_LOG_LEVEL_CHOICES, default=None, help="override advanced.log_level"
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check-config", action="store_true", help="validate config.yaml and exit")
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="connect, fetch, compile, print the entity table, publish nothing",
    )
    mode.add_argument(
        "--discover", action="store_true", help="mDNS-scan for SysAPs and print candidates"
    )
    mode.add_argument(
        "--capture", type=Path, default=None, metavar="PATH", help="record a pseudonymised fixture"
    )
    return parser


def _install_uvloop() -> None:
    if uvloop is not None:
        uvloop.install()


async def _run_discover() -> int:
    candidates = await discover_sysaps()
    if not candidates:
        print("No SysAPs found via mDNS.")
        return 0
    print(f"{'NAME':<32} {'HOST':<16} PORT")
    for candidate in candidates:
        print(f"{candidate.name:<32} {candidate.host:<16} {candidate.port}")
    return 0


def _print_entity_table(model: Model) -> None:
    print(f"{'ID':<32} {'NAME':<24} {'PROFILE':<24} TOPIC")
    for entity in model.entities:
        print(f"{entity.id:<32} {entity.name:<24} {entity.profile:<24} {entity.state_topic}")
    stats = model.stats
    print(
        f"\n{len(model.entities)} entities from {stats.devices_total} devices "
        f"({stats.channels_unsupported} unsupported, {stats.channels_orphaned} orphaned)."
    )


async def _run_dry_run(supervisor: Supervisor) -> int:
    model = await supervisor.dry_run()
    _print_entity_table(model)
    return 0


async def _run_capture(settings: Settings, output_path: Path) -> int:
    async with aiohttp.ClientSession() as session:
        sysap_settings = await fetch_settings(session, settings.sysap.host)
        check_version_supported(sysap_settings.version)
        ssl_context = await build_sysap_ssl(settings.sysap)
        rest = RestClient(
            base_url=settings.sysap.host,
            username=settings.sysap.username,
            password=settings.sysap.password,
            session=session,
            ssl=ssl_context,
            max_inflight=settings.sysap.max_inflight,
            request_timeout=settings.sysap.request_timeout,
        )

        # `pseudonymise_with_map`/`pseudonymise_ws_frame` both expect the raw per-SysAP-UUID
        # wrapped shape (docs/01 §3), but `RestClient`/`WsReader` both unwrap it for every other
        # caller -- fetching the configuration once upfront resolves `rest.sysap_uuid` so both
        # the config and every captured frame can be re-wrapped under the same key.
        config = await rest.get_configuration()
        if rest.sysap_uuid is None:
            raise SysApError("sysap_uuid not resolved after get_configuration()")
        sysap_uuid = rest.sysap_uuid

        frames: list[dict[str, Any]] = []
        ws = WsReader(
            url=f"{settings.sysap.host}/fhapi/v1/api/ws",
            username=settings.sysap.username,
            password=settings.sysap.password,
            session=session,
            ssl=ssl_context,
            on_frame=lambda body: frames.append({sysap_uuid: dict(body)}),
        )
        ws_task = asyncio.create_task(ws.run())
        logger.info("capturing for %.0fs ...", _CAPTURE_WINDOW_S)
        try:
            await asyncio.sleep(_CAPTURE_WINDOW_S)
        finally:
            await ws.stop()
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.wait_for(ws_task, timeout=5.0)

        async def _fetch_config() -> dict[str, Any]:
            return {sysap_uuid: config}

        async def _collect_frames() -> list[dict[str, Any]]:
            return frames

        await capture(
            fetch_config=_fetch_config, collect_ws_frames=_collect_frames, output_path=output_path
        )
    print(f"Wrote capture to {output_path}")
    return 0


async def _run_supervisor(supervisor: Supervisor) -> int:
    loop = asyncio.get_running_loop()
    for sig in _STOP_SIGNALS:
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, lambda: asyncio.ensure_future(supervisor.stop()))
    fatal = False
    try:
        await supervisor.run()
    except* TaskDiedTooManyTimesError as eg:
        fatal = True
        for exc in eg.exceptions:
            logger.error("supervisor task failed permanently: %s", exc)
    except* WsAuthenticationError as auth_eg:
        # docs/06 §3 / docs/01 §1.1: bad credentials, jid fallback already tried and failed too
        # (or the configured username has no jid to fall back to) -- fail loudly, never retry.
        fatal = True
        for auth_exc in auth_eg.exceptions:
            logger.error(
                "SysAP rejected the configured credentials (jid fallback also failed): %s. "
                "Check sysap.username/sysap.password.",
                auth_exc,
            )
    return 1 if fatal or supervisor.restart_requested else 0


async def _async_main(args: argparse.Namespace) -> int:
    if args.discover:
        return await _run_discover()

    try:
        settings = load_settings(args.config, data_dir=args.data_dir)
    except SettingsError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    log.configure_logging(
        level=args.log_level or settings.advanced.log_level,
        log_format=settings.advanced.log_format,
        secrets=[settings.sysap.password, settings.mqtt.password],
    )

    if args.check_config:
        logger.info("configuration OK: %s", args.config)
        return 0

    if args.capture is not None:
        return await _run_capture(settings, args.capture)

    profiles = load_profile_registry(_BUILT_IN_PROFILES_DIR, settings.advanced.profiles_dir)
    supervisor_config = await settings_to_supervisor_config(settings)

    async with aiohttp.ClientSession() as session:
        supervisor = Supervisor(config=supervisor_config, profiles=profiles, http_session=session)
        if args.dry_run:
            return await _run_dry_run(supervisor)
        return await _run_supervisor(supervisor)


def main(argv: list[str] | None = None) -> None:
    _install_uvloop()
    args = _build_arg_parser().parse_args(argv)
    exit_code = asyncio.run(_async_main(args))
    sys.exit(exit_code)
