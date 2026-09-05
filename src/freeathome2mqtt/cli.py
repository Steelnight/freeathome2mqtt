"""Argument parsing, config discovery and ``uvloop`` install (docs/07 §3; docs/11 WP9).

`main()` is the process entry point (`python -m freeathome2mqtt`, the `freeathome2mqtt` console
script). It resolves `config.yaml` through `settings.py`, then dispatches to one of six modes:
`--check-config` (validate and exit), `--discover` (mDNS scan, needs no config file at all),
`--capture PATH` (record a pseudonymised fixture), `--dry-run` (connect, fetch, compile, print --
publish nothing, via `Supervisor.dry_run()`, which never constructs an `MqttClient`), `--health`
(probe a *running* bridge's retained `bridge/state`; what the container `HEALTHCHECK` runs, WP18),
or the default: run the bridge until a signal or `bridge/request/restart` asks it to stop.

`advanced.log_to_mqtt` is no longer a gap here: `configure_logging()` still runs before any
`MqttClient` exists, because secrets must be redacted from the very first log line (P-45), and
WP16 gave `Supervisor` the hook this docstring used to ask for -- it attaches
`log.MqttLogHandler` once MQTT connects and detaches it before shutdown.
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
import aiomqtt
import orjson

try:
    import uvloop
except ImportError:  # pragma: no cover -- exercised only on platforms without uvloop wheels
    uvloop = None  # type: ignore[assignment]

from freeathome2mqtt import log
from freeathome2mqtt.model.profiles import load_profile_registry
from freeathome2mqtt.mqtt import topics
from freeathome2mqtt.settings import (
    Settings,
    SettingsError,
    build_sysap_ssl,
    load_settings,
    parse_mqtt_server,
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

_DEFAULT_HEALTH_TIMEOUT_S = 5.0
"""How long `--health` waits for the retained `bridge/state`. A retained message arrives on
subscribe, so this is a connection budget rather than a poll interval -- generous enough for a
loaded broker, short enough to stay inside a container healthcheck's own timeout.
"""


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
    mode.add_argument(
        "--health",
        action="store_true",
        help="probe a running bridge's retained bridge/state and exit 0 only if it is online",
    )
    parser.add_argument(
        "--health-timeout",
        type=float,
        default=_DEFAULT_HEALTH_TIMEOUT_S,
        help="seconds to wait for the retained bridge/state message (--health only)",
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


async def _run_health(settings: Settings, *, timeout_s: float) -> int:
    """`--health` (docs/07 §3): is the *running* bridge healthy? (docs/12 WP18.)

    Reads the retained `<base>/bridge/state` and succeeds only on `online`. That topic is
    end-to-end health, not broker connectivity (ADR-008): a bridge whose SysAP link has died
    publishes `offline` while still connected to MQTT, and that is exactly the state a
    healthcheck has to fail on.

    What this closes, precisely: the container HEALTHCHECK used to run `--check-config`, which
    parses a file and asks the running process nothing, so a hung-but-alive bridge passed it
    (docs/06 §6 F2). A *dead* process is already handled -- `TaskDiedTooManyTimesError` makes it
    exit non-zero and the restart policy takes over -- so this is the remaining case, not a
    replacement for that one.

    Every failure is a non-zero exit with a logged reason: an unreachable broker, no retained
    message within the timeout, an unparseable payload. A healthcheck that raised would be
    reported by the container runtime as a failure anyway, but without saying why.
    """
    host, port = parse_mqtt_server(settings.mqtt.server)
    topic = topics.bridge_state_topic(settings.mqtt.base_topic)
    try:
        async with (
            asyncio.timeout(timeout_s),
            aiomqtt.Client(
                host,
                port=port,
                username=settings.mqtt.user,
                password=settings.mqtt.password,
            ) as client,
        ):
            await client.subscribe(topic)
            async for message in client.messages:
                return _health_exit_code(message.payload, topic)
    except (aiomqtt.MqttError, OSError) as exc:
        logger.error("health check could not reach the broker at %s:%s: %s", host, port, exc)
        return 1
    except TimeoutError:
        logger.error(
            "health check timed out after %.1fs with no retained message on %s; "
            "no bridge has published its state to this broker",
            timeout_s,
            topic,
        )
        return 1
    return 1  # pragma: no cover -- messages() only exits via the return or an exception above


def _health_exit_code(payload: bytes | bytearray | str | None, topic: str) -> int:
    if not isinstance(payload, bytes | bytearray):
        logger.error("health check: %s carried a non-binary payload", topic)
        return 1
    try:
        body = orjson.loads(payload)
    except orjson.JSONDecodeError:
        logger.error("health check: %s is not valid JSON", topic)
        return 1
    state = body.get("state") if isinstance(body, dict) else None
    if state == "online":
        logger.info("health check: bridge is online")
        return 0
    logger.error("health check: bridge reports %r on %s", state, topic)
    return 1


async def _async_main(args: argparse.Namespace) -> int:
    """Load configuration, set logging up, then hand off to `_dispatch_mode`.

    Split in two because the modes outgrew one function's return budget when `--health` landed
    (CLAUDE.md §2 rule 4: that is a signal to split, not to silence the check). The division is a
    real one rather than cosmetic: everything here happens before any mode runs, and `--discover`
    is above it because it is the one mode that needs no config file at all.
    """
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
    return await _dispatch_mode(args, settings)


async def _dispatch_mode(args: argparse.Namespace, settings: Settings) -> int:
    """The config-dependent modes, in the order docs/07 §3 documents them."""
    if args.check_config:
        logger.info("configuration OK: %s", args.config)
        return 0

    if args.health:
        return await _run_health(settings, timeout_s=args.health_timeout)

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
