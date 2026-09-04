"""Task ownership, startup order, resync/reload, and shutdown (ADR-001; docs/02 §3, §7-8; docs/06
§3-4; docs/11 WP8-WP10).

`Supervisor` owns every long-lived task (docs/02 §3) inside one `asyncio.TaskGroup`, each wrapped
in `restart_on_failure` (docs/02 §3.1): catch, log, back off with jitter, restart -- escalating to
a fatal `TaskDiedTooManyTimesError` after five rapid failures in a row (P-29), which the caller of
`run()` is expected to treat as "exit the process, let the container restart it."

Startup follows docs/02 §7 precisely: probe, connect MQTT (LWT armed before anything risky can
fail, P-30), resolve SysAP credentials (docs/01 §1.1's `jid` fallback, tried once on a WS auth
failure before failing fatally per docs/06 §3 -- `_resolve_sysap_credentials`), open the SysAP
WebSocket and buffer *before* fetching the configuration (P-22), fetch,
compile and build Home Assistant discovery (`homeassistant/discovery.py`, WP10), drain the buffer
over the compiled state, retract any cross-restart-stale discovery topics (P-35), publish discovery
then state then `bridge/devices` then `bridge/state: online`. Resync (docs/06 §4) reuses the same
buffer-then-fetch-then-drain shape on every WebSocket reconnect, topology change (P-13), or
`bridge/request/reload`, publishing only the entities that actually changed (P-23) -- never
re-publishing everything on every blip; discovery uses the same changed-only path, backed by
`persistence.DiscoveryStore` (docs/07 §4.2).

Per-entity `entity/options` overrides are wired end to end: `optimistic`/`debounce_ms` into
`CommandDispatcher` via `_entity_optimistic_overrides`/`_entity_debounce_overrides`, and
`homeassistant` into `build_model_discovery` via `_entity_discovery_overrides` -- each shallow-
merged onto its entity's auto-built discovery payload (`homeassistant/discovery.py`). All three
only take hold on the next `_rebuild_dependents`/`_compile_and_build_discovery` call, i.e. a
resync -- see `_handle_entity_options`'s own docstring for why setting any of them triggers one.

Not yet wired here, by design: a 404-on-write-triggers-resync hook (docs/06 §4.1's last row -- a
real gap, deferred rather than bolted on without an acceptance test to pin its shape down).
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import hashlib
import importlib.metadata
import logging
import random
import ssl as ssl_module
import time
from collections.abc import Awaitable, Callable, Coroutine, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp
import aiomqtt
import orjson

from freeathome2mqtt import log
from freeathome2mqtt.availability import BridgeAvailability, DeviceAvailabilityPublisher
from freeathome2mqtt.bus.commands import CommandDispatcher
from freeathome2mqtt.bus.events import EventPublisher
from freeathome2mqtt.bus.ingress import Ingress
from freeathome2mqtt.bus.publisher import Publisher
from freeathome2mqtt.bus.raw import RawCommandHandler, RawMode, RawStatePublisher, build_raw_map
from freeathome2mqtt.bus.reconcile import RateLimiter, Reconciler
from freeathome2mqtt.bus.state import StateStore
from freeathome2mqtt.homeassistant.components import DiscoveryOptions
from freeathome2mqtt.homeassistant.discovery import DiscoveryPublisher, build_model_discovery
from freeathome2mqtt.metrics import Metrics
from freeathome2mqtt.metrics_server import MetricsServer
from freeathome2mqtt.model.compiler import CompileOptions, Model
from freeathome2mqtt.model.compiler import compile as compile_model
from freeathome2mqtt.model.entity import AttrKind, Entity
from freeathome2mqtt.model.profiles import Profile, ProfileRegistry
from freeathome2mqtt.mqtt import topics
from freeathome2mqtt.mqtt.bridge_api import BridgeApi, BridgeApiError, Handler
from freeathome2mqtt.mqtt.client import MqttClient, MqttClientNotConnectedError
from freeathome2mqtt.persistence import DiscoveryStore, EntitiesStore
from freeathome2mqtt.sysap.codes import Function
from freeathome2mqtt.sysap.rest import (
    AuthenticationError,
    BadRequestError,
    ForbiddenError,
    NotFoundError,
    RestClient,
    SysApError,
)
from freeathome2mqtt.sysap.schema import (
    Channel,
    Configuration,
    Device,
    WsFrameBody,
    parse_function_id,
)
from freeathome2mqtt.sysap.settings_probe import (
    SysApSettings,
    check_version_supported,
    fetch_settings,
    find_jid,
)
from freeathome2mqtt.sysap.ws import WsAuthenticationError, WsReader

logger = logging.getLogger(__name__)

_RESTART_BACKOFF_INITIAL = 1.0
_RESTART_BACKOFF_FACTOR = 2.0
_RESTART_BACKOFF_CAP = 60.0
_ESCALATION_THRESHOLD = 5

# docs/06 §3: "Auth failure -> Immediately. Do not retry." A supervised task failing with one of
# these must propagate straight away, the same way CancelledError already does, rather than being
# treated as a routine crash to restart with backoff (retrying bad credentials is P-20's own
# "trips the SysAP's lockout" pitfall, just reached through the supervised-task path instead of
# RestClient's).
_NEVER_RESTARTED_EXCEPTIONS = (AuthenticationError, WsAuthenticationError)
_RAPID_FAILURE_WINDOW_S = 10.0

_RELOAD_DEBOUNCE_S = 2.0
_RELOAD_MIN_INTERVAL_S = 30.0

_CONFIG_FETCH_BACKOFF_INITIAL = 1.0
_CONFIG_FETCH_BACKOFF_CAP = 30.0

_STARTUP_POLL_INTERVAL_S = 0.01

_TOPOLOGY_KEYS = ("devices", "devicesAdded", "devicesRemoved", "parameters")

# docs/01 §4.5: a virtual device with one of these TTLs never expires, so it needs no keepalive.
_VIRTUAL_DEVICE_NO_EXPIRY = frozenset({-1, 0})
_VIRTUAL_DEVICE_RESERVED_KEYS = frozenset({"serial", "type", "ttl", "transaction"})

# Named tuples, not inline tuple literals, per sysap/rest.py's own convention: Python 3.14's
# grammar allows `except A, B:` without parentheses (PEP 758), which `ruff format` then prefers --
# but a bare comma there reads exactly like the dead Python 2 idiom. A single name sidesteps it.
_NEVER_RETRIED_CONFIG_FETCH_ERRORS = (AuthenticationError, ForbiddenError, BadRequestError)
_RETRYABLE_CONFIG_FETCH_ERRORS = (SysApError, aiohttp.ClientError, TimeoutError)


class TaskDiedTooManyTimesError(Exception):
    """A supervised task failed `_ESCALATION_THRESHOLD` times in a row (docs/02 §3.1; P-29)."""


def _backoff_delay(attempt: int, *, initial: float, factor: float, cap: float) -> float:
    """Full jitter (docs/06 §3): ``sleep = random(0, min(cap, initial * factor**(attempt-1)))``."""
    ceiling = min(cap, initial * factor ** (attempt - 1))
    return random.uniform(0, ceiling)  # noqa: S311 -- timing jitter, not a cryptographic use


def _hash_config(config: Configuration) -> bytes:
    return hashlib.sha256(orjson.dumps(config, option=orjson.OPT_SORT_KEYS)).digest()


def _package_version() -> str:
    try:
        return importlib.metadata.version("freeathome2mqtt")
    except importlib.metadata.PackageNotFoundError:
        return "0.0.0-dev"


def _connected_or_not(connected: bool) -> str:
    return "connected" if connected else "disconnected"


# -------------------------------------------------------------------- bridge/devices (docs/04 §4.3)
#
# Pure, module-level functions (no `self`) -- independently testable, and a natural pairing with
# `_hash_config` above. `_bd_unsupported_reason` is a deliberate simplification: it distinguishes
# "orphaned" (no floor/room), "unknown function ID", and a single generic "no profile claims this
# function" bucket, rather than reconstructing `model/compiler.py`'s private specificity/tie-break
# logic to report exactly *which* profiles almost matched -- the same kind of documented
# simplification `_handle_device_refresh`'s docstring already accepts for a different corner.


def _bd_resolve_floorplan(config: Configuration) -> dict[str, dict[str, str]]:
    floors = config.get("floorplan", {}).get("floors", {})
    result: dict[str, dict[str, str]] = {}
    for floor_id, floor in floors.items():
        rooms = floor.get("rooms") or {}  # P-14: rooms may be null, not just absent
        result[floor_id] = {room_id: room.get("name", "") for room_id, room in rooms.items()}
    return result


def _bd_area(floorplan: Mapping[str, Mapping[str, str]], floor_id: str, room_id: str) -> str | None:
    return floorplan.get(floor_id, {}).get(room_id)


def _bd_device_area(floorplan: Mapping[str, Mapping[str, str]], device: Device) -> str | None:
    floor_id, room_id = device.get("floor"), device.get("room")
    if floor_id is None or room_id is None:
        return None
    return _bd_area(floorplan, floor_id, room_id)


def _bd_function_fields(channel: Channel) -> tuple[str | None, str | None]:
    raw = channel.get("functionID")
    function_id = parse_function_id(raw)
    if function_id is None:
        return raw, None
    try:
        return f"0x{function_id:X}", Function(function_id).name
    except ValueError:
        return f"0x{function_id:X}", None


def _bd_unsupported_reason(channel: Channel, device: Device) -> str:
    floor_id = channel.get("floor") or device.get("floor")
    room_id = channel.get("room") or device.get("room")
    if floor_id is None or room_id is None:
        return "no floor/room assigned (orphaned channel)"
    function_id = parse_function_id(channel.get("functionID"))
    if function_id is None:
        return "unrecognised functionID"
    try:
        Function(function_id)
    except ValueError:
        return "unknown function ID"
    return "no profile claims this function"


def _bd_attribute_entries(entity: Entity, profile: Profile) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for name, kind in zip(entity.attr_names, entity.attr_kinds, strict=True):
        spec = profile.attributes[name]
        entry: dict[str, Any] = {
            "name": name,
            "codec": spec.codec,
            "kind": "event" if kind == AttrKind.EVENT else "state",
        }
        if spec.unit:
            entry["unit"] = spec.unit
        entries.append(entry)
    return entries


def _bd_command_entries(profile: Profile, command_names: Sequence[str]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for name in command_names:
        spec = profile.commands[name]
        entry: dict[str, Any] = {"name": name, "continuous": spec.continuous}
        if spec.range is not None:
            entry["range"] = list(spec.range)
        entries.append(entry)
    return entries


def _bd_supported_channel_entry(
    channel_id: str,
    channel: Channel,
    entity: Entity,
    profile: Profile,
    command_names: Sequence[str],
) -> dict[str, Any]:
    function_id, function_name = _bd_function_fields(channel)
    return {
        "channel_id": channel_id,
        "entity_id": entity.id,
        "topic": entity.state_topic,
        "name": entity.name,
        "area": entity.area,
        "function_id": function_id,
        "function": function_name,
        "profile": entity.profile,
        "supported": True,
        "attributes": _bd_attribute_entries(entity, profile),
        "commands": _bd_command_entries(profile, command_names),
    }


def _bd_unsupported_channel_entry(
    channel_id: str, channel: Channel, device: Device
) -> dict[str, Any]:
    function_id, function_name = _bd_function_fields(channel)
    return {
        "channel_id": channel_id,
        "function_id": function_id,
        "function": function_name,
        "supported": False,
        "reason": _bd_unsupported_reason(channel, device),
    }


def _bd_device_channels(
    serial: str,
    device: Device,
    model: Model,
    profiles: ProfileRegistry,
    commands_by_entity: Mapping[int, set[str]],
) -> list[dict[str, Any]]:
    channels_out: list[dict[str, Any]] = []
    for channel_id in sorted(device.get("channels", {})):
        channel = device["channels"][channel_id]
        entity_idx = model.by_id.get(f"{serial}_{channel_id}")
        if entity_idx is None:
            channels_out.append(_bd_unsupported_channel_entry(channel_id, channel, device))
            continue
        entity = model.entities[entity_idx]
        profile = profiles[entity.profile]
        command_names = [
            name for name in profile.commands if name in commands_by_entity.get(entity_idx, ())
        ]
        channels_out.append(
            _bd_supported_channel_entry(channel_id, channel, entity, profile, command_names)
        )
    return channels_out


def _build_bridge_devices(
    config: Configuration, model: Model, profiles: ProfileRegistry
) -> list[dict[str, Any]]:
    """docs/04 §4.3: every device, its channels, which profile matched, and which did not."""
    devices_raw = config.get("devices", {})
    floorplan = _bd_resolve_floorplan(config)
    commands_by_entity: dict[int, set[str]] = {}
    for entity_idx, command_name in model.egress:
        commands_by_entity.setdefault(entity_idx, set()).add(command_name)

    result: list[dict[str, Any]] = []
    for serial in sorted(devices_raw):
        device = devices_raw[serial]
        result.append(
            {
                "serial": serial,
                "name": device.get("displayName", serial),
                "device_id": device.get("deviceId"),
                "article_number": device.get("articleNumber"),
                "interface": device.get("interface"),
                "area": _bd_device_area(floorplan, device),
                "unresponsive": device.get("unresponsive", False),
                "defect": device.get("defect", False),
                "channels": _bd_device_channels(
                    serial, device, model, profiles, commands_by_entity
                ),
            }
        )
    return result


def _split_devices_payload(devices: Sequence[dict[str, Any]], max_size: int) -> list[bytes]:
    """P-41: split into parts that each fit `max_size`, greedily. A single device whose own
    entry already exceeds `max_size` still gets its own, oversized part -- there is no smaller
    unit to split it into, so this is the best effort possible rather than a silent failure.
    """
    parts: list[bytes] = []
    current: list[dict[str, Any]] = []
    for device in devices:
        candidate = [*current, device]
        payload = orjson.dumps(candidate)
        if len(payload) > max_size and current:
            parts.append(orjson.dumps(current))
            current = [device]
        else:
            current = candidate
    parts.append(orjson.dumps(current))
    return parts


async def _wait_until(predicate: Callable[[], bool]) -> None:
    """Poll with no timeout: docs/06 §7 says startup must not give up while a link is booting."""
    while not predicate():  # noqa: ASYNC110 -- indefinite by design, not a bounded condition wait
        await asyncio.sleep(_STARTUP_POLL_INTERVAL_S)


async def restart_on_failure(
    name: str,
    factory: Callable[[], Awaitable[None]],
    *,
    metrics: Metrics,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> None:
    """Run `factory()` forever, restarting with backoff+jitter on any exception (docs/02 §3.1) --
    except `asyncio.CancelledError` and `_NEVER_RESTARTED_EXCEPTIONS` (docs/06 §3's auth-failure
    rule), both of which propagate immediately instead.

    A task that returns normally is done, not failed -- only the exception path restarts. Escalates
    by raising `TaskDiedTooManyTimesError` once `_ESCALATION_THRESHOLD` failures have each started
    within `_RAPID_FAILURE_WINDOW_S` of the previous one; a task that ran a good while before
    failing resets the streak, since that is not the "dies immediately" pattern P-29 describes.

    `clock`/`sleep` are injectable, like every other timer in this codebase (`RateLimiter`,
    `Reconciler`, ...), so a test can prove the streak-reset logic without real backoff delays.
    """
    attempt = 0
    while True:
        start = clock()
        try:
            await factory()
            return
        except asyncio.CancelledError:
            raise
        except _NEVER_RESTARTED_EXCEPTIONS:
            raise
        except Exception:
            logger.exception("supervised task %r failed", name)
            metrics.task_restarts += 1
            attempt = attempt + 1 if (clock() - start) < _RAPID_FAILURE_WINDOW_S else 1
            if attempt >= _ESCALATION_THRESHOLD:
                raise TaskDiedTooManyTimesError(name) from None
            await sleep(
                _backoff_delay(
                    attempt,
                    initial=_RESTART_BACKOFF_INITIAL,
                    factor=_RESTART_BACKOFF_FACTOR,
                    cap=_RESTART_BACKOFF_CAP,
                )
            )


class _ReloadDebouncer:
    """P-55: 2 s debounce + 30 s minimum interval between actual resyncs, collapsing repeated
    requests into a single "reload again" flag instead of queueing them (docs/05 §4.3).
    """

    def __init__(
        self,
        *,
        resync: Callable[[], Awaitable[None]],
        debounce_s: float = _RELOAD_DEBOUNCE_S,
        min_interval_s: float = _RELOAD_MIN_INTERVAL_S,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._resync = resync
        self._debounce_s = debounce_s
        self._min_interval_s = min_interval_s
        self._clock = clock
        self._timer: asyncio.Task[None] | None = None
        self._last_resync_at: float | None = None
        self._reload_again = False
        self.reload_count = 0

    def request(self) -> None:
        if self._timer is not None:
            self._reload_again = True
            return
        self._timer = asyncio.create_task(self._wait_then_resync())

    async def _wait_then_resync(self) -> None:
        await asyncio.sleep(self._debounce_s)
        if self._last_resync_at is not None:
            since = self._clock() - self._last_resync_at
            if since < self._min_interval_s:
                await asyncio.sleep(self._min_interval_s - since)
        self._timer = None
        # Captured *before* the reset below, and checked against this local afterward -- not
        # `self._reload_again` again, which reflects only what happened during `_resync()`'s own
        # await, having already lost whatever `request()` set during the debounce/interval wait.
        reload_again = self._reload_again
        self._reload_again = False
        self._last_resync_at = self._clock()
        self.reload_count += 1
        await self._resync()
        if reload_again:
            self.request()


@dataclass(frozen=True, slots=True)
class SupervisorConfig:
    """Everything `Supervisor` needs that isn't produced at runtime.

    Deliberately a plain, WP8-local dataclass rather than `settings.py`'s pydantic model (WP9) --
    that module will translate a validated `config.yaml` into one of these, not the other way
    round.
    """

    sysap_base_url: str
    sysap_username: str
    sysap_password: str
    sysap_ssl: ssl_module.SSLContext | bool = True
    sysap_max_inflight: int = 4
    sysap_request_timeout_s: float = 10.0
    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_client_id: str | None = None
    mqtt_username: str | None = None
    mqtt_password: str | None = None
    mqtt_tls: ssl_module.SSLContext | None = None
    mqtt_keepalive: int = 60
    mqtt_qos_state: int = 0
    mqtt_qos_discovery: int = 1
    mqtt_force_disable_retain: bool = False
    base_topic: str = "freeathome2mqtt"
    compile_options: CompileOptions = field(default_factory=CompileOptions)
    data_dir: Path = field(default_factory=lambda: Path("/data"))
    coalesce_ms: int = 20
    publish_last_changed: bool = True
    command_debounce_s: float = 0.05
    default_optimistic: bool = True
    reconcile_delay_s: float = 3.0
    get_rate_limit_s: float = 5.0
    grace_seconds: float = 10.0
    availability_enabled: bool = True
    availability_per_device: bool = True
    config_refresh_interval_s: float = 300.0
    reload_debounce_s: float = _RELOAD_DEBOUNCE_S
    reload_min_interval_s: float = _RELOAD_MIN_INTERVAL_S
    link_backoff_initial: float = 1.0
    link_backoff_factor: float = 2.0
    link_backoff_cap: float = 60.0
    ws_heartbeat_s: float | None = 30.0
    ws_idle_timeout_s: float = 90.0
    homeassistant_enabled: bool = True
    homeassistant_discovery_topic: str = "homeassistant"
    homeassistant_status_topic: str = "homeassistant/status"
    homeassistant_republish_delay_s: float = 5.0
    mqtt_maximum_packet_size: int = 1048576
    raw_mode: RawMode = False
    metrics_enabled: bool = False
    metrics_port: int = 9102


class Supervisor:
    """Owns the process lifecycle: startup, the long-lived tasks, resync/reload, and shutdown."""

    def __init__(
        self,
        *,
        config: SupervisorConfig,
        profiles: ProfileRegistry,
        http_session: aiohttp.ClientSession,
    ) -> None:
        self._config = config
        self._profiles = profiles
        self._http_session = http_session

        self.metrics = Metrics()
        self._entities_store = EntitiesStore(config.data_dir / "entities.json")
        self._discovery_store = DiscoveryStore(config.data_dir / "discovery.json")

        self._model: Model | None = None
        self._state: StateStore | None = None
        self._ingress: Ingress | None = None
        self._events: EventPublisher | None = None
        self._publisher: Publisher | None = None
        self._rate_limiter: RateLimiter | None = None
        self._reconciler: Reconciler | None = None
        self._commands: CommandDispatcher | None = None
        self._raw_commands: RawCommandHandler | None = None
        self._device_availability: DeviceAvailabilityPublisher | None = None
        self._discovery_publisher: DiscoveryPublisher | None = None

        self._mqtt: MqttClient | None = None
        self._metrics_server: MetricsServer | None = None
        self._rest: RestClient | None = None
        self._ws: WsReader | None = None
        self._availability: BridgeAvailability | None = None
        self._bridge_api: BridgeApi | None = None
        self.restart_requested = False
        self._sysap_settings: SysApSettings | None = None
        self._started_at: float | None = None
        self._virtual_device_tasks: dict[str, asyncio.Task[None]] = {}

        self._cold_start_done = False
        self._last_config_hash: bytes | None = None
        self._reload_debouncer = _ReloadDebouncer(
            resync=self._resync,
            debounce_s=config.reload_debounce_s,
            min_interval_s=config.reload_min_interval_s,
        )

        self._tg: asyncio.TaskGroup | None = None
        self._publisher_task: asyncio.Task[None] | None = None
        self._config_refresh_task: asyncio.Task[None] | None = None
        self._background_tasks: set[asyncio.Task[None]] = set()

        self._shutdown_event = asyncio.Event()

    # ------------------------------------------------------------------------------ public API

    async def run(self) -> None:
        """Start up, run every supervised task, and stop cleanly once `stop()` is called."""
        async with asyncio.TaskGroup() as tg:
            self._tg = tg
            await self._startup()
            await self._shutdown_event.wait()
            await self._graceful_shutdown()

    async def stop(self) -> None:
        """Request a graceful shutdown (docs/08 §10); `run()` returns once it completes."""
        self._shutdown_event.set()

    async def dry_run(self) -> Model:
        """docs/07 §3 `--dry-run`: probe, fetch, compile -- and nothing else. Never constructs
        `MqttClient` or `WsReader`, so "publishes nothing" is guaranteed by construction rather
        than by a flag threaded through the normal startup path.
        """
        self._entities_store.load()
        settings = await fetch_settings(self._http_session, self._config.sysap_base_url)
        check_version_supported(settings.version)
        self._sysap_settings = settings

        self._rest = self._build_rest_client()
        try:
            config = await self._fetch_configuration_with_retry()
        except AuthenticationError:
            # docs/01 §1.1 / F4: no WsReader exists here to probe cheaply (dry_run never
            # constructs one, by design), and this is a one-shot diagnostic command rather than
            # a hot startup path, so retrying the full configuration fetch once with the jid is
            # an acceptable cost -- unlike `_resolve_sysap_credentials`'s WS-probe-first approach.
            jid = find_jid(settings.users, self._config.sysap_username)
            if jid is None:
                raise
            logger.info(
                "SysAP rejected username %r; retrying once with the jid fallback (docs/01 §1.1)",
                self._config.sysap_username,
            )
            self._rest = self._build_rest_client(username=jid)
            config = await self._fetch_configuration_with_retry()

        model = self._compile_and_build_discovery(config)
        self._model = model
        return model

    def _spawn_supervised(
        self, name: str, factory: Callable[[], Awaitable[None]]
    ) -> asyncio.Task[None]:
        if self._tg is None:
            raise RuntimeError("_spawn_supervised called outside of run()")
        return self._tg.create_task(restart_on_failure(name, factory, metrics=self.metrics))

    def _spawn_background(self, coro: Coroutine[Any, Any, None], *, name: str) -> None:
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(lambda t: self._log_background_result(t, name))

    def _log_background_result(self, task: asyncio.Task[None], name: str) -> None:
        self._background_tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error("background task %r failed: %s", name, exc, exc_info=exc)

    # --------------------------------------------------------------------------------- startup

    async def _startup(self) -> None:
        self._started_at = time.monotonic()
        self._entities_store.load()
        self._discovery_store.load()

        settings = await fetch_settings(self._http_session, self._config.sysap_base_url)
        check_version_supported(settings.version)
        self._sysap_settings = settings

        mqtt = self._build_mqtt_client(settings)
        self._mqtt = mqtt
        availability, device_availability = self._build_mqtt_dependents(mqtt)

        self._spawn_supervised("mqtt_client", mqtt.run)
        await _wait_until(lambda: mqtt.reconnect_count >= 1)
        self._start_metrics_server_if_enabled()

        rest, ws = await self._resolve_sysap_credentials(settings)
        self._rest = rest
        self._ws = ws
        ws.start_buffering()  # armed before the very first connect too (docs/02 §7)
        self._spawn_supervised("ws_reader", ws.run)
        await _wait_until(lambda: ws.reconnect_count >= 1)
        availability.set_sysap_connected(True)

        config = await self._fetch_configuration_with_retry()
        new_model = self._compile_and_build_discovery(config)
        self._last_config_hash = _hash_config(config)
        state = self._seed_state(new_model)
        self._rebuild_dependents(model=new_model, state=state, mqtt=mqtt, rest=rest, config=config)

        for body in ws.drain_buffer():
            self._ingress_or_raise().process_frame(body)

        # A restart may find topics `discovery.json` remembers publishing in a *previous* run
        # that this run's model no longer has (P-35's cross-restart case) -- only meaningful on
        # this very first compile, since the in-memory diff `_diff_and_apply` does on every later
        # resync already catches everything removed while this process is running.
        await self._retract_stale_discovery(new_model)
        await self._publish_discovery(new_model)
        state.dirty.update(range(len(new_model.entities)))
        await self._publisher_or_raise().flush()
        await device_availability.publish(new_model.entities, config.get("devices", {}))
        await self._publish_bridge_devices(config, new_model)

        availability.set_model_loaded(True)
        await availability.publish_now()

        self._cold_start_done = True
        self._publisher_task = self._spawn_supervised("publisher", self._publisher_or_raise().run)
        self._config_refresh_task = self._spawn_supervised(
            "config_refresher", self._config_refresh_loop
        )

    def _build_mqtt_client(self, settings: SysApSettings) -> MqttClient:
        return MqttClient(
            host=self._config.mqtt_host,
            port=self._config.mqtt_port,
            base_topic=self._config.base_topic,
            sysap_serial=settings.serial_number,
            client_id=self._config.mqtt_client_id,
            username=self._config.mqtt_username,
            password=self._config.mqtt_password,
            tls_context=self._config.mqtt_tls,
            keepalive=self._config.mqtt_keepalive,
            homeassistant_discovery_topic=(
                self._config.homeassistant_discovery_topic
                if self._config.homeassistant_enabled
                else None
            ),
            raw_mode_enabled=self._config.raw_mode is not False,
            force_disable_retain=self._config.mqtt_force_disable_retain,
            backoff_initial=self._config.link_backoff_initial,
            backoff_factor=self._config.link_backoff_factor,
            backoff_cap=self._config.link_backoff_cap,
            on_message=self._on_mqtt_message,
            on_reconnected=self._on_mqtt_reconnected,
            on_disconnected=self._on_mqtt_disconnected,
        )

    def _build_mqtt_dependents(
        self, mqtt: MqttClient
    ) -> tuple[BridgeAvailability, DeviceAvailabilityPublisher]:
        # Handlers are bound methods closing over `self`, so they always see whatever `_model`/
        # `_rest`/etc. currently are -- constructed once here, never rebuilt on resync, unlike
        # `_commands` below (docs/04 §5).
        self._bridge_api = BridgeApi(
            base_topic=self._config.base_topic, mqtt=mqtt, handlers=self._bridge_api_handlers()
        )
        availability = BridgeAvailability(
            mqtt=mqtt, base_topic=self._config.base_topic, grace_seconds=self._config.grace_seconds
        )
        self._availability = availability
        device_availability = DeviceAvailabilityPublisher(
            mqtt=mqtt,
            enabled=self._config.availability_enabled and self._config.availability_per_device,
        )
        self._device_availability = device_availability
        self._discovery_publisher = DiscoveryPublisher(
            mqtt=mqtt, store=self._discovery_store, qos=self._config.mqtt_qos_discovery
        )
        return availability, device_availability

    async def _resolve_sysap_credentials(
        self, settings: SysApSettings
    ) -> tuple[RestClient, WsReader]:
        """docs/01 §1.1 / F4: try the configured username first; on a WS handshake auth failure,
        retry once with the `jid` looked up from `settings.json`'s `users[]` before letting the
        bridge fail fatally (docs/06 §3: "Auth failure -> Immediately. Do not retry."). Probed
        over WS (`WsReader.connect_once`), not a REST config fetch, so this costs one cheap
        handshake rather than a second full configuration fetch (ADR-007) -- and must happen
        before `ws_reader` is spawned as a supervised, never-gives-up task (docs/06 §3's SysAP-
        booting case): a bad username there would otherwise hang startup forever waiting for a
        connection that can never succeed.
        """
        rest = self._build_rest_client()
        ws = self._build_ws_reader()
        try:
            await ws.connect_once()
        except WsAuthenticationError:
            jid = find_jid(settings.users, self._config.sysap_username)
            if jid is None:
                raise
            logger.info(
                "SysAP rejected username %r; retrying once with the jid fallback (docs/01 §1.1)",
                self._config.sysap_username,
            )
            rest = self._build_rest_client(username=jid)
            ws = self._build_ws_reader(username=jid)
            await ws.connect_once()  # a second failure propagates fatally, per docs/06 §3
        return rest, ws

    def _build_rest_client(self, *, username: str | None = None) -> RestClient:
        return RestClient(
            base_url=self._config.sysap_base_url,
            username=username if username is not None else self._config.sysap_username,
            password=self._config.sysap_password,
            session=self._http_session,
            ssl=self._config.sysap_ssl,
            max_inflight=self._config.sysap_max_inflight,
            request_timeout=self._config.sysap_request_timeout_s,
        )

    def _build_ws_reader(self, *, username: str | None = None) -> WsReader:
        return WsReader(
            url=f"{self._config.sysap_base_url}/fhapi/v1/api/ws",
            username=username if username is not None else self._config.sysap_username,
            password=self._config.sysap_password,
            session=self._http_session,
            ssl=self._config.sysap_ssl,
            on_frame=self._on_ws_frame,
            on_connected=self._on_ws_connected,
            heartbeat=self._config.ws_heartbeat_s,
            idle_timeout=self._config.ws_idle_timeout_s,
            backoff_initial=self._config.link_backoff_initial,
            backoff_factor=self._config.link_backoff_factor,
            backoff_cap=self._config.link_backoff_cap,
        )

    def _start_metrics_server_if_enabled(self) -> None:
        """`advanced.metrics.enabled` (docs/00 §5, docs/11 WP12) -- optional, off by default."""
        if not self._config.metrics_enabled:
            return
        metrics_server = MetricsServer(metrics=self.metrics, port=self._config.metrics_port)
        self._metrics_server = metrics_server
        self._spawn_supervised("metrics_server", metrics_server.run)

    def _ingress_or_raise(self) -> Ingress:
        if self._ingress is None:
            raise RuntimeError("ingress used before startup completed")
        return self._ingress

    def _publisher_or_raise(self) -> Publisher:
        if self._publisher is None:
            raise RuntimeError("publisher used before startup completed")
        return self._publisher

    def _seed_state(self, model: Model) -> StateStore:
        state = StateStore(model.entities)
        for idx, values in enumerate(model.initial_values):
            for attr_idx, value in enumerate(values):
                state.seed(idx, attr_idx, value)
        return state

    def _effective_compile_options(self) -> CompileOptions:
        aliases = {
            entity_id: record.alias
            for entity_id, record in self._entities_store.entities.items()
            if record.alias is not None
        }
        excluded_entity_ids = frozenset(
            entity_id
            for entity_id, record in self._entities_store.entities.items()
            if record.options.get("enabled") is False
        )
        return dataclasses.replace(
            self._config.compile_options,
            topic_prefix=self._config.base_topic,
            aliases=aliases,
            excluded_entity_ids=excluded_entity_ids,
        )

    def _entity_optimistic_overrides(self, model: Model) -> dict[int, bool]:
        """docs/04 §5 `entity/options {"optimistic": ...}`: keyed by idx in *this* model, since
        `entities.json` itself is keyed by the stable entity id, not a compile-specific index.
        """
        overrides: dict[int, bool] = {}
        for entity_id, record in self._entities_store.entities.items():
            value = record.options.get("optimistic")
            idx = model.by_id.get(entity_id)
            if isinstance(value, bool) and idx is not None:
                overrides[idx] = value
        return overrides

    def _entity_debounce_overrides(self, model: Model) -> dict[int, float]:
        """docs/04 §5 `entity/options {"debounce_ms": ...}`, converted to seconds to match
        `CommandDispatcher`'s own unit.
        """
        overrides: dict[int, float] = {}
        for entity_id, record in self._entities_store.entities.items():
            value = record.options.get("debounce_ms")
            idx = model.by_id.get(entity_id)
            if isinstance(value, int | float) and not isinstance(value, bool) and idx is not None:
                overrides[idx] = value / 1000
        return overrides

    def _discovery_options(self) -> DiscoveryOptions:
        settings = self._sysap_settings
        return DiscoveryOptions(
            enabled=self._config.homeassistant_enabled,
            discovery_topic=self._config.homeassistant_discovery_topic,
            base_topic=self._config.base_topic,
            sysap_serial=settings.serial_number if settings is not None else "",
            bridge_version=_package_version(),
        )

    def _entity_discovery_overrides(self) -> dict[str, dict[str, Any]]:
        """`entity_id -> {"homeassistant": {...}}`'s inner dict (docs/04 §5's `entity/options`),
        round-tripped through `entities.json` and shallow-merged onto each entity's built discovery
        payload by `build_model_discovery`.
        """
        overrides: dict[str, dict[str, Any]] = {}
        for entity_id, record in self._entities_store.entities.items():
            value = record.options.get("homeassistant")
            if isinstance(value, dict):
                overrides[entity_id] = value
        return overrides

    def _compile_and_build_discovery(self, config: Configuration) -> Model:
        model = compile_model(config, self._profiles, self._effective_compile_options())
        return build_model_discovery(
            model,
            self._profiles,
            config,
            self._discovery_options(),
            self._entity_discovery_overrides(),
        )

    async def _fetch_configuration_with_retry(self) -> Configuration:
        """docs/06 §7: the SysAP may simply be booting alongside us -- retry indefinitely rather
        than giving up, except for auth failures, which are never retried (P-20).
        """
        rest = self._rest
        if rest is None:
            raise RuntimeError("_fetch_configuration_with_retry called before RestClient exists")
        attempt = 0
        while True:
            try:
                config: Configuration = await rest.get_configuration()  # type: ignore[assignment]
                return config
            except _NEVER_RETRIED_CONFIG_FETCH_ERRORS:
                raise
            except _RETRYABLE_CONFIG_FETCH_ERRORS as exc:
                attempt += 1
                logger.warning("configuration fetch failed (attempt %d): %s", attempt, exc)
                await asyncio.sleep(
                    _backoff_delay(
                        attempt,
                        initial=_CONFIG_FETCH_BACKOFF_INITIAL,
                        factor=_RESTART_BACKOFF_FACTOR,
                        cap=_CONFIG_FETCH_BACKOFF_CAP,
                    )
                )

    async def _publish_discovery(self, model: Model) -> None:
        """Changed-only publish (docs/05 §5): a no-op reload publishes zero discovery messages."""
        publisher = self._discovery_publisher
        if publisher is None:
            raise RuntimeError("_publish_discovery called before MqttClient exists")
        await publisher.publish_changed(model)

    async def _retract_stale_discovery(self, model: Model) -> None:
        publisher = self._discovery_publisher
        if publisher is None:
            return
        stale = publisher.stale_topics(model)
        if stale:
            await publisher.retract(stale)

    async def _publish_bridge_devices(self, config: Configuration, model: Model) -> None:
        """docs/04 §4.3: the retained device/channel inventory, split into indexed parts if it
        would exceed `mqtt.maximum_packet_size` (P-41) -- published sequentially, one publish
        per part, never gathered (P-49 applies here just as much as to the initial entity flood).
        """
        mqtt = self._mqtt
        if mqtt is None:
            raise RuntimeError("_publish_bridge_devices called before MqttClient exists")
        devices = _build_bridge_devices(config, model, self._profiles)
        parts = _split_devices_payload(devices, self._config.mqtt_maximum_packet_size)
        base = self._config.base_topic
        if len(parts) == 1:
            await mqtt.publish(topics.bridge_devices_topic(base), parts[0], qos=1, retain=True)
            return
        part_topics = [topics.bridge_devices_part_topic(base, n) for n in range(len(parts))]
        for part_topic, payload in zip(part_topics, parts, strict=True):
            await mqtt.publish(part_topic, payload, qos=1, retain=True)
        await mqtt.publish(
            topics.bridge_devices_topic(base),
            orjson.dumps({"parts": part_topics}),
            qos=1,
            retain=True,
        )

    def _rebuild_dependents(
        self,
        *,
        model: Model,
        state: StateStore,
        mqtt: MqttClient,
        rest: RestClient,
        config: Configuration,
    ) -> None:
        events = EventPublisher(mqtt=mqtt)
        rate_limiter = RateLimiter(min_interval_s=self._config.get_rate_limit_s)
        reconciler = Reconciler(
            state=state,
            rest=rest,
            ingress_table=model.ingress,
            rate_limiter=rate_limiter,
            delay_s=self._config.reconcile_delay_s,
        )
        raw_map = build_raw_map(
            config, model, mode=self._config.raw_mode, base_topic=self._config.base_topic
        )
        raw_publisher = (
            RawStatePublisher(mqtt=mqtt, topics=raw_map.state_topics)
            if self._config.raw_mode is not False
            else None
        )
        self._raw_commands = (
            RawCommandHandler(
                rest=rest,
                base_topic=self._config.base_topic,
                writable_channels=raw_map.writable_channels,
            )
            if self._config.raw_mode is not False
            else None
        )
        self._model = model
        self._state = state
        self._events = events
        self._ingress = Ingress(
            entities=model.entities,
            ingress_table=model.ingress,
            state=state,
            events=events,
            metrics=self.metrics,
            raw=raw_publisher,
        )
        self._publisher = Publisher(
            entities=model.entities,
            state=state,
            mqtt=mqtt,
            coalesce_ms=self._config.coalesce_ms,
            publish_last_changed=self._config.publish_last_changed,
            qos_state=self._config.mqtt_qos_state,
        )
        self._rate_limiter = rate_limiter
        self._reconciler = reconciler
        self._commands = CommandDispatcher(
            entities=model.entities,
            egress=model.egress,
            by_topic=model.by_topic,
            state=state,
            rest=rest,
            mqtt=mqtt,
            reconciler=reconciler,
            rate_limiter=rate_limiter,
            base_topic=self._config.base_topic,
            debounce_s=self._config.command_debounce_s,
            default_optimistic=self._config.default_optimistic,
            optimistic_overrides=self._entity_optimistic_overrides(model),
            debounce_overrides=self._entity_debounce_overrides(model),
        )

    # ------------------------------------------------------------------- live callbacks (WP8)

    def _on_ws_frame(self, body: WsFrameBody) -> None:
        if self._ingress is not None:
            self._ingress.process_frame(body)
        if self._cold_start_done and any(body.get(key) for key in _TOPOLOGY_KEYS):
            self._reload_debouncer.request()

    def _on_mqtt_message(self, message: aiomqtt.Message) -> None:
        if self._commands is not None:
            self._commands.on_message(message)
        if self._raw_commands is not None:
            self._raw_commands.on_message(message)
        if self._bridge_api is not None:
            self._bridge_api.on_message(message)
        if self._is_ha_birth_message(message):
            self._spawn_background(self._handle_ha_birth(), name="ha_birth_republish")

    def _is_ha_birth_message(self, message: aiomqtt.Message) -> bool:
        """P-36: Home Assistant announces itself as `homeassistant/status: "online"` after a
        (re)start or a purge -- that is the bridge's only signal to republish discovery so HA
        recovers its entities.
        """
        if not self._config.homeassistant_enabled:
            return False
        return (
            str(message.topic) == self._config.homeassistant_status_topic
            and message.payload == b"online"
        )

    async def _handle_ha_birth(self) -> None:
        """P-37: wait `republish_delay_s` before republishing -- an instant republish can race
        Home Assistant's own MQTT integration still coming up after the birth message.
        """
        await asyncio.sleep(self._config.homeassistant_republish_delay_s)
        model = self._model
        publisher = self._discovery_publisher
        if model is not None and publisher is not None:
            await publisher.publish_all(model)

    async def _on_mqtt_reconnected(self) -> None:
        """docs/08 §9: republish the accumulated dirty batch and flip `bridge/state` back online
        -- both are explicit reconnect-flow steps, not something `Publisher.run()`'s own wake-loop
        can be relied on to redo by itself (`wake` may already be clear from a publish attempt
        that failed while disconnected).
        """
        if self._availability is not None:
            self._availability.set_mqtt_connected(True)
        if self._publisher is not None:
            await self._publisher.flush()
        if self._availability is not None:
            await self._availability.publish_now()

    def _on_mqtt_disconnected(self) -> None:
        if self._availability is not None:
            self._availability.set_mqtt_connected(False)

    def _on_ws_connected(self) -> None:
        if self._ws is not None:
            self._ws.start_buffering()
        if self._availability is not None:
            self._availability.set_sysap_connected(True)
        if self._cold_start_done:
            self._spawn_background(self._resync(), name="ws_reconnect_resync")

    # ------------------------------------------------------------------------ resync / reload

    async def _resync(self, config: Configuration | None = None) -> None:
        """One config fetch, compile, diff, publish deltas (docs/06 §4) -- shared by every
        trigger: a WS reconnect, a debounced topology/reload request, and the periodic refresher
        (which passes an already-fetched, already-hash-checked `config` to avoid a second fetch).
        """
        mqtt = self._mqtt
        rest = self._rest
        ws = self._ws
        if mqtt is None or rest is None or ws is None:
            raise RuntimeError("_resync called before startup completed")

        ws.start_buffering()
        if config is None:
            config = await self._fetch_configuration_with_retry()
        new_model = self._compile_and_build_discovery(config)
        self._last_config_hash = _hash_config(config)
        await self._diff_and_apply(new_model, mqtt=mqtt, rest=rest, config=config)
        await self._publish_discovery(new_model)

        ingress = self._ingress_or_raise()
        for body in ws.drain_buffer():
            ingress.process_frame(body)

        await self._publisher_or_raise().flush()

        availability = self._availability
        device_availability = self._device_availability
        if availability is None or device_availability is None:
            raise RuntimeError("_resync called before availability trackers exist")
        await availability.publish_now()
        await device_availability.publish(new_model.entities, config.get("devices", {}))
        await self._publish_bridge_devices(config, new_model)

        self.metrics.config_reloads += 1

    async def _diff_and_apply(
        self, new_model: Model, *, mqtt: MqttClient, rest: RestClient, config: Configuration
    ) -> None:
        """Diff the freshly-fetched truth against the *live* state (not the old model's initial
        snapshot -- values may have moved since compile), mark only what changed dirty (P-23), and
        retract any entity that no longer exists (docs/08 §6).
        """
        old_model = self._model
        old_state = self._state

        new_state = self._seed_state(new_model)
        if old_model is not None and old_state is not None:
            for entity_id, new_idx in new_model.by_id.items():
                old_idx = old_model.by_id.get(entity_id)
                if old_idx is None or old_state.values[old_idx] != new_state.values[new_idx]:
                    new_state.dirty.add(new_idx)

        self._rebuild_dependents(
            model=new_model, state=new_state, mqtt=mqtt, rest=rest, config=config
        )

        if old_model is not None:
            removed_ids = sorted(set(old_model.by_id) - set(new_model.by_id))
            for entity_id in removed_ids:
                await self._retract_entity(mqtt, old_model.entities[old_model.by_id[entity_id]])

    async def _retract_entity(self, mqtt: MqttClient, entity: Entity) -> None:
        """docs/08 §6: clear an entity's retained topics so it does not linger in Home Assistant."""
        await mqtt.publish(entity.state_topic, b"", qos=0, retain=True)
        if entity.availability_topic is not None:
            await mqtt.publish(entity.availability_topic, b"", qos=1, retain=True)
        publisher = self._discovery_publisher
        if publisher is not None:
            await publisher.retract(topic for topic, _ in entity.discovery)

    async def _config_refresh_loop(self) -> None:
        """docs/06 §4.1: fetch + hash on a timer; resync only if the hash actually changed."""
        interval = self._config.config_refresh_interval_s
        if interval <= 0:
            return
        while True:
            await asyncio.sleep(interval)
            config = await self._fetch_configuration_with_retry()
            if _hash_config(config) == self._last_config_hash:
                continue
            await self._resync(config)

    # ---------------------------------------------------------------------------- bridge API (WP9)

    def _bridge_api_handlers(self) -> dict[str, Handler]:
        """The docs/04 §5 command table -- bound methods, not a lookup by arbitrary string
        (CLAUDE.md rule 8): every key here is a name docs/04 §5 documents, closed and reviewed.
        """
        return {
            "reload": self._handle_reload,
            "restart": self._handle_restart,
            "entity/rename": self._handle_entity_rename,
            "entity/options": self._handle_entity_options,
            "entity/remove": self._handle_entity_remove,
            "device/refresh": self._handle_device_refresh,
            "discovery/republish": self._handle_discovery_republish,
            "log_level": self._handle_log_level,
            "health": self._handle_health,
            "virtualdevice/create": self._handle_virtualdevice_create,
        }

    async def _handle_reload(self, _args: dict[str, Any]) -> dict[str, Any]:
        """docs/04 §5 `reload`: re-fetch config, recompile, diff, publish deltas -- reuses the
        same debounced path a topology frame or a WS reconnect already goes through (P-55), so a
        burst of reload requests still collapses to one resync.
        """
        self._reload_debouncer.request()
        return {}

    async def _handle_restart(self, _args: dict[str, Any]) -> dict[str, Any]:
        """docs/04 §5 `restart`: graceful shutdown, then exit non-zero so the process supervisor
        (systemd/Docker) restarts us -- `run()`'s own shutdown sequence does the graceful part;
        this only flags the reason and asks for it, mirroring `stop()`.
        """
        self.restart_requested = True
        self._shutdown_event.set()
        return {}

    async def _handle_entity_rename(self, args: dict[str, Any]) -> dict[str, Any]:
        """docs/04 §5 `entity/rename`: the ADR-010 transaction -- clear every old retained topic
        (state and discovery), persist the alias, recompile so the new topic takes effect, force a
        republish under it (a rename usually doesn't change the *value*, so the ordinary
        diff-by-value resync would never republish it on its own), then emit `bridge/event`. The
        entity's discovery config republishes under its new topic automatically: `_resync()`
        below now always (re)builds and changed-only-publishes discovery for the current model,
        and the renamed entity's topic (its `object_id` segment) is new from the store's point of
        view, so it is never mistaken for "unchanged". `unique_id` staying the entity id (P-34) is
        what keeps this from creating a duplicate entity in Home Assistant.
        """
        entity_id = args.get("id")
        new_name = args.get("name")
        if not isinstance(entity_id, str) or not entity_id:
            raise BridgeApiError("entity/rename requires a non-empty 'id'")
        if not isinstance(new_name, str) or not new_name:
            raise BridgeApiError("entity/rename requires a non-empty 'name'")

        model, mqtt = self._model, self._mqtt
        if model is None or mqtt is None:
            raise BridgeApiError("bridge is not ready yet")
        old_idx = model.by_id.get(entity_id)
        if old_idx is None:
            raise BridgeApiError(f"unknown entity id: {entity_id!r}")
        old_entity = model.entities[old_idx]

        await self._retract_entity(mqtt, old_entity)
        self._entities_store.set_alias(entity_id, new_name)
        await self._resync()

        new_model, new_state = self._model, self._state
        new_topic = old_entity.state_topic
        if new_model is not None and new_state is not None:
            new_idx = new_model.by_id.get(entity_id)
            if new_idx is not None:
                new_topic = new_model.entities[new_idx].state_topic
                new_state.dirty.add(new_idx)
                await self._publisher_or_raise().flush()

        await mqtt.publish(
            topics.bridge_event_topic(self._config.base_topic),
            orjson.dumps(
                {
                    "type": "entity_renamed",
                    "id": entity_id,
                    "from": old_entity.state_topic,
                    "to": new_topic,
                }
            ),
            qos=0,
            retain=False,
        )
        return {"id": entity_id, "topic": new_topic}

    async def _handle_entity_options(self, args: dict[str, Any]) -> dict[str, Any]:
        """docs/04 §5 `entity/options`: persists overrides to `entities.json` (docs/07 §4.1).

        `enabled` acts on compilation itself (`CompileOptions.excluded_entity_ids`, symmetric
        with `aliases`); `optimistic`/`debounce_ms` act on `CommandDispatcher`
        (`Supervisor._entity_optimistic_overrides`/`_entity_debounce_overrides`, rebuilt on every
        `_rebuild_dependents`); `homeassistant` is shallow-merged onto the entity's discovery
        payload by `build_model_discovery` (`Supervisor._entity_discovery_overrides`). All four
        still need a resync to actually apply (`_rebuild_dependents`/`_compile_and_build_discovery`
        only run there), so any of them requests one -- not just `enabled`.
        """
        entity_id = args.get("id")
        options = args.get("options")
        if not isinstance(entity_id, str) or not entity_id:
            raise BridgeApiError("entity/options requires a non-empty 'id'")
        if not isinstance(options, dict):
            raise BridgeApiError("entity/options requires an 'options' object")
        self._entities_store.set_options(entity_id, options)
        if options.keys() & {"enabled", "optimistic", "debounce_ms", "homeassistant"}:
            self._reload_debouncer.request()
        return {"id": entity_id, "options": self._entities_store.options_for(entity_id)}

    async def _handle_entity_remove(self, args: dict[str, Any]) -> dict[str, Any]:
        """docs/04 §5 `entity/remove`: durably excludes the entity (`enabled: false`, the same
        mechanism `entity/options` uses) and requests a resync -- the ordinary removed-entity
        retraction path (P-35) then clears its retained topics exactly as it would for a device
        that genuinely left the installation. A durable exclusion, kept until an explicit
        `entity/options {"enabled": true}` brings it back, is deliberately chosen over a
        record-pruning "until next reload" reading of docs/07 §4.1 -- pruning the persisted record
        would erase the very marker that keeps the entity excluded, un-hiding it on the next
        unrelated resync (a periodic refresh, a topology blip) instead of staying gone. Fixed in
        that document in this commit, per CLAUDE.md §4.
        """
        entity_id = args.get("id")
        if not isinstance(entity_id, str) or not entity_id:
            raise BridgeApiError("entity/remove requires a non-empty 'id'")
        self._entities_store.set_options(entity_id, {"enabled": False})
        self._reload_debouncer.request()
        return {"id": entity_id}

    async def _handle_device_refresh(self, args: dict[str, Any]) -> dict[str, Any]:
        """docs/04 §5 `device/refresh`: confirm the device exists with a targeted
        `GET /api/rest/device/...`, then apply any change via the ordinary resync path rather than
        a bespoke single-device merge -- docs/06 §4's resync is already cheap (one HTTP request,
        P8's budget), so "refresh just this device" degrading to "resync everything, having first
        confirmed this device is real" is a deliberate, documented simplification.
        """
        serial = args.get("serial")
        if not isinstance(serial, str) or not serial:
            raise BridgeApiError("device/refresh requires a non-empty 'serial'")
        rest = self._rest
        if rest is None:
            raise BridgeApiError("bridge is not ready yet")
        try:
            await rest.get_device(serial)
        except NotFoundError as exc:
            raise BridgeApiError(f"unknown device serial: {serial!r}") from exc
        self._reload_debouncer.request()
        return {"serial": serial}

    async def _handle_discovery_republish(self, _args: dict[str, Any]) -> dict[str, Any]:
        """docs/04 §5 `discovery/republish`: force a full HA discovery republish -- every topic,
        regardless of `discovery.json`'s changed-only bookkeeping (`_publish_discovery` is the
        changed-only path ordinary resyncs use; this one always publishes everything).
        """
        model = self._model
        publisher = self._discovery_publisher
        if model is None or publisher is None:
            raise BridgeApiError("bridge is not ready yet")
        await publisher.publish_all(model)
        return {}

    async def _handle_log_level(self, args: dict[str, Any]) -> dict[str, Any]:
        """docs/04 §5 `log_level`: change verbosity at runtime, no restart."""
        level = args.get("level")
        if not isinstance(level, str) or not level:
            raise BridgeApiError("log_level requires a non-empty 'level'")
        try:
            log.set_level(level)
        except ValueError as exc:
            raise BridgeApiError(str(exc)) from exc
        return {"level": level.lower()}

    async def _handle_health(self, _args: dict[str, Any]) -> dict[str, Any]:
        """docs/04 §5 `health`: the `bridge/info` body plus a pass/fail check list."""
        availability = self._availability
        checks = [
            {
                "name": "mqtt_connected",
                "ok": availability.mqtt_connected if availability is not None else False,
            },
            {
                "name": "sysap_connected",
                "ok": availability.sysap_connected if availability is not None else False,
            },
            {
                "name": "model_loaded",
                "ok": availability.model_loaded if availability is not None else False,
            },
        ]
        return {"info": self._build_bridge_info(), "checks": checks}

    def _build_bridge_info(self) -> dict[str, Any]:
        """`bridge/info` (docs/04 §4.2). A first cut: every field that already has a real source
        (compile stats, availability, reconnect counters, `entities.json`-adjacent config) is
        populated; `stats.commands`/`command_errors`/`state_publishes`/`latency_ms` have no
        counter anywhere yet (`CommandDispatcher`/`Publisher` don't track them) and are a real,
        named gap for a later WP, not silently fabricated.
        """
        availability = self._availability
        model = self._model
        settings = self._sysap_settings

        counts: dict[str, int] = {}
        if model is not None:
            stats = model.stats
            counts = {
                "devices": stats.devices_total,
                "channels": stats.channels_total,
                "entities": stats.entities_created,
                "unsupported_channels": stats.channels_unsupported,
                "orphan_channels_skipped": stats.channels_orphaned,
            }

        sysap: dict[str, Any] = {"url": self._config.sysap_base_url}
        if settings is not None:
            sysap["name"] = settings.name
            sysap["serial"] = settings.serial_number
            sysap["version"] = settings.version
        if self._rest is not None and self._rest.sysap_uuid is not None:
            sysap["uuid"] = self._rest.sysap_uuid

        stats_body: dict[str, Any] = {
            "uptime_s": round(time.monotonic() - self._started_at, 1)
            if self._started_at is not None
            else 0.0,
            "datapoints_in": self.metrics.datapoints_in,
            "unmapped_datapoints": self.metrics.unmapped_datapoints,
            "events": self.metrics.events,
            "codec_errors": self.metrics.codec_errors,
            "config_reloads": self.metrics.config_reloads,
            "task_restarts": self.metrics.task_restarts,
        }
        if self._ws is not None:
            stats_body["reconnects_ws"] = self._ws.reconnect_count
        if self._mqtt is not None:
            stats_body["reconnects_mqtt"] = self._mqtt.reconnect_count

        return {
            "version": _package_version(),
            "sysap": sysap,
            "links": {
                "mqtt": _connected_or_not(availability is not None and availability.mqtt_connected),
                "sysap_rest": "ok"
                if self._rest is not None and self._rest.sysap_uuid is not None
                else "unknown",
                "sysap_ws": _connected_or_not(
                    availability is not None and availability.sysap_connected
                ),
            },
            "counts": counts,
            "config": {
                "base_topic": self._config.base_topic,
                "topic_style": self._config.compile_options.topic_style,
                "coalesce_ms": self._config.coalesce_ms,
                "max_inflight": self._config.sysap_max_inflight,
            },
            "stats": stats_body,
        }

    async def _handle_virtualdevice_create(self, args: dict[str, Any]) -> dict[str, Any]:
        """docs/04 §5 `virtualdevice/create`: create/refresh a virtual device (docs/01 §4.5).

        A finite `ttl` gets a keepalive task re-`PUT`ting at `ttl / 2` (P-16) -- re-creating for an
        already-running serial cancels and replaces its keepalive rather than stacking a second
        one, matching "create/refresh" being the same operation either way.
        """
        serial = args.get("serial")
        type_ = args.get("type")
        ttl = args.get("ttl")
        if not isinstance(serial, str) or not serial:
            raise BridgeApiError("virtualdevice/create requires a non-empty 'serial'")
        if not isinstance(type_, str) or not type_:
            raise BridgeApiError("virtualdevice/create requires a non-empty 'type'")
        if not isinstance(ttl, int) or isinstance(ttl, bool):
            raise BridgeApiError("virtualdevice/create requires an integer 'ttl'")
        rest = self._rest
        if rest is None:
            raise BridgeApiError("bridge is not ready yet")

        properties = {k: v for k, v in args.items() if k not in _VIRTUAL_DEVICE_RESERVED_KEYS}
        await rest.create_virtual_device(serial, type_=type_, ttl=ttl, **properties)
        self._start_virtual_device_keepalive(serial, type_=type_, ttl=ttl, properties=properties)
        return {"serial": serial}

    def _start_virtual_device_keepalive(
        self, serial: str, *, type_: str, ttl: int, properties: dict[str, Any]
    ) -> None:
        self._cancel_virtual_device_keepalive(serial)
        if ttl in _VIRTUAL_DEVICE_NO_EXPIRY:
            return
        task = asyncio.create_task(
            self._virtual_device_keepalive_loop(serial, type_=type_, ttl=ttl, properties=properties)
        )
        self._virtual_device_tasks[serial] = task
        task.add_done_callback(
            lambda t: self._log_background_result(t, f"virtualdevice_keepalive[{serial}]")
        )

    def _cancel_virtual_device_keepalive(self, serial: str) -> None:
        task = self._virtual_device_tasks.pop(serial, None)
        if task is not None:
            task.cancel()

    async def _virtual_device_keepalive_loop(
        self, serial: str, *, type_: str, ttl: int, properties: dict[str, Any]
    ) -> None:
        interval = ttl / 2
        with contextlib.suppress(asyncio.CancelledError):
            while True:
                await asyncio.sleep(interval)
                rest = self._rest
                if rest is None:
                    return
                await rest.create_virtual_device(serial, type_=type_, ttl=ttl, **properties)

    async def _stop_virtual_device_keepalives(self) -> None:
        """docs/01 §4.5, P-16: stopped cleanly on shutdown -- part of docs/08 §10's connection
        teardown, not left to expire on the SysAP on their own.
        """
        tasks = list(self._virtual_device_tasks.values())
        self._virtual_device_tasks.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    # -------------------------------------------------------------------------------- shutdown

    async def _graceful_shutdown(self) -> None:
        """docs/08 §10, in order: stop new commands, flush pending ones, flush publisher state,
        publish an explicit offline, snapshot persistence, then close every connection.

        A broker that happens to be mid-reconnect at this exact moment must not turn a clean
        shutdown into a crash: these final publishes are best-effort (there's nobody to receive
        them if MQTT isn't there), so `MqttClientNotConnectedError` here is logged, not fatal.
        """
        await self._stop_virtual_device_keepalives()
        if self._commands is not None:
            self._commands.stop_accepting()
            await self._commands.flush_pending(deadline_s=2.0)
        if self._publisher is not None:
            await self._best_effort_publish(self._publisher.flush(), what="final state flush")
        if self._availability is not None:
            await self._best_effort_publish(
                self._availability.publish_forced_offline(), what="bridge/state offline"
            )
        await self._entities_store.save()
        if self._ws is not None:
            await self._ws.stop()
        await self._http_session.close()
        if self._mqtt is not None:
            await self._mqtt.stop()
        if self._metrics_server is not None:
            await self._metrics_server.stop()
        if self._publisher_task is not None:
            self._publisher_task.cancel()
        if self._config_refresh_task is not None:
            self._config_refresh_task.cancel()

    async def _best_effort_publish(self, coro: Coroutine[Any, Any, None], *, what: str) -> None:
        try:
            await coro
        except MqttClientNotConnectedError:
            logger.warning("shutdown: could not publish %s -- MQTT is disconnected", what)
