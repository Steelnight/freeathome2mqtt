"""Task ownership, startup order, resync/reload, and shutdown (ADR-001; docs/02 §3, §7-8; docs/06
§3-4; docs/11 WP8).

`Supervisor` owns every long-lived task (docs/02 §3) inside one `asyncio.TaskGroup`, each wrapped
in `restart_on_failure` (docs/02 §3.1): catch, log, back off with jitter, restart -- escalating to
a fatal `TaskDiedTooManyTimesError` after five rapid failures in a row (P-29), which the caller of
`run()` is expected to treat as "exit the process, let the container restart it."

Startup follows docs/02 §7 precisely: probe, connect MQTT (LWT armed before anything risky can
fail, P-30), open the SysAP WebSocket and buffer *before* fetching the configuration (P-22), fetch
and compile, drain the buffer over the compiled state, publish discovery then state then
`bridge/state: online`. Resync (docs/06 §4) reuses the same buffer-then-fetch-then-drain shape on
every WebSocket reconnect, topology change (P-13), or `bridge/request/reload`, publishing only the
entities that actually changed (P-23) -- never re-publishing everything on every blip.

Not yet wired here, by design: `homeassistant/discovery.py` (WP10, so `model.discovery` is always
empty and `_publish_discovery` is a documented no-op), `mqtt/bridge_api.py`'s `bridge/devices` /
`bridge/info` / `reload` / rename commands (WP9), and a 404-on-write-triggers-resync hook (docs/06
§4.1's last row -- a real gap, deferred rather than bolted on without an acceptance test to pin its
shape down).
"""

from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import logging
import random
import ssl as ssl_module
import time
from collections.abc import Awaitable, Callable, Coroutine
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp
import aiomqtt
import orjson

from freeathome2mqtt.availability import BridgeAvailability, DeviceAvailabilityPublisher
from freeathome2mqtt.bus.commands import CommandDispatcher
from freeathome2mqtt.bus.events import EventPublisher
from freeathome2mqtt.bus.ingress import Ingress
from freeathome2mqtt.bus.publisher import Publisher
from freeathome2mqtt.bus.reconcile import RateLimiter, Reconciler
from freeathome2mqtt.bus.state import StateStore
from freeathome2mqtt.metrics import Metrics
from freeathome2mqtt.model.compiler import CompileOptions, Model
from freeathome2mqtt.model.compiler import compile as compile_model
from freeathome2mqtt.model.entity import Entity
from freeathome2mqtt.model.profiles import ProfileRegistry
from freeathome2mqtt.mqtt.client import MqttClient, MqttClientNotConnectedError
from freeathome2mqtt.persistence import EntitiesStore
from freeathome2mqtt.sysap.rest import (
    AuthenticationError,
    BadRequestError,
    ForbiddenError,
    RestClient,
    SysApError,
)
from freeathome2mqtt.sysap.schema import Configuration, WsFrameBody
from freeathome2mqtt.sysap.settings_probe import check_version_supported, fetch_settings
from freeathome2mqtt.sysap.ws import WsReader

logger = logging.getLogger(__name__)

_RESTART_BACKOFF_INITIAL = 1.0
_RESTART_BACKOFF_FACTOR = 2.0
_RESTART_BACKOFF_CAP = 60.0
_ESCALATION_THRESHOLD = 5
_RAPID_FAILURE_WINDOW_S = 10.0

_RELOAD_DEBOUNCE_S = 2.0
_RELOAD_MIN_INTERVAL_S = 30.0

_CONFIG_FETCH_BACKOFF_INITIAL = 1.0
_CONFIG_FETCH_BACKOFF_CAP = 30.0

_STARTUP_POLL_INTERVAL_S = 0.01

_TOPOLOGY_KEYS = ("devices", "devicesAdded", "devicesRemoved", "parameters")

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
    """Run `factory()` forever, restarting with backoff+jitter on any exception (docs/02 §3.1).

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
    mqtt_host: str = "localhost"
    mqtt_port: int = 1883
    mqtt_username: str | None = None
    mqtt_password: str | None = None
    mqtt_tls: ssl_module.SSLContext | None = None
    mqtt_keepalive: int = 60
    base_topic: str = "freeathome2mqtt"
    compile_options: CompileOptions = field(default_factory=CompileOptions)
    data_dir: Path = field(default_factory=lambda: Path("/data"))
    coalesce_ms: int = 20
    publish_last_changed: bool = True
    command_debounce_s: float = 0.05
    reconcile_delay_s: float = 3.0
    get_rate_limit_s: float = 5.0
    grace_seconds: float = 10.0
    config_refresh_interval_s: float = 300.0
    reload_debounce_s: float = _RELOAD_DEBOUNCE_S
    reload_min_interval_s: float = _RELOAD_MIN_INTERVAL_S
    link_backoff_initial: float = 1.0
    link_backoff_factor: float = 2.0
    link_backoff_cap: float = 60.0
    ws_heartbeat_s: float | None = 30.0
    ws_idle_timeout_s: float = 90.0


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

        self._model: Model | None = None
        self._state: StateStore | None = None
        self._ingress: Ingress | None = None
        self._events: EventPublisher | None = None
        self._publisher: Publisher | None = None
        self._rate_limiter: RateLimiter | None = None
        self._reconciler: Reconciler | None = None
        self._commands: CommandDispatcher | None = None
        self._device_availability: DeviceAvailabilityPublisher | None = None

        self._mqtt: MqttClient | None = None
        self._rest: RestClient | None = None
        self._ws: WsReader | None = None
        self._availability: BridgeAvailability | None = None

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
        self._entities_store.load()

        settings = await fetch_settings(self._http_session, self._config.sysap_base_url)
        check_version_supported(settings.version)

        mqtt = MqttClient(
            host=self._config.mqtt_host,
            port=self._config.mqtt_port,
            base_topic=self._config.base_topic,
            sysap_serial=settings.serial_number,
            username=self._config.mqtt_username,
            password=self._config.mqtt_password,
            tls_context=self._config.mqtt_tls,
            keepalive=self._config.mqtt_keepalive,
            backoff_initial=self._config.link_backoff_initial,
            backoff_factor=self._config.link_backoff_factor,
            backoff_cap=self._config.link_backoff_cap,
            on_message=self._on_mqtt_message,
            on_reconnected=self._on_mqtt_reconnected,
            on_disconnected=self._on_mqtt_disconnected,
        )
        self._mqtt = mqtt
        availability = BridgeAvailability(
            mqtt=mqtt, base_topic=self._config.base_topic, grace_seconds=self._config.grace_seconds
        )
        self._availability = availability
        self._device_availability = DeviceAvailabilityPublisher(mqtt=mqtt)

        self._spawn_supervised("mqtt_client", mqtt.run)
        await _wait_until(lambda: mqtt.reconnect_count >= 1)

        rest = RestClient(
            base_url=self._config.sysap_base_url,
            username=self._config.sysap_username,
            password=self._config.sysap_password,
            session=self._http_session,
            ssl=self._config.sysap_ssl,
            max_inflight=self._config.sysap_max_inflight,
        )
        self._rest = rest

        ws = WsReader(
            url=f"{self._config.sysap_base_url}/fhapi/v1/api/ws",
            username=self._config.sysap_username,
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
        self._ws = ws
        ws.start_buffering()  # armed before the very first connect too (docs/02 §7)
        self._spawn_supervised("ws_reader", ws.run)
        await _wait_until(lambda: ws.reconnect_count >= 1)
        availability.set_sysap_connected(True)

        config = await self._fetch_configuration_with_retry()
        new_model = compile_model(config, self._profiles, self._effective_compile_options())
        self._last_config_hash = _hash_config(config)
        state = self._seed_state(new_model)
        self._rebuild_dependents(model=new_model, state=state, mqtt=mqtt, rest=rest)

        for body in ws.drain_buffer():
            self._ingress_or_raise().process_frame(body)

        await self._publish_discovery(new_model)
        state.dirty.update(range(len(new_model.entities)))
        await self._publisher_or_raise().flush()
        await self._device_availability.publish(new_model.entities, config.get("devices", {}))

        availability.set_model_loaded(True)
        await availability.publish_now()

        self._cold_start_done = True
        self._publisher_task = self._spawn_supervised("publisher", self._publisher_or_raise().run)
        self._config_refresh_task = self._spawn_supervised(
            "config_refresher", self._config_refresh_loop
        )

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
        return dataclasses.replace(
            self._config.compile_options, topic_prefix=self._config.base_topic, aliases=aliases
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
        """WP10 will populate `model.discovery`; until then this is a documented no-op."""
        mqtt = self._mqtt
        if mqtt is None:
            raise RuntimeError("_publish_discovery called before MqttClient exists")
        for topic, payload in model.discovery:
            await mqtt.publish(topic, payload, qos=1, retain=True)

    def _rebuild_dependents(
        self, *, model: Model, state: StateStore, mqtt: MqttClient, rest: RestClient
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
        self._model = model
        self._state = state
        self._events = events
        self._ingress = Ingress(
            entities=model.entities,
            ingress_table=model.ingress,
            state=state,
            events=events,
            metrics=self.metrics,
        )
        self._publisher = Publisher(
            entities=model.entities,
            state=state,
            mqtt=mqtt,
            coalesce_ms=self._config.coalesce_ms,
            publish_last_changed=self._config.publish_last_changed,
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
        new_model = compile_model(config, self._profiles, self._effective_compile_options())
        self._last_config_hash = _hash_config(config)
        await self._diff_and_apply(new_model, mqtt=mqtt, rest=rest)

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

        self.metrics.config_reloads += 1

    async def _diff_and_apply(
        self, new_model: Model, *, mqtt: MqttClient, rest: RestClient
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

        self._rebuild_dependents(model=new_model, state=new_state, mqtt=mqtt, rest=rest)

        if old_model is not None:
            removed_ids = sorted(set(old_model.by_id) - set(new_model.by_id))
            for entity_id in removed_ids:
                await self._retract_entity(mqtt, old_model.entities[old_model.by_id[entity_id]])

    async def _retract_entity(self, mqtt: MqttClient, entity: Entity) -> None:
        """docs/08 §6: clear an entity's retained topics so it does not linger in Home Assistant."""
        await mqtt.publish(entity.state_topic, b"", qos=0, retain=True)
        if entity.availability_topic is not None:
            await mqtt.publish(entity.availability_topic, b"", qos=1, retain=True)
        for discovery_topic, _ in entity.discovery:
            await mqtt.publish(discovery_topic, b"", qos=1, retain=True)

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

    # -------------------------------------------------------------------------------- shutdown

    async def _graceful_shutdown(self) -> None:
        """docs/08 §10, in order: stop new commands, flush pending ones, flush publisher state,
        publish an explicit offline, snapshot persistence, then close every connection.

        A broker that happens to be mid-reconnect at this exact moment must not turn a clean
        shutdown into a crash: these final publishes are best-effort (there's nobody to receive
        them if MQTT isn't there), so `MqttClientNotConnectedError` here is logged, not fatal.
        """
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
        if self._publisher_task is not None:
            self._publisher_task.cancel()
        if self._config_refresh_task is not None:
            self._config_refresh_task.cancel()

    async def _best_effort_publish(self, coro: Coroutine[Any, Any, None], *, what: str) -> None:
        try:
            await coro
        except MqttClientNotConnectedError:
            logger.warning("shutdown: could not publish %s -- MQTT is disconnected", what)
