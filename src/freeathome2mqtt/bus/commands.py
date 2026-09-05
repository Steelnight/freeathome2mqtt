"""`/set` and `/get` handling: object/attribute/scalar forms, validate-then-clamp, debounce
(docs/02 §5; docs/04 §3; docs/11 WP7).

`CommandDispatcher.on_message` is the `on_message` callback handed to `mqtt.client.MqttClient` --
like `bus/ingress.py`'s `Ingress.process_frame`, it stays synchronous itself and hands the actual
work off to a tracked fire-and-forget task, so a slow REST write or MQTT publish can never block
message dispatch.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable, Mapping, Sequence
from typing import TYPE_CHECKING, Any

import orjson

from freeathome2mqtt.metrics import Metrics
from freeathome2mqtt.model.codecs import CommandError
from freeathome2mqtt.model.entity import AttrKind
from freeathome2mqtt.model.transforms import get_transform
from freeathome2mqtt.mqtt import topics
from freeathome2mqtt.sysap.rest import NotFoundError, SysApError

if TYPE_CHECKING:
    import aiomqtt

    from freeathome2mqtt.bus.reconcile import RateLimiter, Reconciler
    from freeathome2mqtt.bus.state import StateStore
    from freeathome2mqtt.model.entity import EgressBinding, Entity
    from freeathome2mqtt.mqtt.client import MqttClient
    from freeathome2mqtt.sysap.rest import RestClient

logger = logging.getLogger(__name__)

_TRANSACTION_KEY = "transaction"
_NO_OPTIMISTIC_KEY = "no_optimistic"
_RESERVED_OBJECT_KEYS = frozenset({_TRANSACTION_KEY, _NO_OPTIMISTIC_KEY})
_DEFAULT_DEBOUNCE_S = 0.05
_ON_OFF = {"ON": True, "OFF": False}
_TOPIC_PARTS_TERMINAL = 2  # "<slug>/set" or "<slug>/get"
_TOPIC_PARTS_ATTRIBUTE = 3  # "<slug>/set/<attribute>"


def _decode_payload(payload: bytes) -> Any:
    """JSON-decode if it parses, else treat as a plain string (docs/04 §3.2)."""
    try:
        return orjson.loads(payload)
    except orjson.JSONDecodeError:
        text = payload.decode("utf-8", errors="replace")
        return _ON_OFF.get(text, text)


class CommandDispatcher:
    """Routes incoming `/set`/`/get` messages to validated, debounced SysAP writes."""

    def __init__(
        self,
        *,
        entities: Sequence[Entity],
        egress: Mapping[tuple[int, str], EgressBinding],
        by_topic: Mapping[str, int],
        state: StateStore,
        rest: RestClient,
        mqtt: MqttClient,
        reconciler: Reconciler,
        rate_limiter: RateLimiter,
        base_topic: str,
        debounce_s: float = _DEFAULT_DEBOUNCE_S,
        default_optimistic: bool = True,
        optimistic_overrides: Mapping[int, bool] | None = None,
        debounce_overrides: Mapping[int, float] | None = None,
        metrics: Metrics | None = None,
        on_topology_changed: Callable[[], None] | None = None,
    ) -> None:
        self._entities = entities
        self._egress = egress
        self._by_topic = by_topic
        self._state = state
        self._rest = rest
        self._mqtt = mqtt
        self._reconciler = reconciler
        self._rate_limiter = rate_limiter
        self._base_topic = base_topic
        self._debounce_s = debounce_s
        self._default_optimistic = default_optimistic
        # docs/04 §5 `entity/options`: per-entity overrides of the installation-wide defaults
        # above, keyed by entity idx (rebuilt from entities.json on every resync, docs/07 §4.1).
        self._optimistic_overrides = optimistic_overrides or {}
        self._debounce_overrides = debounce_overrides or {}
        self._metrics = metrics if metrics is not None else Metrics()
        # docs/06 §4.1's last row: a `404` on a write means the compiled model no longer matches
        # the installation. The dispatcher only *reports* that; the Supervisor owns what to do
        # about it, and its existing `_ReloadDebouncer` (P-55) is what keeps a burst of 404s from
        # becoming a burst of config fetches (ADR-007).
        self._on_topology_changed = on_topology_changed

        self._ordered_commands: dict[int, list[str]] = {}
        for entity_idx, cmd_name in egress:
            self._ordered_commands.setdefault(entity_idx, []).append(cmd_name)

        self._pending: dict[tuple[int, str], str] = {}
        self._last_sent: dict[tuple[int, str], str] = {}
        self._windows: dict[tuple[int, str], asyncio.Task[None]] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self._accepting = True

    def stop_accepting(self) -> None:
        """Shutdown step 1 (docs/08 §10): ignore every message from here on. A command already
        dispatched to a background task is unaffected -- `flush_pending` is what settles those.
        """
        self._accepting = False

    async def flush_pending(self, *, deadline_s: float = 2.0) -> None:
        """Shutdown step 2 (docs/08 §10): send whatever is still sitting in an open debounce
        window right now, bounded by `deadline_s` -- a value the user just set must not be
        silently dropped, but shutdown cannot wait indefinitely for a slider still being dragged.
        """
        for key in list(self._windows):
            window = self._windows.pop(key, None)
            if window is not None:
                window.cancel()
            pending = self._pending.pop(key, None)
            binding = self._egress.get(key)
            if pending is None or binding is None:
                continue
            self._send(key, pending, binding, None)
        pending_tasks = list(self._tasks)
        if not pending_tasks:
            return
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(deadline_s):
                await asyncio.gather(*pending_tasks, return_exceptions=True)

    def on_message(self, message: aiomqtt.Message) -> None:
        """Parse the topic only; everything else runs in a tracked background task."""
        if not self._accepting:
            return
        parsed = self._parse_topic(str(message.topic))
        if parsed is None:
            return
        entity_idx, form, attribute = parsed
        task = asyncio.create_task(self._dispatch(entity_idx, form, attribute, message.payload))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _parse_topic(self, topic: str) -> tuple[int, str, str | None] | None:
        prefix = f"{self._base_topic}/"
        if not topic.startswith(prefix):
            return None
        parts = topic[len(prefix) :].split("/")
        entity_idx = self._by_topic.get(parts[0])
        if entity_idx is None:
            return None
        if len(parts) == _TOPIC_PARTS_TERMINAL and parts[1] in ("set", "get"):
            return entity_idx, parts[1], None
        if len(parts) == _TOPIC_PARTS_ATTRIBUTE and parts[1] == "set":
            return entity_idx, "set", parts[2]
        return None

    async def _dispatch(
        self, entity_idx: int, form: str, attribute: str | None, payload: bytes
    ) -> None:
        if form == "get":
            await self._handle_get(entity_idx, payload)
        elif attribute is not None:
            await self._handle_attribute_form(entity_idx, attribute, payload)
        else:
            await self._handle_set(entity_idx, payload)

    # ---------------------------------------------------------------------------------- /set

    async def _handle_set(self, entity_idx: int, payload: bytes) -> None:
        decoded = _decode_payload(payload)
        if isinstance(decoded, dict):
            await self._handle_object_form(entity_idx, decoded)
            return
        # Scalar shorthand (docs/04 §3.3): a bare value addresses the entity's primary command,
        # the first one declared in the profile (attr_names/commands preserve YAML order).
        ordered = self._ordered_commands.get(entity_idx, [])
        if not ordered:
            return
        await self._apply_command(
            entity_idx, ordered[0], decoded, transaction=None, optimistic=True
        )

    async def _handle_object_form(self, entity_idx: int, obj: dict[str, Any]) -> None:
        transaction = obj.get(_TRANSACTION_KEY)
        optimistic = not obj.get(_NO_OPTIMISTIC_KEY, False)
        ordered = self._ordered_commands.get(entity_idx, [])
        # Profile-declaration order for known commands (docs/04 §3.1: JSON key order must not
        # matter); anything else in the payload is processed too, in payload order, so a typo'd
        # or unknown command still gets its own rejection rather than being silently skipped.
        known = [name for name in ordered if name in obj]
        unknown = [key for key in obj if key not in _RESERVED_OBJECT_KEYS and key not in ordered]
        for cmd_name in known + unknown:
            await self._apply_command(
                entity_idx, cmd_name, obj[cmd_name], transaction=transaction, optimistic=optimistic
            )

    async def _handle_attribute_form(self, entity_idx: int, attribute: str, payload: bytes) -> None:
        await self._apply_command(
            entity_idx, attribute, _decode_payload(payload), transaction=None, optimistic=True
        )

    async def _apply_command(
        self,
        entity_idx: int,
        cmd_name: str,
        raw_value: Any,
        *,
        transaction: str | None,
        optimistic: bool,
    ) -> None:
        sub_writes = self._resolve_writes(entity_idx, cmd_name, raw_value)
        if sub_writes is None:
            error = f"unknown command: {cmd_name!r}"
            await self._respond_error(entity_idx, transaction, "set", error)
            return

        for sub_name, sub_value in sub_writes:
            applied = await self._apply_one_write(
                entity_idx, sub_name, sub_value, transaction=transaction, optimistic=optimistic
            )
            if not applied:
                return

        if transaction is not None:
            await self._respond_ok(entity_idx, transaction, "set")

    def _resolve_writes(
        self, entity_idx: int, cmd_name: str, raw_value: Any
    ) -> list[tuple[str, Any]] | None:
        """A transformed entity's command is offered to `Transform.command()` first (docs/03 §7):
        `room_temperature_controller` only claims its synthetic `hvac_mode`, raising `CommandError`
        for everything else so real profile commands (`on_off`, `eco`, `mode`) fall through to the
        direct path below unchanged; `cover_with_slats` claims every one of its real commands too,
        to add side effects (the slat auto-reset on a full-open `position`). `None` means "no such
        command at all", the caller's cue to report `unknown command`.
        """
        entity = self._entities[entity_idx]
        if entity.transform is not None:
            try:
                return get_transform(entity.transform).command(
                    cmd_name, raw_value, self._state.values[entity_idx]
                )
            except CommandError:
                pass
        if (entity_idx, cmd_name) in self._egress:
            return [(cmd_name, raw_value)]
        return None

    async def _apply_one_write(
        self,
        entity_idx: int,
        cmd_name: str,
        raw_value: Any,
        *,
        transaction: str | None,
        optimistic: bool,
    ) -> bool:
        """Validate, encode, optimistically mark and enqueue one `(entity_idx, cmd_name)` write.
        Returns whether it succeeded; a failure has already sent the error response itself, so
        `_apply_command` only needs to stop processing further sub-writes, not respond again.
        """
        binding = self._egress.get((entity_idx, cmd_name))
        if binding is None:
            # A transform named a sub-command absent from this compiled entity -- its datapoint
            # or codec was unavailable at compile time (docs/03 §5), a profile-authoring bug
            # surfaced here rather than silently dropped.
            error = f"unknown command: {cmd_name!r}"
            await self._respond_error(entity_idx, transaction, "set", error)
            return False
        try:
            validated = binding.validate(raw_value)
            encoded = binding.encode(validated)
        except (TypeError, ValueError, CommandError) as exc:
            await self._respond_error(entity_idx, transaction, "set", str(exc))
            return False

        # entity/options {"optimistic": false} (docs/04 §5) forces optimism off regardless of
        # this message's own no_optimistic flag; unset or true leaves the message's flag as-is.
        # `performance.optimistic` (docs/07 §2) is the installation-wide fallback when no
        # per-entity override exists at all.
        effective_optimistic = optimistic and self._optimistic_overrides.get(
            entity_idx, self._default_optimistic
        )
        if effective_optimistic and binding.optimistic_attr is not None:
            attr_idx = binding.optimistic_attr
            self._state.mark_optimistic(entity_idx, attr_idx, validated, attr_bit=1 << attr_idx)
            if binding.confirm:
                self._reconciler.schedule(entity_idx, attr_idx)

        # docs/04 §4.2's `commands`, counted here: after validation (so a rejected message is an
        # error, not a command) and before the debouncer (so the figure says how much is being
        # asked of the bridge, which is the question docs/05 §9 step 4 uses it to answer -- the
        # write count that survives debouncing is a different, deliberately different number).
        self._metrics.commands += 1
        self._enqueue_write(entity_idx, cmd_name, encoded, binding, transaction)
        return True

    # ------------------------------------------------------------- debounce (docs/05 §4.2)

    def _enqueue_write(
        self,
        entity_idx: int,
        cmd_name: str,
        encoded: str,
        binding: EgressBinding,
        transaction: str | None,
    ) -> None:
        """Leading edge + trailing edge, classic debounce semantics (docs/05 §4.2): the *first*
        message in a burst sends immediately; every message after that, for as long as they keep
        arriving faster than `debounce_s` apart, only updates `pending` and resets the window's
        deadline -- so a held slider produces exactly one more write (the final position), not one
        per window-width, however long the drag runs.
        """
        key = (entity_idx, cmd_name)
        if not binding.continuous:
            self._send(key, encoded, binding, transaction)
            return
        existing_window = self._windows.get(key)
        if existing_window is None:
            # Leading edge: send now.
            self._send(key, encoded, binding, transaction)
        else:
            existing_window.cancel()
        self._pending[key] = encoded
        self._windows[key] = asyncio.create_task(self._close_window(key, binding))

    def _debounce_s_for(self, entity_idx: int) -> float:
        return self._debounce_overrides.get(entity_idx, self._debounce_s)

    async def _close_window(self, key: tuple[int, str], binding: EgressBinding) -> None:
        await asyncio.sleep(self._debounce_s_for(key[0]))
        self._windows.pop(key, None)
        pending = self._pending.pop(key, None)
        if pending is not None and pending != self._last_sent.get(key):
            # Trailing edge: no new message arrived during the window, so this is the settled
            # final value -- it has no single originating message, so no transaction to echo.
            self._send(key, pending, binding, None)

    def _send(
        self, key: tuple[int, str], encoded: str, binding: EgressBinding, transaction: str | None
    ) -> None:
        self._last_sent[key] = encoded
        task = asyncio.create_task(self._write_now(key[0], key[1], encoded, binding, transaction))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _write_now(
        self,
        entity_idx: int,
        cmd_name: str,
        encoded: str,
        binding: EgressBinding,
        transaction: str | None,
    ) -> None:
        try:
            await self._rest.put_datapoint(binding.rest_path, encoded)
        except NotFoundError as exc:
            # docs/06 §4.1: the datapoint is gone, so the model is stale -- a different failure
            # from a rejected value, and the only write error that says anything about topology.
            logger.warning(
                "command write for entity %d %r hit a 404; requesting a resync: %s",
                entity_idx,
                cmd_name,
                exc,
            )
            await self._respond_error(entity_idx, transaction, "set", str(exc))
            if self._on_topology_changed is not None:
                self._on_topology_changed()
            if binding.optimistic_attr is not None:
                await self._reconciler.reconcile_now(entity_idx, binding.optimistic_attr)
        except SysApError as exc:
            # F12: no retry; error to bridge/response; reconcile immediately rather than waiting
            # for the 3s timer, so the optimistic guess is corrected within one round trip.
            logger.warning("command write failed for entity %d %r: %s", entity_idx, cmd_name, exc)
            await self._respond_error(entity_idx, transaction, "set", str(exc))
            if binding.optimistic_attr is not None:
                await self._reconciler.reconcile_now(entity_idx, binding.optimistic_attr)

    # ---------------------------------------------------------------------------------- /get

    async def _handle_get(self, entity_idx: int, payload: bytes) -> None:
        decoded = _decode_payload(payload) if payload else {}
        entity = self._entities[entity_idx]
        attribute = decoded.get("attribute") if isinstance(decoded, dict) else None

        if attribute is not None:
            if attribute not in entity.attr_names:
                error = f"unknown attribute: {attribute!r}"
                await self._respond_error(entity_idx, None, "get", error)
                return
            targets = [entity.attr_names.index(attribute)]
        else:
            targets = [idx for idx, kind in enumerate(entity.attr_kinds) if kind == AttrKind.STATE]

        if not self._rate_limiter.allow(entity_idx):
            await self._respond_error(entity_idx, None, "get", "rate_limited")
            return

        for attr_idx in targets:
            await self._reconciler.read_one(entity_idx, attr_idx)

    # --------------------------------------------------------------------------- responses

    async def _respond_error(
        self, entity_idx: int, transaction: str | None, command: str, error: str
    ) -> None:
        # The single funnel for every command rejection -- validation, unknown command, unknown
        # attribute, rate limit -- so `command_errors` is counted here exactly once per error.
        self._metrics.command_errors += 1
        payload: dict[str, Any] = {
            "status": "error",
            "error": error,
            "id": self._entities[entity_idx].id,
        }
        if transaction is not None:
            payload["transaction"] = transaction
        await self._publish_response(command, payload)

    async def _respond_ok(self, entity_idx: int, transaction: str, command: str) -> None:
        payload: dict[str, Any] = {
            "status": "ok",
            "id": self._entities[entity_idx].id,
            "transaction": transaction,
        }
        await self._publish_response(command, payload)

    async def _publish_response(self, command: str, payload: dict[str, Any]) -> None:
        topic = topics.bridge_response_topic(self._base_topic, command)
        await self._mqtt.publish(topic, orjson.dumps(payload), qos=0, retain=False)
