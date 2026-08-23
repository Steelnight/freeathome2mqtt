# 09 — Pitfalls

A catalogue of the traps in this problem domain. Each has an ID so tests and code comments can
reference it (`# see P-14`). Most were observed in, or are avoided by design in, the reference
implementations; the rest are generic MQTT-bridge failure modes that this domain makes worse.

The **Test** column names the test that must exist to prove the mitigation works. An implementing
agent should treat these as a checklist: no pitfall is closed without its test.

---

## A. free@home protocol

### P-01 — Reading state from an input datapoint
**Symptom** A light switched at the wall never updates; only bridge-initiated changes appear.
**Cause** Reading `idpXXXX` (the last commanded value) instead of `odpXXXX` (device feedback).
**Mitigation** The profile schema has separate `attributes` (outputs) and `commands` (inputs). The
compiler searches only `outputs` for attributes and only `inputs` for commands, and fails the
profile at load if the pairing is on the wrong side.
**Test** `test_compiler_rejects_attribute_bound_to_input`

### P-02 — Writing to an output datapoint
**Symptom** Commands silently do nothing. No error anywhere.
**Cause** Writing `odpXXXX`.
**Mitigation** As P-01, enforced at compile time rather than discovered at runtime.
**Test** `test_compiler_rejects_command_bound_to_output`

### P-03 — Assuming input and output encodings match
**Symptom** Forced position reads back as `unknown` after being set successfully.
**Cause** `AL_FORCED` writes `3`/`2` but `AL_INFO_FORCE` reads `4`/`5`
([`docs/01 §6.4`](01-freeathome-api.md#64-forced-position)).
**Mitigation** Attribute `values` and command `values` are independent maps in the profile, in
opposite directions. Never derive one from the other.
**Test** `test_forced_position_roundtrip_asymmetric`

### P-04 — Coercing the empty string
**Symptom** An uninitialised window sensor reports "closed"; an unpaired temperature reads 0 °C. Both
look completely plausible and get acted on by automations.
**Cause** `""` → `bool("")` = `False`, or `int(float(""))` raising and being caught into a default.
**Mitigation** Every decoder maps `""` to `None`, centrally in `codecs.py`.
**Test** `test_all_codecs_map_empty_to_none` — parametrised over the whole codec registry, so a new
codec cannot be added without satisfying it.

### P-05 — `int("42.0")` raises
**Symptom** `ValueError` in the ingress loop; with a bare `except` the attribute silently stops
updating.
**Cause** Integer-typed datapoints arrive as `"42.0"`.
**Mitigation** `int(float(v))`, always.
**Test** `test_int_codec_accepts_float_strings`

### P-06 — `functionID` parsed as decimal
**Symptom** Channels randomly unsupported; `"1B"` raises, `"12"` silently matches the wrong function.
**Cause** `int(v)` instead of `int(v, 16)`. The decimal case is worse than the exception: it maps
function `0x12` (dimming actuator) onto function `12` (something else) with no error.
**Mitigation** One parse helper, used everywhere; unparseable values skip the channel and are
counted.
**Test** `test_function_id_hex_parsing`

### P-07 — Cover position inverted
**Symptom** "Open the blinds" closes them.
**Cause** free@home `0` = open, `100` = closed; HA and human intuition are the opposite
([`docs/01 §6.1`](01-freeathome-api.md#61-covers-are-inverted)).
**Mitigation** `percent_inverted` codec at the boundary; the raw orientation never reaches an MQTT
payload.
**Test** `test_cover_position_inversion_roundtrip`

### P-08 — Brightness `0` used for off
**Symptom** Setting brightness 0 leaves the light on at minimum, or the SysAP rejects the write.
**Cause** `AL_ABSOLUTE_SET_VALUE_CONTROL` accepts `1..100`.
**Mitigation** `range: [1, 100]` clamps; on/off is a separate command. HA's JSON light schema is
handled by the profile mapping `brightness: 0` to `state: false`.
**Test** `test_brightness_zero_maps_to_off`

### P-09 — Colour temperature treated as Kelvin
**Symptom** Every light is either maximally warm or maximally cool; nothing in between works.
**Cause** free@home uses `0..100` (0 = warmest). HA sends Kelvin/mireds.
**Mitigation** `color_temp_pct` codec bound per entity with that channel's
`PID_TEMPERATURE_COLOR_PHYSICAL_WARMEST` / `_COOLEST` parameters. Never hardcode 2700–6500 K.
**Test** `test_color_temp_uses_channel_parameters`

### P-10 — Hardcoding the SysAP UUID
**Symptom** Works for everyone except one user, for whom nothing works at all.
**Cause** Assuming `00000000-0000-0000-0000-000000000000`.
**Mitigation** Resolve from the configuration response's single key; cache; warn once and drop
frames keyed by anything else.
**Test** `test_non_default_sysap_uuid`

### P-11 — Confusing REST and WS datapoint separators
**Symptom** `400` on every write, or every WS key misses the ingress table.
**Cause** REST uses `serial.channel.datapoint`, WS uses `serial/channel/datapoint`.
**Mitigation** Both forms pre-computed at compile time; no runtime string building
([`docs/01 §2.1`](01-freeathome-api.md#21-datapoint-addressing--the-two-separators)).
**Test** `test_compiled_paths_use_correct_separators`

### P-12 — Ignoring `result` on a 200 response
**Symptom** Commands report success and do nothing.
**Cause** The SysAP returns HTTP 200 with `{"result": "<not OK>"}`.
**Mitigation** Check `result.lower() == "ok"`; treat anything else as a failure and reconcile.
**Test** `test_non_ok_result_is_a_failure`

### P-13 — Only handling the `datapoints` key
**Symptom** A device added, removed or renamed in the free@home app never appears until a restart.
**Cause** Ignoring `devices`, `devicesAdded`, `devicesRemoved`, `parameters`
([`docs/01 §5.1`](01-freeathome-api.md#51-frame-schema)). **Both reference implementations do this.**
**Mitigation** All keys handled; topology keys trigger a debounced recompile.
**Test** `test_devices_added_triggers_reload`, `test_devices_removed_retracts_discovery`

### P-14 — `floorplan.floors[x].rooms` is `null`
**Symptom** `AttributeError: 'NoneType' object has no attribute 'get'` during compile.
**Cause** A floor with no rooms serialises as `null`, not `{}`.
**Mitigation** Guard in the floorplan resolver; fixture with a null-rooms floor.
**Test** `test_floorplan_null_rooms`

### P-15 — Placeholder channel names
**Symptom** Entities called `Ⓐ` and `ⓑ`; slugs collapse to nothing.
**Cause** Multi-channel devices use circled letters as sub-channel display names.
**Mitigation** Fall back to the device name, then `Channel {id}`
([`docs/01 §6.5`](01-freeathome-api.md#65-placeholder-channel-names)).
**Test** `test_placeholder_channel_names_fall_back`

### P-16 — Virtual device TTL expiry
**Symptom** A virtual device the bridge created vanishes after 15 minutes.
**Cause** A finite `ttl` requires periodic re-registration.
**Mitigation** If the bridge creates virtual devices it owns a keepalive task refreshing at `ttl/2`,
stopped cleanly on shutdown.
**Test** `test_virtual_device_ttl_keepalive`

### P-17 — Hue/Sonos double-bridging
**Symptom** Lights flicker or oscillate; state ping-pongs between two integrations.
**Cause** Both the SysAP (via this bridge) and a native Hue integration drive the same lamp; each
sees the other's change as external and re-asserts.
**Mitigation** `interfaces` excludes `hue` and `sonos` by default, documented prominently.
**Test** `test_default_interface_filter_excludes_hue_sonos`

### P-18 — Orphan channel flood
**Symptom** 400 entities appear, most of them meaningless.
**Cause** Unassigned physical inputs and internal channels have no floor/room.
**Mitigation** `include_orphan_channels: false` by default; the count is reported in `bridge/info`
so the exclusion is visible rather than mysterious.
**Test** `test_orphan_filter_counts_reported`

### P-19 — Assuming every command echoes
**Symptom** Endless reconciliation reads for sensor and trigger channels.
**Cause** Not all channel types echo on the WebSocket
([`docs/01 §6.8`](01-freeathome-api.md#68-not-every-command-echoes)).
**Mitigation** `confirm: false` per command in the profile.
**Test** `test_no_reconcile_when_confirm_false`

### P-20 — Retrying `401` in a loop
**Symptom** The user is locked out of their own free@home app.
**Cause** Generic retry-everything logic applied to auth failures.
**Mitigation** `401`/`403`/`400` are never retried; only `502` and connection errors are
([`docs/06 §3`](06-resilience.md#3-backoff-policy)).
**Test** `test_auth_failure_is_not_retried`

---

## B. Connection and lifecycle

### P-21 — No WebSocket heartbeat
**Symptom** "It worked for three days then stopped." No error, no log line, `bridge/state` still
`online`.
**Cause** The SysAP sends no application heartbeat, so a dead TCP connection is indistinguishable
from an idle one. **The single nastiest failure in this system.**
**Mitigation** `heartbeat=30` on `ws_connect`, plus an independent 90 s idle watchdog
([`docs/06 §2`](06-resilience.md#2-the-heartbeat-again)).
**Test** `test_ws_reconnects_on_silence` (fake SysAP stops responding without closing)

### P-22 — Fetching the config before opening the WebSocket
**Symptom** Occasional entities permanently wrong after a restart, with no pattern.
**Cause** Changes occurring between the snapshot and the WS connect are lost forever.
**Mitigation** Connect and buffer first, then fetch, then drain
([`docs/02 §7`](02-architecture.md#7-startup-order)). **Both references have this race.**
**Test** `test_no_events_lost_during_startup_window`

### P-23 — N-request resync
**Symptom** Reconnecting takes minutes and sometimes takes the SysAP down with it.
**Cause** One `GET /api/rest/datapoint/...` per datapoint per channel.
**Mitigation** One configuration fetch ([ADR-007](00-overview-and-decisions.md#adr-007)).
**Test** `test_resync_issues_exactly_one_request`

### P-24 — Fixed retry interval, no jitter
**Symptom** A rebooting SysAP is held down by reconnect attempts from several clients in lockstep.
**Cause** `await asyncio.sleep(5)` in a loop.
**Mitigation** Exponential backoff with full jitter.
**Test** `test_backoff_is_exponential_and_jittered`

### P-25 — Blocking the WebSocket reader
**Symptom** Under load, events arrive late or are dropped; the SysAP closes the connection.
**Cause** Awaiting an MQTT publish, an HTTP request or a synchronous file write inside the reader.
**Mitigation** Rule R1 ([`docs/05 §3`](05-performance.md#3-the-hot-path-rules)); the reader hands off
to in-memory structures only.
**Test** `test_ws_reader_never_awaits_io` (a static check plus a load test asserting no frame loss
while the broker is artificially slowed)

### P-26 — Pausing ingestion when the broker is down
**Symptom** After a broker outage, entity states are stale until each one happens to change again.
**Cause** Treating an MQTT failure as a reason to stop consuming the WebSocket.
**Mitigation** Ingestion continues; the dirty set accumulates the union of changes and is published
on reconnect ([`docs/08 §9`](08-workflows.md#9-broker-outage)).
**Test** `test_state_correct_after_broker_outage`

### P-27 — Not re-subscribing after an MQTT reconnect
**Symptom** State keeps publishing; commands silently stop working.
**Cause** Assuming the broker preserved the session.
**Mitigation** Re-subscribe unconditionally on every connect. Idempotent and cheap.
**Test** `test_resubscribe_after_reconnect`

### P-28 — Wall-clock timers
**Symptom** After an NTP correction or a DST change, backoffs stall for hours or fire immediately.
**Cause** `time.time()` used for durations.
**Mitigation** `time.monotonic()` / `loop.time()` for every duration; wall clock only in payloads.
**Test** `test_timers_use_monotonic_clock`

### P-29 — Silent task death
**Symptom** The process is "up" but nothing publishes.
**Cause** An unhandled exception kills one task in a set of `create_task` calls nobody awaits.
**Mitigation** TaskGroup + restart shim + escalation to process exit
([`docs/02 §3.1`](02-architecture.md#31-task-supervision)).
**Test** `test_task_restart_and_escalation`

### P-30 — LWT armed too late
**Symptom** A crash during startup leaves consumers seeing the last `online` forever.
**Cause** Connecting to MQTT after the risky work.
**Mitigation** MQTT connect with LWT is step 3 of startup, before the SysAP is touched.
**Test** `test_lwt_armed_before_sysap_connect`

---

## C. MQTT and Home Assistant

### P-31 — Subscribing to `<base>/#`
**Symptom** Doubled broker traffic; a growing memory profile; occasional swallowed external
messages.
**Cause** Subscribing broadly and filtering out your own publishes with an ever-growing set.
**Mitigation** Narrow subscriptions ([ADR-006](00-overview-and-decisions.md#adr-006)).
**Test** `test_bridge_subscribes_only_to_command_topics`

### P-32 — Retaining edge events
**Symptom** Every restart fires "button pressed" automations.
**Cause** Publishing button/scene/doorbell events retained.
**Mitigation** `kind: event` → never retained, never coalesced, never deduplicated.
**Test** `test_events_are_not_retained`

### P-33 — Orphaned retained topics after a rename
**Symptom** Ghost entities in Home Assistant that cannot be deleted.
**Cause** Renaming changes the topic without clearing the old retained payloads.
**Mitigation** Transactional rename ([ADR-010](00-overview-and-decisions.md#adr-010)).
**Test** `test_rename_clears_old_retained_topics`

### P-34 — Duplicate HA entities after a rename
**Symptom** `light.kitchen_ceiling_2`.
**Cause** `unique_id` derived from the name.
**Mitigation** `unique_id` is the immutable entity id.
**Test** `test_unique_id_stable_across_rename`

### P-35 — Not retracting removed entities
**Symptom** Deleted devices reappear in HA on every restart.
**Cause** Discovery is retained; without an empty payload the broker keeps serving it forever.
**Mitigation** Retract on removal; `discovery.json` tracks what was published so retraction survives
a bridge restart too.
**Test** `test_removed_entities_are_retracted`

### P-36 — Ignoring the HA birth message
**Symptom** After reinstalling or purging HA's MQTT integration, no entities ever appear.
**Cause** Not subscribing to `homeassistant/status`.
**Mitigation** Subscribe; republish everything after `republish_delay`.
**Test** `test_ha_birth_triggers_republish`

### P-37 — Republishing discovery instantly on the birth message
**Symptom** Intermittently, entities do not appear after an HA restart.
**Cause** HA's MQTT integration is not ready when `online` is published.
**Mitigation** Wait `republish_delay` (default 5 s).
**Test** `test_ha_birth_republish_is_delayed`

### P-38 — Wildcards in topic names
**Symptom** Broker errors, or a publish that fans out unpredictably.
**Cause** A device named `Light #1` slugged carelessly.
**Mitigation** Slugification restricted to `[a-z0-9_]`, plus an assertion in the publisher.
**Test** `test_publish_rejects_wildcard_topics`

### P-39 — Slug collisions
**Symptom** Two channels share a topic; one overwrites the other's state, seemingly at random.
**Cause** "Deckenlicht" in two rooms.
**Mitigation** Deterministic collision resolution: area prefix, then channel id
([`docs/03 §1.1`](03-model-and-profiles.md#11-entity-identity-and-naming)); WARNING with both ids.
**Test** `test_slug_collision_resolution_is_deterministic`

### P-40 — Lossy umlaut transliteration
**Symptom** "Küche" becomes `kche`; "Straße" becomes `strae`.
**Cause** NFKD normalisation drops combining marks and does nothing for `ß`.
**Mitigation** Explicit German transliteration table before normalisation.
**Test** `test_slugify_german_umlauts`

### P-41 — Oversized `bridge/devices`
**Symptom** The inventory topic is never published; no error visible.
**Cause** Exceeding the broker's maximum packet size on a large installation.
**Mitigation** Split into indexed parts ([`docs/04 §4.3`](04-mqtt-interface.md#43-bridgedevices)).
**Test** `test_large_inventory_is_split`

### P-42 — Broker that does not persist retained messages
**Symptom** After a broker restart, consumers see nothing until something changes.
**Cause** Retained messages held only in memory.
**Mitigation** Republish the retained set once, 2 s after connect.
**Test** `test_retained_republish_after_reconnect`

### P-43 — Two bridges, one client id
**Symptom** Both bridges flap, each evicting the other, several times a second.
**Cause** A fixed client id.
**Mitigation** `freeathome2mqtt_<sysap_serial>` by default.
**Test** `test_client_id_includes_sysap_serial`

### P-44 — Log-storm amplification
**Symptom** A broker problem produces a log storm published to the broker, making it worse.
**Cause** `log_to_mqtt` with no rate limit.
**Mitigation** 20 msg/s cap with a dropped-count summary; MQTT logging off by default.
**Test** `test_mqtt_log_sink_is_rate_limited`

### P-45 — Credentials in logs or diagnostics
**Symptom** A user pastes a debug log into a public issue containing their installer password.
**Cause** Logging the config object, the auth header, or the SysAP URL with embedded credentials.
**Mitigation** Central redaction in `log.py`; `bridge/info` carries no secrets; `--capture`
pseudonymises.
**Test** `test_no_secrets_in_logs_or_bridge_info` — asserts against a config with distinctive
sentinel secrets, over the full startup log and every bridge topic.

---

## D. Performance and correctness under load

### P-46 — Forwarding every `/set` to the SysAP
**Symptom** Dragging a dimmer slider makes the SysAP unresponsive and drops the WebSocket — the
write path destroying the read path.
**Cause** 30–60 writes per second to an embedded device.
**Mitigation** Leading+trailing debounce on `continuous` commands; bounded in-flight requests
([`docs/05 §4.2`](05-performance.md#42-command-debouncing)).
**Test** `bench_command_debounce`, budget P5

### P-47 — Publishing per datapoint instead of per entity
**Symptom** A scene produces hundreds of retained messages; HA's recorder database grows fast.
**Cause** No coalescing.
**Mitigation** Dirty set + coalescing window
([ADR-005](00-overview-and-decisions.md#adr-005)).
**Test** `bench_burst`, budget P4

### P-48 — No change detection
**Symptom** Constant MQTT traffic on an idle installation.
**Cause** Publishing every received datapoint, including repeats.
**Mitigation** Compare before storing (rule R4).
**Test** `bench_dedup`, budget P12

### P-49 — `gather` over the initial publish
**Symptom** Startup hangs or takes minutes on a large installation.
**Cause** 2 000 concurrent publishes exceeding the broker's in-flight window (Mosquitto's default is
20).
**Mitigation** Sequential publishing; the client pipelines internally.
**Test** `test_initial_publish_is_sequential`

### P-50 — Eager f-string logging on the hot path
**Symptom** 30–40 % of CPU spent formatting strings that are discarded.
**Cause** `logger.debug(f"...")` — the f-string is evaluated regardless of level.
**Mitigation** Lazy `%` args (rule R7); a lint rule (`ruff` `G004`) enforces it.
**Test** enforced by lint, plus `bench_ingest` with debug logging configured but disabled

### P-51 — Holding the parsed config forever
**Symptom** RSS 3–4× the budget on large installations.
**Cause** Keeping the parsed configuration dict alive after compilation; in Python a 4 MB JSON
document is tens of megabytes of objects.
**Mitigation** Compile, extract, `del`; retain only the bytes needed for hashing
([`docs/05 §6`](05-performance.md#6-memory)).
**Test** `bench_memory`, budget P9

### P-52 — Unbounded `/get` amplification
**Symptom** A script publishing `/get` for every entity takes the SysAP down.
**Cause** No rate limit on a user-triggered read path.
**Mitigation** Per-entity 5 s limit, shared global budget with reconciliation, explicit
`rate_limited` errors rather than silent drops
([`docs/04 §3.4`](04-mqtt-interface.md#34-get)).
**Test** `test_get_storm_is_rate_limited`

### P-53 — Optimistic state with no reconciliation
**Symptom** The UI shows a light on that never turned on.
**Cause** Fire-and-forget writes with an optimistic publish and no verification.
**Mitigation** Unconfirmed marks + reconciliation timer
([ADR-012](00-overview-and-decisions.md#adr-012)).
**Test** `test_unconfirmed_command_is_reconciled`

### P-54 — Non-deterministic compilation
**Symptom** Entity topics change between restarts; HA accumulates duplicates.
**Cause** Iterating over sets, or resolving ties by dict ordering that depends on input order.
**Mitigation** Deterministic ordering everywhere in the compiler; asserted directly.
**Test** `test_compiler_is_deterministic` (compile twice from shuffled inputs, compare byte-for-byte)

### P-55 — Reload storms
**Symptom** Pairing a new device causes dozens of full config fetches in a minute.
**Cause** One reload per topology frame.
**Mitigation** 2 s debounce, 30 s minimum interval, reload-again flag instead of queueing
([`docs/05 §4.3`](05-performance.md#43-config-reload-debouncing)).
**Test** `test_reload_debounce_and_rate_limit`

### P-56 — Publishing `last_changed` on unchanged state
**Symptom** Consumers that deduplicate by payload comparison see every message as new.
**Cause** A timestamp in every payload.
**Mitigation** `last_changed` only updates when something actually changed (guaranteed by change
detection); the field is a documented flag because the trade-off is real.
**Test** `test_last_changed_only_updates_on_change`

---

## E. Process and ecosystem

### P-57 — Copying reference code without its context
**Symptom** Importing the anti-patterns in [`docs/05 §7`](05-performance.md#7-anti-patterns--explicitly-do-not-do-these)
along with the domain knowledge.
**Cause** The reference implementations are correct **for a Home Assistant integration**, where the
event volume is low and HA supplies the session, the scheduler and the entity registry. A bridge has
none of that.
**Mitigation** Take *semantics* (pairing meanings, function mappings, quirks) from the references;
take *structure* from this plan.

### P-58 — Vendoring generated tables without checking the licence
**Symptom** A licence incompatibility discovered after release.
**Cause** Pairing/function tables originate in vendor code with unclear terms.
**Mitigation** Resolve in WP0, before any generated file is committed; retain notices in
`sysap/codes/NOTICE` ([ADR-002](00-overview-and-decisions.md#adr-002)).

### P-59 — No way to report an unsupported device
**Symptom** Users open issues saying "my device doesn't work" with nothing actionable.
**Cause** Unsupported channels are invisible.
**Mitigation** `bridge/devices` lists them with function IDs; `raw_mode: unsupported_only` lets the
user drive them meanwhile; `--capture` produces an attachable, pseudonymised fixture.
**Test** `test_unsupported_channels_are_reported`

### P-60 — Untested against a real installation
**Symptom** Everything passes CI; nothing works on real hardware.
**Cause** Fixtures written from the documentation rather than captured from a device.
**Mitigation** Every **⚠ verify empirically** marker in [`docs/01`](01-freeathome-api.md) is a WP1
task with a capture attached; CI runs against at least one real captured configuration fixture.
