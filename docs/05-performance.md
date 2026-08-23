# 05 — Performance

The request that started this project was "focus on performance". This document says what that
means concretely, what the budgets are, which rules are non-negotiable, and how each claim is
measured.

## 1. Budgets

Measured on the reference target — **Raspberry Pi 4, 2 GB, Python 3.13, uvloop, local Mosquitto** —
with the 1 000-channel synthetic installation from [`docs/10 §3`](10-testing.md#3-fixtures-and-the-fake-sysap).
CI enforces these as regression gates with a 25 % tolerance band.

| # | Metric | Budget | Measured by |
|---|---|---|---|
| P1 | WS frame → MQTT publish, p99, `coalesce_ms: 20` | **≤ 50 ms** | `bench_latency` |
| P2 | Same, `coalesce_ms: 0` | **≤ 10 ms** | `bench_latency` |
| P3 | Sustained ingest without falling behind | **≥ 5 000 datapoints/s** | `bench_ingest` |
| P4 | Scene burst: 500 datapoints over 40 entities → publishes | **≤ 40** | `bench_burst` |
| P5 | Slider drag: 60 `/set` over 2 s → SysAP writes | **≤ 6** | `bench_command_debounce` |
| P6 | Cold start (process → `bridge/state: online`), 1 000 channels | **≤ 3 s** | `bench_startup` |
| P7 | Of which: config parse + compile (walk/match/resolve/bind; **excludes** discovery pre-render, which `bench_compile` measures as a separate line) | **≤ 400 ms** | `bench_compile` |
| P8 | Resync after a 60 s WS outage | **≤ 1.5 s**, **1** HTTP request | `bench_resync` |
| P9 | RSS at 1 000 entities, steady state | **≤ 120 MB** | `bench_memory` |
| P10 | Idle CPU (0.1 events/s) | **≤ 0.5 %** of one core | `bench_idle` |
| P11 | Concurrent SysAP requests, ever | **≤ `max_inflight`** | asserted in the fake SysAP |
| P12 | Unchanged-value frames producing MQTT traffic | **0** | `bench_dedup` |

P12 deserves emphasis. In real installations a large fraction of WebSocket frames carry a value
identical to the one already held — sensors re-report, actuators echo, scenes re-assert. Change
detection is a three-line check that removes most of the publish volume for free, and it is the
highest-leverage optimisation in the whole system.

## 2. Where the time actually goes

Rough expected distribution at the design target. Numbers are order-of-magnitude, to direct
attention rather than to be believed precisely.

**Cold start, 1 000 channels / 4 MB config:**

| Phase | ~Cost | Notes |
|---|---|---|
| HTTP fetch of the config | 300–800 ms | SysAP-bound; the device is the slow part, not us |
| `orjson.loads` of 4 MB | 25–40 ms | ~10× faster than stdlib `json` here |
| Compile (walk, match, resolve, bind) | 150–250 ms | Pure Python; the biggest slice we control |
| Pre-render + serialise 1 000 discovery payloads | 60–120 ms | Done once; zero at runtime |
| Publish 1 000 discovery (QoS 1) + 1 000 state (QoS 0) | 400–900 ms | Broker-bound; QoS 1 round trips dominate |
| MQTT + WS connect, TLS | 100–300 ms | |

**Steady state, per datapoint:** a dict lookup, a small decode, a comparison. Under a microsecond.
The loop overhead of iterating a frame's `datapoints` dict dominates, which is why frames are parsed
whole rather than per-key.

**Per publish:** `orjson.dumps` of a small dict (~1–3 µs) plus MQTT framing and a socket write.

The conclusion that should shape every implementation choice: **we are broker-bound and
SysAP-bound, never CPU-bound.** Optimisation effort belongs in *reducing message and request count*,
not in shaving Python cycles. Every rule below follows from that.

## 3. The hot-path rules

These are non-negotiable and should be enforced in review.

**R1 — `ws_reader` awaits nothing but the socket.**
No MQTT publish, no HTTP request, no file I/O, no `asyncio.Lock`, no executor call. It parses and
hands off to in-memory structures. Blocking it fills the SysAP's send buffer and the SysAP drops
the connection — the write path silently destroys the read path.

**R2 — No string parsing on the hot path.**
The WebSocket datapoint key is used *verbatim* as a dict key. No `split`, `rsplit`, `partition`,
`startswith`, f-string or `format()` between frame receipt and state update. Everything that needs
parsing was parsed at compile time.

**R3 — No `O(n)` scans per event.**
Never iterate a channel's outputs to find a pairing, never iterate entities to find a match. Every
lookup on the hot path is a dict hit or a list index.

**R4 — Change detection before anything else.**
Compare the decoded value against the stored one and return early if equal. This gates all
downstream work: no dirty mark, no publish, no transform, no HA traffic (budget P12).

**R5 — No per-event allocation beyond the decoded value.**
No temporary dicts, no tuples for keys, no exception objects in the normal path. `Binding` is a
frozen slots dataclass so attribute access compiles to a slot read.

**R6 — Publishing is proportional to entities, not events.**
The dirty set is the mechanism ([ADR-005](00-overview-and-decisions.md#adr-005)). Never publish
per-attribute; publish the entity's whole payload once.

**R7 — Logging on the hot path is lazy and level-gated.**
`log.debug("dp %s=%s", key, value)` — never an f-string, never `stringify(payload)` evaluated before
the level check. A debug f-string in the ingress loop costs more than everything else in it
combined, and it costs that even when debug is off.

**R8 — Bound every outbound path to the SysAP.**
The semaphore is not advisory. If the code can issue an unbounded number of HTTP requests in
response to anything — a config reload, a `/get` storm, a reconciliation sweep — that is a bug, not
a tuning issue.

**R9 — One `ClientSession`, one WebSocket, one MQTT client, for the process's lifetime.**
Creating a session per request means a TLS handshake per request to a device that struggles with
one.

**R10 — Validation happens at load time, not per message.**
pydantic is for `config.yaml` and profiles. The ingress path sees only pre-validated, pre-bound
structures.

## 4. Coalescing

### 4.1 State publishing

```python
async def publisher_loop(self) -> None:
    while True:
        await self.wake.wait()
        self.wake.clear()

        if self.coalesce_ms:
            await asyncio.sleep(self.coalesce_ms / 1000)

        batch, self.dirty = self.dirty, set()
        for idx in batch:
            e = self.entities[idx]
            await self.mqtt.publish(e.state_topic,
                                    orjson.dumps(self.build_payload(idx)),
                                    qos=0, retain=True)
        self.metrics.publishes += len(batch)
```

- `coalesce_ms` (default **20**) is the maximum added latency and the coalescing window in one
  number. `0` disables it for latency-critical installs; the dirty set still deduplicates within a
  single frame, so even at `0` a scene burst collapses substantially.
- The set swap is atomic under a single-threaded loop — no lock, no copy.
- Publishing is sequential, not `gather`ed. `gather` over 1 000 publishes creates 1 000 tasks and
  makes ordering unpredictable for no throughput gain, since the MQTT client serialises onto one
  socket anyway.

**Adaptive variant** (optional, `coalesce_adaptive: true`): if the batch size exceeds
`coalesce_burst_threshold` (default 25), grow the next window up to `coalesce_max_ms` (default 200);
shrink back geometrically when batches are small. This keeps single-button-press latency at ~0 ms
while making an "all lights off" scene cost one round of publishes. Implement it only after P1–P4
pass without it, and keep it behind a flag.

### 4.2 Command debouncing

```python
class CommandCoalescer:
    pending: dict[tuple[int, str], PendingCommand]     # (entity_idx, command) -> latest
    timers:  dict[tuple[int, str], asyncio.TimerHandle]
```

This is a leading-edge **throttle**, not a quiescence-reset debounce — it deliberately keeps
streaming intermediate values during a continuous drag rather than sending nothing until the user
pauses (which would make a slow drag "arrive late"):

1. First `/set` for a key → send immediately, start the window.
2. Further `/set` inside the window → overwrite `pending`, do **not** send.
3. Window closes → if `pending` differs from what was last sent, send it and restart the window.

The number of writes for a continuous drag of duration `D` is therefore `1 + floor(D / window)`
(leading edge plus one per elapsed window), bounded above by `2 + D/window`
([`docs/10 §5`](10-testing.md#5-property-based-tests)). A 2-second slider drag at 30 Hz produces
60 messages and **≤ 6 writes** at the default window: the first position immediately, then the
latest position roughly every window until the drag ends (budget P5). Latency stays interactive
because of the leading edge; the SysAP stays alive because the write rate is capped at
`1000/window` per second — well under the ≤ 10/s sustained-write budget
([`docs/01 §8`](01-freeathome-api.md#8-rate-and-concurrency-limits)).

Only `continuous: true` commands are throttled ([`docs/03 §3.3`](03-model-and-profiles.md#33-command-object)).
Discrete commands go straight through — throttling on/off would make light switches feel broken.

Throttle window default **350 ms** (≈ 6 writes over a 2 s drag, ≈ 3 writes/s), overridable per
entity via `bridge/request/entity/options`. Lowering it trades SysAP load for responsiveness; a
window below 100 ms breaches the ≤ 10/s sustained-write budget during a continuous drag.

### 4.3 Config-reload debouncing

`devices` / `devicesAdded` / `devicesRemoved` / `parameters` frames arrive in bursts (pairing a
device can emit dozens). Reloads are debounced 2 s and rate-limited to one per 30 s. A reload while
one is in flight sets a "reload again when done" flag rather than queueing another.

## 5. Startup optimisation

**Config cache.** After a successful load, persist the raw config bytes plus their hash to
`cache/config.json.zst`. On start, fetch the live config, hash it, and if the hash matches, reuse the
already-parsed compile artefacts. This does not avoid the HTTP fetch (we must know the config is
current) but it does avoid parse + compile + discovery rendering — roughly 400 ms of the 3 s budget.

**Publish order.** Discovery (QoS 1) before state (QoS 0) before `bridge/state: online`. Home
Assistant creates entities from discovery and then immediately fills them from the retained state,
so users never see a screen of "unknown".

**Do not `gather` the initial publish.** 2 000 concurrent `publishAsync` calls will exceed a
default Mosquitto's `max_inflight_messages` (20) and stall. Publish sequentially; the client
pipelines internally.

**Discovery is skipped when unchanged.** Startup after a restart with no config change publishes
zero discovery messages, because the last-published bytes are persisted alongside the entity map.

## 6. Memory

| Item | 1 000 entities |
|---|---|
| `Entity` objects (slots, ~14 fields) | ~1.4 MB |
| `ingress` dict, 6 000 entries | ~2.5 MB |
| `values` as `list[list]` | ~0.6 MB |
| Pre-serialised discovery payloads | ~1.5 MB |
| Parsed config **released after compile** | 0 |
| aiohttp + aiomqtt + uvloop + interpreter | ~50–70 MB |

The big one is the last row: the Python baseline dominates, so micro-optimising our own structures
is pointless. What *does* matter is **releasing the parsed configuration after compilation** —
holding a 4 MB parsed dict (which in Python is more like 30–60 MB of objects) for the process's
lifetime is the single easiest way to blow the budget. Compile, extract, `del`, and keep only the
raw bytes needed for hashing.

Two known unbounded-growth traps, both taken from real bridges:

- A published-topics set used for echo suppression — avoided entirely by ADR-006.
- A per-entity history/last-seen dict that is written but never pruned. If `last_changed` is
  enabled, store it in the entity's slot, not a side dict.

## 7. Anti-patterns — explicitly do not do these

Each of these appears in one of the reference implementations. They are called out by name so an
implementing agent copying reference code recognises them.

| Anti-pattern | Where seen | Cost | Instead |
|---|---|---|---|
| `refresh_state()` doing one HTTP GET per datapoint per channel | `local-abbfreeathome` | Thousands of requests; minutes; can take the SysAP down | One config fetch + diff ([ADR-007](00-overview-and-decisions.md#adr-007)) |
| `if pairingID == X: elif == Y: ...` chains per event | `local-abbfreeathome` | `O(pairings)` per datapoint, plus `.get()` allocations | Compiled `Binding` ([ADR-004](00-overview-and-decisions.md#adr-004)) |
| Subscribe `base/#`, filter own publishes via a Set | `zigbee2mqtt` | Doubles broker traffic; unbounded memory; swallows legitimate messages | Narrow subscriptions ([ADR-006](00-overview-and-decisions.md#adr-006)) |
| Handling only `datapoints` from the WS frame | both | Device add/remove/rename invisible until restart | Handle all frame keys ([`docs/01 §5.1`](01-freeathome-api.md#51-frame-schema)) |
| Fetching config *before* connecting the WS | both | Silent permanent loss of changes in the gap | Connect + buffer first ([`docs/02 §7`](02-architecture.md#7-startup-order)) |
| Fixed 5 s retry with no backoff or jitter | `local-abbfreeathome` | Reconnect storms against a rebooting SysAP | Exponential backoff + jitter ([`docs/06 §3`](06-resilience.md#3-backoff-policy)) |
| A property getter per attribute, re-read by callbacks | `local-abbfreeathome` | Indirection per access; callee re-derives what the caller already knew | Positional slots; pass the changed value |
| `CACHE_IGNORE_PROPERTIES` string list to suppress event-like attributes | `zigbee2mqtt` | Pattern matching at runtime; easy to get wrong | `kind: state \| event` in the profile ([ADR-005](00-overview-and-decisions.md#adr-005)) |
| Retaining button/scene events | common | Automations fire on every restart | Events are never retained ([`docs/04 §2.1`](04-mqtt-interface.md#21-events)) |
| `logger.debug(f"...")` on the hot path | common | Formatting cost paid even when disabled | Lazy `%` args (R7) |
| Rendering HA `value_template`s in the bridge | — | Template engine per publish | Templates are HA's job; we publish plain JSON |

## 8. Benchmarks

`tests/bench/`, run with `pytest-benchmark`. All run against the fake SysAP and an embedded broker,
so they are hermetic and can gate CI.

| Bench | Scenario | Asserts |
|---|---|---|
| `bench_latency` | 1 000 single datapoint changes, timestamped in/out | P1, P2 |
| `bench_ingest` | 60 s of synthetic traffic at 5 000 dp/s | P3; publisher never falls behind by > 1 window |
| `bench_burst` | One frame with 500 datapoints across 40 entities | P4 |
| `bench_command_debounce` | 60 `/set` over 2 s on one continuous command | P5 |
| `bench_startup` | Cold start against a 1 000-channel fixture | P6, P7 |
| `bench_resync` | Kill the WS for 60 s with changes meanwhile, restore | P8; exactly 1 config request |
| `bench_memory` | `tracemalloc` + RSS after 10 min steady state | P9; no growth trend over the window |
| `bench_idle` | 10 min at 0.1 events/s | P10 |
| `bench_dedup` | 10 000 frames all repeating current values | P12: zero publishes |
| `bench_compile` | Compile only, 1 000 / 2 500 channels | P7; near-linear scaling |

Results are written to `bench/results.json` and compared against a committed baseline; CI fails on a
regression beyond tolerance. This turns the budgets above from aspiration into a contract.

## 9. Profiling recipe

When a budget fails, in this order:

1. `py-spy top --pid <pid>` against the running bridge under `bench_ingest`. If the top frames are
   not `orjson`, socket writes, or the ingress loop, something is wrong architecturally.
2. `py-spy record -o flame.svg` for 30 s of the failing scenario.
3. `tracemalloc` snapshots 60 s apart for memory growth; diff by allocation site.
4. Instrument counters rather than guessing: `datapoints_in` vs `state_publishes` vs `commands`
   in `bridge/info` usually localise the problem to ingress, egress or the broker without a profiler
   at all.

If a fix requires breaking one of the R-rules in §3, that is a design change and belongs in an ADR,
not in a commit.
