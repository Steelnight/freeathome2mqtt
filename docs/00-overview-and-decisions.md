# 00 — Overview & Architecture Decisions

## 1. Problem statement

An ABB / Busch-Jaeger free@home installation exposes a local API on its System Access Point
(SysAP). That API is:

- **Flat and untyped.** Everything is a string. Device capability is encoded as a hex `functionID`
  on a channel, and the meaning of each individual value is encoded as a numeric `pairingID` on an
  input or output datapoint. There is no "this is a light" anywhere in the protocol.
- **Whole-world oriented.** The only way to learn the topology is `GET /api/rest/configuration`,
  which returns the *entire* installation — every device, channel, datapoint and its current
  value — as one JSON document. On a large install this is several megabytes.
- **Push-capable but lossy.** A WebSocket streams datapoint changes, but sends no application-level
  heartbeat, cannot accept commands, and gives you no way to know what you missed while
  disconnected.
- **Running on a small ARM box.** It will happily accept more concurrent requests than it can
  service, and then start returning `502` and dropping the WebSocket.

MQTT consumers — Home Assistant, openHAB, Node-RED, custom dashboards — want the opposite: stable
named entities, typed JSON payloads, retained last-known state, and availability signalling.

`freeathome2mqtt` is the translation layer. It is designed as **one self-contained module/service**
that owns both links and the mapping between them.

## 2. Goals

| Goal | Concrete target |
|---|---|
| Low event latency | WS frame → MQTT publish, p99 < 50 ms at default settings; < 10 ms with coalescing disabled |
| Burst tolerance | A scene touching 200 datapoints across 30 channels produces ≤ 30 state publishes |
| Protect the SysAP | Never more than `max_inflight` (default 4) concurrent HTTP requests; continuous commands debounced |
| Fast cold start | ≤ 3 s from process start to all entities discovered and state published, for 1 000 channels on a Raspberry Pi 4 |
| Correct after failure | Zero permanently-stale entities after any link loss, without an N-request storm |
| Small footprint | < 120 MB RSS at 1 000 channels; idle CPU < 0.5 % of one core |
| Extensible without code | New device types added as validated **data** files, not Python classes |
| Documented contract | The MQTT topic tree is the product; Home Assistant discovery is a layer on top |

## 3. Non-goals

- **Cloud API support.** Local only. The cloud API has different auth, different rate limits and
  different semantics; supporting both would compromise the design of each.
- **A web frontend.** zigbee2mqtt ships one; here it is out of scope. The bridge API over MQTT is
  complete enough that a frontend can be a separate project.
- **Arbitrary user code execution.** No external converters/extensions evaluated as code
  (see [ADR-011](#adr-011)).
- **Writing free@home configuration.** The bridge does not create rooms, assign channels, or
  reconfigure the installation. The one exception is *virtual device* registration, which is a
  first-class local-API feature and is supported.
- **Being a Home Assistant custom component.** That already exists and is good. This is for people
  who want MQTT as the integration bus.

## 4. Scale assumptions

Derived from real installations; the implementation should hold these as test parameters.

| Dimension | Typical | Design target | Stress test |
|---|---|---|---|
| Physical devices | 20–80 | 250 | 500 |
| Channels (→ entities) | 60–400 | 1 000 | 2 500 |
| Datapoints (outputs) | 300–2 000 | 6 000 | 15 000 |
| Config JSON size | 0.5–3 MB | 8 MB | 20 MB |
| Steady-state event rate | 0.1–5 /s | 50 /s | 500 /s |
| Burst (scene / all-off) | 50–200 frames | 500 frames | 5 000 frames |

## 5. Technology stack

| Concern | Choice | Why |
|---|---|---|
| Language / runtime | **Python 3.14.7, pinned exactly** | The only comprehensive open-source mapping of free@home function → semantics is Python ([`local-abbfreeathome`](https://github.com/kingsleyadam/local-abbfreeathome)). This workload is I/O-bound; the CPU-heavy parts (JSON, TLS, MQTT framing) are all C extensions. Pinned via `.python-version` and `requires-python == 3.14.7` (rather than a floating `>=`) so `uv sync` resolves the identical interpreter in dev, CI and the container image; bump all three together, deliberately, when moving to a newer patch. |
| Event loop | **uvloop** | 2–4× throughput over the stdlib loop, one line to install, no API change. Fall back to the stdlib loop where unavailable (e.g. some musl builds). |
| JSON | **orjson** | 3–10× faster than `json` and, crucially, parses the multi-MB config snapshot in a fraction of the time. Also faster to serialise the many small state payloads. |
| HTTP + WS client | **aiohttp** | Proven against SysAP quirks (self-signed certs, `502` on overload, WS heartbeat). Single `ClientSession` for connection reuse. |
| MQTT client | **aiomqtt** (over paho-mqtt v2) | Mature, correct reconnect semantics, async context-manager ergonomics. `gmqtt` is faster on paper but less maintained; revisit only if benchmarks in WP10 show MQTT framing is a bottleneck (it will not be). |
| Validation | **pydantic v2** | Rust core. Used for `config.yaml` and channel-profile validation **at load time only** — never on the hot path. |
| Hot-path data | `@dataclass(slots=True, frozen=True)`, plain `dict`/`list` | No validation, no attribute dicts, no property indirection on the event path. |
| Logging | stdlib `logging` with lazy `%`-style args | Zero formatting cost when a level is disabled. A `structlog` JSON sink is optional. |
| CLI / packaging | `argparse` + `hatchling`, developed with `uv` | Minimal dependency surface. |
| Container | `python:3.14.7-slim`, multi-arch `linux/amd64,linux/arm64,linux/arm/v7` | The primary deployment target is a Raspberry Pi next to the SysAP. Matches the pinned interpreter above exactly. |

**Rejected alternative: TypeScript/Node.** It would let us follow zigbee2mqtt's structure more
literally and its `mqtt.js` client is excellent. It was rejected because every ounce of free@home
*domain knowledge* in the open-source world — the function tables, the channel semantics, the
pairing-ID meanings, the quirks — lives in Python, and re-deriving it is the expensive part of this
project, not the plumbing. Performance is not a differentiator here: at 500 events/s the bridge is
nowhere near CPU-bound in either language.

---

## 6. Architecture Decision Records

### ADR-001
**Single process, task-based concurrency; no worker processes or threads on the data path.**

The workload is I/O-bound and the shared state (entity values, routing tables) is hot and mutable.
Multiprocessing would require serialising that state across a boundary for no gain. A single
asyncio loop with a small set of long-lived tasks keeps all state access lock-free.

*Exception:* blocking work that cannot be avoided — reading a CA certificate file, `ssl.create_default_context`,
writing the persistence snapshot — goes to `loop.run_in_executor`. Never call blocking I/O directly
from a coroutine; it stalls the WS reader and the SysAP will drop the connection.

---

### ADR-002
**`local-abbfreeathome` is a specification source, not a runtime dependency.**

The library is excellent for its purpose (a Home Assistant integration) but its shape is wrong for
a bridge:

- One Python class per device type, ~150–250 LOC each, ~40 of them.
- Per-event work is `O(pairings)`: `rsplit` the key, look up the output dict, then walk a chain of
  `if datapoint.get("pairingID") == Pairing.X.value` comparisons.
- `refresh_state()` issues **one HTTP GET per datapoint per channel** — a full resync of 1 000
  channels is thousands of requests against an embedded device.
- Callback fan-out is a `set` of zero-arg closures per attribute; the callee then re-reads the
  property. Fine for HA, wasteful for a bridge that already knows what changed.

What *is* valuable and will be reused: the `Pairing`, `Function`, `Parameter` and `Interface`
enumerations, and the `functionID → behaviour` knowledge encoded in its channel classes. These are
extracted into generated code tables and declarative profiles by `tools/gen_codes.py` (WP1).

**Licence note.** `local-abbfreeathome` is MIT; vendoring generated tables derived from it requires
retaining the MIT notice in `src/freeathome2mqtt/sysap/codes/NOTICE`. The upstream pairing IDs
originate from `Busch-Jaeger/node-free-at-home` (`src/pairingIds.ts`); check its licence before
vendoring and prefer generating from the vendor source where the licence permits. Resolve this in
WP0 before writing any generated file.

**Resolved in WP0.** `Busch-Jaeger/node-free-at-home`'s `package.json` declares an **ISC** licence —
permissive, MIT-compatible, requiring only retention of the copyright/permission notice. Both
upstream sources are therefore safe to vendor generated tables from. This project is licensed
**MIT** (see `LICENSE`), which is compatible with consuming both an MIT and an ISC source. When
`tools/gen_codes.py` lands in WP1, it must emit `src/freeathome2mqtt/sysap/codes/NOTICE` retaining:

- the `local-abbfreeathome` MIT copyright notice, for the `Pairing`/`Function`/`Parameter`/
  `Interface` enumerations and function→behaviour knowledge extracted from it, and
- the `Busch-Jaeger/node-free-at-home` ISC copyright notice, for any pairing ID sourced from its
  `src/pairingIds.ts`,

each attributed to its origin file. No generated file is committed before `NOTICE` exists alongside
it.

---

### ADR-003
**Device support is declarative data, not code.**

A channel type is described by a YAML **profile**: which `functionID`s it claims, which pairing IDs
map to which attributes, what codec each uses, which commands it accepts, and how it renders to
Home Assistant. Profiles are validated against a JSON Schema at load.

Compared with the alternatives:

| Approach | Cost of a new device type | Testability | Risk |
|---|---|---|---|
| A class per type (`local-abbfreeathome`) | ~200 LOC + a test module | Good | Verbose; behaviour drift between classes |
| Converter functions (`zigbee2mqtt`) | A JS function | Good | Arbitrary logic, hard to reason about statically |
| **Declarative profile (chosen)** | ~15 lines of YAML | Excellent — one table-driven test suite covers all | Needs an escape hatch for genuinely complex types |

The escape hatch: a profile may name a `transform:` referencing a small, in-tree, named Python
function for the handful of channels that need cross-datapoint logic (room temperature controller,
blind with slats). These are enumerated in [`docs/03`](03-model-and-profiles.md#7-complex-profiles-and-the-transform-escape-hatch),
and there should never be more than a dozen.

---

### ADR-004
**Compile the configuration into flat routing tables at load time.**

At startup, after fetching the configuration snapshot, walk every device → channel → datapoint
**once** and emit:

- `ingress: dict[str, Binding]` keyed by the exact WebSocket datapoint key
  (`"{serial}/{channel}/{odp}"`) → `(entity_index, attribute_index, codec, kind)`.
- `egress: dict[tuple[int, str], EgressBinding]` keyed by `(entity_index, command_name)` →
  the pre-formatted REST path `"{serial}.{channel}.{idp}"` plus the encoder.
- `discovery: list[bytes]` — Home Assistant payloads pre-serialised, ready to publish verbatim.

After compilation nothing on the hot path parses a key, scans a pairing list, formats a topic, or
renders a template. This is the single most important performance decision in the project; see
[`docs/05 §3`](05-performance.md#3-the-hot-path-rules).

---

### ADR-005
**State propagation uses a dirty set, not a FIFO queue.**

State is idempotent and last-write-wins: if a datapoint changes twice before we publish, only the
second value matters. A FIFO makes the publisher's work proportional to the *event* rate; a dirty
set makes it proportional to the number of *distinct entities* touched.

```
state[entity][attr] = value          # O(1), always applied
dirty.add(entity_index)              # O(1), naturally deduplicating
```

An "all off" scene across 200 datapoints on 30 channels becomes 30 publishes. The queue can never
back up, so there is no drop policy to get wrong and no unbounded memory growth.

**Events are exempt.** Button presses, scene triggers and doorbell rings are *edges*, not states —
coalescing them loses information. They take a separate, non-deduplicating, non-retained path. This
distinction is explicit in the profile (`kind: state | event`) and is the clean answer to the
problem zigbee2mqtt patches with its `CACHE_IGNORE_PROPERTIES` list.

---

### ADR-006
**Subscribe narrowly. Never subscribe to `<base_topic>/#`.**

zigbee2mqtt subscribes to its whole base topic and then filters out its own publishes using an
in-memory `Set` of published topics. That has three costs: the broker echoes every state publish
back to the bridge (doubling traffic), the `Set` grows without bound, and a *legitimate* external
publish to a topic the bridge has published is silently swallowed.

We subscribe to exactly:

```
freeathome2mqtt/+/set
freeathome2mqtt/+/set/+
freeathome2mqtt/+/get
freeathome2mqtt/bridge/request/#
homeassistant/status          # only if HA discovery is enabled
```

No echo, no filter set, no ambiguity. Command topics are strictly disjoint from state topics, so
there is no path by which the bridge can receive its own output.

---

### ADR-007
**Resynchronise from the configuration snapshot, not per-datapoint reads.**

`GET /api/rest/configuration` returns every output datapoint's **current value**. One request
therefore yields a complete, consistent picture of the whole installation. After any WebSocket
gap — reconnect, SysAP reboot, suspend/resume — the correct recovery is a single config fetch and a
diff against in-memory state, publishing only what changed.

The naive alternative (`refresh_state()` per channel, as the reference library does) is thousands
of requests, takes minutes, and can itself knock over the SysAP. Never do it.

Config fetches are hashed; if the hash is unchanged, discovery is not republished. Reload requests
are debounced (default 2 s) because pairing a new device emits a burst of config-change frames.

---

### ADR-008
**`bridge/state` means end-to-end health, not MQTT connectivity.**

The obvious implementation — LWT says `offline`, publish `online` when MQTT connects — is wrong
for this bridge. If the SysAP link dies while MQTT stays up, the bridge is "online" but every
retained value is a lie, and Home Assistant will keep showing stale state indefinitely.

`bridge/state` is therefore `online` only when **MQTT is connected AND the SysAP WebSocket is
connected AND the initial config load has succeeded.** Losing the SysAP publishes `{"state":"offline"}`,
which propagates through HA discovery's `availability_topic` and correctly marks every entity
unavailable. `bridge/info` carries the finer-grained breakdown for diagnostics.

There is a deliberate grace period (default 10 s) before flipping to `offline`, so a routine
reconnect does not flap every entity in Home Assistant.

---

### ADR-009
**The MQTT interface is the contract; Home Assistant discovery is an optional layer.**

Everything the bridge can do is reachable over plain, documented MQTT topics with plain JSON.
The `homeassistant/` discovery tree is generated *from* the same compiled model and can be
disabled entirely without losing any capability. This keeps openHAB, Node-RED and custom consumers
first-class, and keeps HA-specific concerns (component types, `value_template`, device registry
semantics) confined to one module.

---

### ADR-010
**Stable internal IDs; friendly names are aliases with a managed rename.**

Entity identity is `{device_serial}_{channel_id}` (e.g. `ABB7F500E17A_ch0003`) — derived from the
installation, stable across restarts, never changing.

The **topic segment** defaults to a slugified friendly name, because
`freeathome2mqtt/kitchen_ceiling` is what users want. Renaming is therefore a topic change, which
zigbee2mqtt handles badly: old retained topics are orphaned and automations break silently.

Our rename is a transaction (`bridge/request/entity/rename`):

1. Publish an empty retained payload to every old topic (state, availability, HA discovery).
2. Update the persisted alias map.
3. Republish discovery and state under the new topic.
4. Emit `bridge/event` with `{"type":"entity_renamed","from":...,"to":...,"id":...}`.

Every state payload also carries the immutable `id`, so a consumer can key on identity rather than
topic. `topic_style: id` is available for deployments that want topics that never move.

---

### ADR-011
**No user-supplied executable code. Extensibility is data plus the bridge API.**

zigbee2mqtt allows external converters and extensions as evaluated JavaScript. That is a large
attack surface and an unbounded support burden for a service holding installer credentials to a
building's electrical system.

Extension points here are:

- **Out-of-tree profiles** — drop a schema-validated YAML file into `profiles_dir`; it is merged
  over the built-ins. Covers "my device isn't supported yet".
- **The bridge API** — anything a plugin would do (rename, re-read, publish, reload) is available
  as an MQTT request/response, so external automation lives in the user's own process.
- **Raw passthrough** — an optional `raw` mode that publishes and accepts datapoints by their
  native address, so an advanced user can reach a channel the profiles do not yet model, without
  waiting for us.

---

### ADR-012
**Optimistic state with WebSocket reconciliation.**

A `/set` publishes the expected new state to MQTT immediately, then fires the REST `PUT` without
waiting for the response. Perceived latency becomes one MQTT round trip instead of MQTT + HTTP +
free@home bus.

The optimistic value is marked unconfirmed and started on a reconciliation timer (default 3 s). If
the SysAP echoes the change over the WebSocket, the mark clears. If it does not, the bridge issues
a single targeted `GET /api/rest/datapoint/...` for that datapoint and publishes the truth — which
may be a rollback.

This is safe because it is bounded: at most one reconciliation read per unconfirmed command, and
only for commands that were actually sent. It also *detects* the real failure mode of fire-and-forget
(`{"result": "..."}` not `OK`, or a `502` swallowed by the background task) rather than hiding it.

Optimistic publishing can be disabled per profile (`optimistic: false`) for channels where the
SysAP is authoritative in a way the bridge cannot predict — a room temperature controller's derived
mode, for example.

---

## 7. What "performance" means here, precisely

It is worth being blunt, because it shapes every later document. This bridge will never be
CPU-bound in normal operation. The things that actually go wrong in a bridge like this are:

1. **Blocking the WebSocket reader.** Any `await` on the WS task that can take longer than a few
   milliseconds — an MQTT publish to a slow broker, an HTTP request, a synchronous file write —
   stalls frame consumption. The SysAP's socket buffer fills, it drops the connection, and you lose
   events. *Mitigation: the WS task parses and hands off, nothing else.*
2. **Flooding the SysAP.** A dimmer slider produces 30–60 `/set` messages per second. Forwarded
   naively that is 60 TLS-bearing PUTs per second to an ARM box, which will start returning `502`
   and drop the WebSocket — so the *write* path destroys the *read* path.
   *Mitigation: debounce continuous commands, bound in-flight requests.*
3. **Flooding the broker.** Publishing every datapoint change separately, retained, at QoS 1,
   multiplies a 200-frame scene into 200 retained round trips. *Mitigation: coalescing dirty set,
   QoS 0 for state.*
4. **Startup cost.** Parsing an 8 MB config and rendering 1 000 discovery payloads with a template
   engine is seconds of CPU. *Mitigation: orjson, pre-serialised payloads, on-disk config cache
   keyed by hash.*
5. **Recovery cost.** The N-request resync described in ADR-007 is the single worst thing this
   class of software does. *Mitigation: one config fetch, diff, publish deltas.*

Every performance rule in [`docs/05`](05-performance.md) traces back to one of these five.
