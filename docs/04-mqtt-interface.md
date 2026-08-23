# 04 — The MQTT Interface

This is the product's public contract ([ADR-009](00-overview-and-decisions.md#adr-009)). It should
be treated as stable and versioned; breaking it needs a major version bump and a migration note.

Base topic is configurable (`mqtt.base_topic`, default `freeathome2mqtt`). `<base>` below means
that value. `<entity>` means the entity's topic segment ([`docs/03 §1.1`](03-model-and-profiles.md#11-entity-identity-and-naming)).

## 1. Topic map

```
<base>/bridge/state                     retained  QoS1   end-to-end health + LWT
<base>/bridge/info                      retained  QoS1   versions, links, counters, config summary
<base>/bridge/devices                   retained  QoS1   full inventory
<base>/bridge/event                                QoS0   topology and lifecycle events
<base>/bridge/logging                              QoS0   log stream (opt-in)
<base>/bridge/request/<command>                    QoS0   ← in
<base>/bridge/response/<command>                   QoS0   → out

<base>/<entity>                         retained  QoS0   entity state (JSON object)
<base>/<entity>/set                                QoS0   ← in, JSON object or scalar
<base>/<entity>/set/<attribute>                    QoS0   ← in, scalar
<base>/<entity>/get                                QoS0   ← in, force refresh
<base>/<entity>/availability            retained  QoS1   per-device availability (opt-in)
<base>/<entity>/event                              QoS0   edge events, never retained
```

Subscriptions taken by the bridge, and only these ([ADR-006](00-overview-and-decisions.md#adr-006)):

```
<base>/+/set        <base>/+/set/+        <base>/+/get
<base>/bridge/request/#
homeassistant/status                     (only when homeassistant.enabled)
```

### 1.1 QoS and retain rationale

| Topic class | QoS | Retain | Why |
|---|---|---|---|
| Entity state | 0 | **yes** | High volume. Retain already guarantees a late subscriber gets the last value, which is what QoS 1 would be buying — at 2–4× the packets. Loss of one state message is self-correcting: the next change resends the whole object. |
| Entity events | 0 | **no** | Retaining an edge event means it replays on every restart and fires automations spuriously. |
| Discovery | 1 | **yes** | Must arrive exactly once and survive an HA restart. Low volume, published rarely. |
| `bridge/state` | 1 | **yes** | Availability must not be lost. |
| `bridge/info`, `bridge/devices` | 1 | **yes** | Low volume, consumers expect them on connect. |
| Commands / responses | 0 | no | Interactive; a retained command would re-execute on reconnect, which is dangerous. |

`mqtt.force_disable_retain: true` exists for brokers with retained-message quotas; it degrades
gracefully (consumers see state only after the next change) and is logged loudly at startup.

## 2. Entity state

Topic: `<base>/<entity>` — retained JSON object, **always the complete state**, never a partial
patch. A consumer that receives one message has the full picture; there is no ordering requirement
and no need to merge.

```json
{
  "id": "ABB7F500E17A_ch0003",
  "state": true,
  "brightness": 43,
  "forced_position": "deactivated",
  "last_changed": "2026-08-23T14:31:02.118Z"
}
```

Rules:

- `id` is always present and immutable — key on it, not on the topic.
- Unknown/uninitialised datapoints are `null`, never `0` or `false`.
- `last_changed` is on by default and opt-out (`publish_last_changed`, default `true`). Note that it changes on every
  publish, which defeats byte-comparison deduplication for consumers; it is a genuine trade-off, so
  it is a flag.
- Attribute names are stable per profile. Adding an attribute to a profile is a minor change;
  renaming or removing one is breaking.

### 2.1 Events

Topic: `<base>/<entity>/event` — not retained.

```json
{ "id": "ABB7F500E17A_ch0002", "event": "press", "attribute": "button", "value": 1,
  "timestamp": "2026-08-23T14:31:02.118Z" }
```

Emitted for `kind: event` attributes: wall switches, triggers, doorbell rings, scene activations.
These bypass coalescing and change detection entirely — every edge produces exactly one message,
including two identical presses in a row.

## 3. Commands

### 3.1 Object form (preferred)

```
<base>/kueche_deckenlicht/set
{ "state": true, "brightness": 80 }
```

Multiple attributes in one message are applied in **profile-declaration order**, not JSON key
order, so `{"brightness": 80, "state": true}` and `{"state": true, "brightness": 80}` behave
identically. Profiles must declare on/off before level for this to do the intuitive thing.

Optional keys understood in the object:

| Key | Effect |
|---|---|
| `transaction` | Echoed in `bridge/response/set`; enables request/response correlation |
| `no_optimistic` | `true` suppresses the optimistic publish for this command only |

### 3.2 Attribute form

```
<base>/kueche_deckenlicht/set/brightness
80
```

The payload is a bare scalar (`80`, `true`, `"forced_on"`). Convenient from shell scripts and
Node-RED. JSON-decoded if it parses as JSON, otherwise treated as a string.

### 3.3 Scalar shorthand on `/set`

A bare `ON`, `OFF`, `true`, `false`, `0`, `1` published to `<base>/<entity>/set` is interpreted as
the entity's **primary** command — the profile's `primary:` key, defaulting to the first declared
command (`state` for most) ([`docs/03 §3.1`](03-model-and-profiles.md#31-schema-normative)). This
exists because Home Assistant's simpler component types and a lot of existing tooling send exactly
this.

### 3.4 `/get`

```
<base>/kueche_deckenlicht/get
{}                        # refresh all attributes
{"attribute": "state"}    # refresh one
```

Issues targeted `GET /api/rest/datapoint/...` reads and republishes state. **Rate limited** — one
`/get` per entity per 5 s, and the whole-bridge budget is shared with reconciliation, because a
loop publishing `/get` for 1 000 entities would take the SysAP down. Excess requests are answered
with a `rate_limited` error on `bridge/response/get` rather than silently dropped.

For a full refresh, use `bridge/request/reload` instead — one config fetch beats 1 000 datapoint
reads ([ADR-007](00-overview-and-decisions.md#adr-007)).

## 4. Bridge topics

### 4.1 `bridge/state`

```json
{ "state": "online" }
```

`online` only when MQTT is connected **and** the SysAP WebSocket is up **and** the initial load
succeeded ([ADR-008](00-overview-and-decisions.md#adr-008)). The LWT payload is `{"state":"offline"}`,
retained, QoS 1. There is a `availability.grace_seconds` (default 10) before a SysAP drop flips this,
so routine reconnects do not flap Home Assistant.

### 4.2 `bridge/info`

```json
{
  "version": "1.0.0",
  "commit": "abc1234",
  "sysap": {
    "name": "My Home",
    "serial": "ABB700ABCDEF",
    "version": "2.6.4",
    "hardware_version": "...",
    "uuid": "00000000-0000-0000-0000-000000000000",
    "url": "https://192.168.1.50"
  },
  "links": { "mqtt": "connected", "sysap_rest": "ok", "sysap_ws": "connected" },
  "counts": {
    "devices": 43, "channels": 187, "entities": 164,
    "unsupported_channels": 11, "orphan_channels_skipped": 12
  },
  "config": {
    "base_topic": "freeathome2mqtt", "topic_style": "friendly",
    "homeassistant": true, "coalesce_ms": 20, "max_inflight": 4
  },
  "stats": {
    "uptime_s": 84213,
    "ws_frames": 391044, "datapoints_in": 1204882, "unmapped_datapoints": 4021,
    "state_publishes": 88213, "events": 1044,
    "commands": 3012, "command_errors": 2,
    "reconnects_ws": 3, "reconnects_mqtt": 0,
    "config_reloads": 7, "codec_errors": 0,
    "latency_ms": { "p50": 4, "p95": 22, "p99": 38 }
  }
}
```

Republished on change and at most every 30 s. `stats` makes the performance claims in
[`docs/05`](05-performance.md) observable in production without a metrics stack.

### 4.3 `bridge/devices`

Retained inventory: every device, its channels, which profile matched, and — importantly —
**which channels did not match**, with their raw function IDs. This is what a user attaches to a
support request.

```json
[
  {
    "serial": "ABB7F500E17A",
    "name": "Kitchen Ceiling",
    "device_id": "910C",
    "article_number": "...",
    "interface": "TP",
    "area": "Küche",
    "unresponsive": false,
    "defect": false,
    "channels": [
      { "channel_id": "ch0003", "entity_id": "ABB7F500E17A_ch0003",
        "topic": "freeathome2mqtt/kueche_deckenlicht",
        "name": "Deckenlicht", "area": "Küche",
        "function_id": "0x12", "function": "FID_DIMMING_ACTUATOR",
        "profile": "dimming_actuator", "supported": true,
        "attributes": [ {"name":"state","codec":"bool01","kind":"state"},
                        {"name":"brightness","codec":"percent_int","kind":"state","unit":"%"} ],
        "commands":  [ {"name":"state","continuous":false},
                       {"name":"brightness","continuous":true,"range":[1,100]} ] },
      { "channel_id": "ch0007", "function_id": "0x2A1", "function": null,
        "supported": false, "reason": "no profile claims this function" }
    ]
  }
]
```

If the payload would exceed `mqtt.maximum_packet_size`, it is split into
`bridge/devices/<n>` parts with a `bridge/devices` index message that lists them. Publishing a
message the broker will reject is a silent failure mode worth engineering around.

### 4.4 `bridge/event`

```json
{ "type": "device_joined", "data": { "serial": "ABB7F5001234", "name": "New Dimmer" } }
```

Types: `device_joined`, `device_leave`, `device_unresponsive`, `device_responsive`,
`entity_added`, `entity_removed`, `entity_renamed`, `config_reloaded`, `sysap_disconnected`,
`sysap_reconnected`, `scene_triggered`.

### 4.5 `bridge/logging`

```json
{ "level": "warning", "message": "...", "logger": "bus.ingress", "ts": "..." }
```

Opt-in (`advanced.log_to_mqtt`, default `false`) and **rate limited** to 20 messages/s with a
dropped-count summary. A log storm published to MQTT while MQTT is the thing that is broken is a
classic self-amplifying outage.

Credentials are redacted centrally in `log.py` before any sink sees them, not per call site.

## 5. The bridge API

Request: `<base>/bridge/request/<command>` → response: `<base>/bridge/response/<command>`.

```json
// response envelope, always this shape
{ "status": "ok" | "error",
  "data": { ... },
  "error": "human-readable message",
  "transaction": "<echoed if supplied>" }
```

| Command | Payload | Effect |
|---|---|---|
| `reload` | `{}` | Re-fetch config, recompile, diff, publish deltas |
| `restart` | `{}` | Graceful shutdown; exit non-zero so the supervisor restarts |
| `entity/rename` | `{"id": "...", "name": "new_name"}` | Transactional rename ([ADR-010](00-overview-and-decisions.md#adr-010)) |
| `entity/options` | `{"id": "...", "options": {...}}` | Per-entity overrides: `optimistic`, `debounce_ms`, `homeassistant`, `enabled` |
| `entity/remove` | `{"id": "..."}` | Retract discovery and retained topics; exclude until next reload |
| `device/refresh` | `{"serial": "..."}` | `GET /api/rest/device/...`, update that device's entities only |
| `discovery/republish` | `{}` | Force a full HA discovery republish |
| `log_level` | `{"level": "debug"}` | Change level at runtime; no restart |
| `health` | `{}` | Returns the `bridge/info` body plus a pass/fail check list |
| `virtualdevice/create` | `{"serial":"...","type":"...","ttl":900,...}` | Create/refresh a virtual device ([`docs/01 §4.5`](01-freeathome-api.md#45-virtual-devices)) |

`entity/rename` and `entity/options` persist to `entities.json`
([`docs/07 §4`](07-configuration.md#4-persisted-state)).

## 6. Home Assistant discovery

Enabled by default (`homeassistant.enabled: true`). Discovery topic default `homeassistant`.
Refuses to start if `homeassistant.discovery_topic == mqtt.base_topic`, which would corrupt both
trees.

### 6.1 Topic and payload

```
homeassistant/<component>/<node_id>/<object_id>/config
```

- `<node_id>` = the entity id (`ABB7F500E17A_ch0003`) — stable, unique, and never changes on rename.
- `<object_id>` = the attribute, or the profile's `primary:` command for single-object components
  ([`docs/03 §3.1`](03-model-and-profiles.md#31-schema-normative)).

```json
{
  "name": "Deckenlicht",
  "unique_id": "ABB7F500E17A_ch0003",
  "object_id": "kueche_deckenlicht",
  "state_topic": "freeathome2mqtt/kueche_deckenlicht",
  "state_value_template": "{{ 'ON' if value_json.state else 'OFF' }}",
  "command_topic": "freeathome2mqtt/kueche_deckenlicht/set",
  "payload_on": "ON",
  "payload_off": "OFF",
  "brightness_state_topic": "freeathome2mqtt/kueche_deckenlicht",
  "brightness_value_template": "{{ value_json.brightness | int(0) }}",
  "brightness_command_topic": "freeathome2mqtt/kueche_deckenlicht/set/brightness",
  "brightness_scale": 100,
  "availability": [
    { "topic": "freeathome2mqtt/bridge/state", "value_template": "{{ value_json.state }}" },
    { "topic": "freeathome2mqtt/kueche_deckenlicht/availability",
      "value_template": "{{ value_json.state }}" }
  ],
  "availability_mode": "all",
  "device": {
    "identifiers": ["ABB7F500E17A"],
    "name": "Kitchen Ceiling",
    "manufacturer": "Busch-Jaeger / ABB",
    "model": "FID_DIMMING_ACTUATOR (0x12)",
    "hw_version": "910C",
    "suggested_area": "Küche",
    "via_device": "ABB700ABCDEF"
  },
  "origin": { "name": "freeathome2mqtt", "sw": "1.0.0",
              "url": "https://github.com/Steelnight/freeathome2mqtt" }
}
```

Points that matter:

- `unique_id` is the entity id → **renaming never creates a duplicate entity in Home Assistant.**
  This is the payoff for ADR-010's stable identity.
- `object_id` seeds HA's initial `entity_id`, so users get `light.kueche_deckenlicht`.
- `state_value_template`/`brightness_value_template` read the shared typed state topic and map it
  to HA's expected form — this is the default light schema, deliberately **not** `schema: json`
  (see §6.2.1); the state topic keeps carrying `"state": true`, not `"ON"`.
- `device.identifiers` is the **device serial**, so all channels of a 4-gang switch land under one
  HA device.
- `via_device` is the SysAP, so the whole installation forms a tree.
- `availability_mode: all` combines bridge health and per-device responsiveness correctly.
- Payloads are **pre-serialised at compile time** and published verbatim
  ([ADR-004](00-overview-and-decisions.md#adr-004)).

### 6.2 Component mapping

| Profile class | HA component | Notes |
|---|---|---|
| switch actuator | `switch` | value template over `value_json.state`; `device_class: outlet` where the channel says so |
| dimming actuator | `light` | default schema, `brightness_scale: 100`, value templates (§6.2.1) |
| colour-temperature actuator | `light` | default schema + `color_temp`, Kelvin bounds from channel parameters |
| cover actuators | `cover` | `position_topic`, `set_position_topic`; slats → `tilt_*` |
| window/door sensor | `binary_sensor` | `device_class: window` / `door` |
| movement detector | `binary_sensor` + `sensor` | occupancy + the brightness value it also reports |
| smoke / CO detector | `binary_sensor` | `device_class: smoke` / `carbon_monoxide` |
| temperature / brightness / humidity / CO₂ / VOC | `sensor` | `state_class: measurement` |
| room temperature controller | `climate` | Modes derived by a transform |
| valve / heating actuator | `valve` or `number` | Percentage actuation |
| door opener | `lock` | |
| switch sensor / trigger / door ring | `event` | HA's `event` platform — the right fit for edges |
| unsupported (raw mode) | `sensor` | `entity_category: diagnostic`, disabled by default |

Where "notes" say *value template*, that is the mechanism in §6.2.1 below, not the JSON light
schema.

### 6.2.1 State encoding — the wire stays typed, HA bridges it

The generic MQTT contract publishes **typed JSON** (`"state": true`, `"brightness": 43`,
`"position": 0`, `null` for unknown — [§2](#2-entity-state)). Home Assistant's own conventions
differ per component: the `light` **JSON schema** (`schema: json`) requires the payload's `state`
field to be the *strings* `"ON"`/`"OFF"`, and does **not** offer a template to remap a boolean.
Emitting `"ON"`/`"OFF"` on the shared state topic would leak an HA-specific encoding into the
documented contract, breaking openHAB/Node-RED consumers and ADR-009.

The rule, therefore: **stateful actuators (light, switch, lock, valve, cover, climate) use HA's
default/template schema, not `schema: json`**, and bridge the typed payload with value templates
over the *same* shared state topic — e.g. `state_value_template: "{{ 'ON' if value_json.state
else 'OFF' }}"`, `brightness_value_template: "{{ value_json.brightness | int(0) }}"`,
`position_template: "{{ value_json.position }}"`. `binary_sensor`, `sensor` and `event` already
read `value_json.<attr>` this way. The boolean↔`ON`/`OFF` (and percent, and enum) mapping thus
lives **entirely in the `homeassistant/` layer**, exactly where ADR-009 puts HA-specific concerns,
and the wire never carries an HA-ism.

> **⚠ verify against a real HA instance (WP10).** The *rule* above (typed wire, templates in
> discovery, no JSON light schema) is settled. The *exact* discovery keys per component —
> `on_command_type`, whether `switch` uses `value_template`+`state_on`/`state_off` vs.
> `payload_on`/`payload_off`, `brightness` behaviour when `state` is off, tilt/position keys for
> covers — must be confirmed against the running HA version before 1.0, because HA changes MQTT
> discovery keys between releases and the plan should not hardcode unverified ones.

### 6.3 Lifecycle

- **Publish** after compile, before state, retained QoS 1.
- **Skip unchanged.** Keep the last published bytes per topic; republish only on difference. On a
  no-op reload this publishes zero messages.
- **Retract** removed entities with an empty retained payload — otherwise HA resurrects them from
  the broker forever.
- **HA birth message.** Subscribe to `homeassistant/status`; on `online`, republish everything after
  a short delay (default 5 s, so HA's MQTT integration is fully up). This is how HA recovers after a
  purge or a fresh install.
- **Startup gate.** Do not publish discovery until the config load has succeeded. Publishing
  discovery for entities whose state never arrives gives users a screen full of "unavailable".

## 7. Raw mode

`advanced.raw_mode: true` adds, for **every** channel including unsupported ones:

```
<base>/raw/<serial>/<channel>/<datapoint>          retained, the raw string value
<base>/raw/<serial>/<channel>/<datapoint>/set      write a raw string
```

This is the pressure valve for unsupported hardware ([ADR-011](00-overview-and-decisions.md#adr-011)):
a user can drive a channel today and open a profile PR tomorrow. It is off by default because it
roughly doubles publish volume and exposes the un-abstracted protocol.

`raw_mode: unsupported_only` publishes raw topics only for channels with no matching profile — the
useful middle ground, and the recommended setting for anyone filing an issue.

## 8. Compatibility notes

- Topic segments never contain `+` or `#`; slugification guarantees `[a-z0-9_]` only, and the
  publisher asserts it (zigbee2mqtt had to add this guard after a live incident).
- Payloads are UTF-8 JSON. `bridge/devices` may be large; see §4.3.
- The bridge sets an MQTT client id of `freeathome2mqtt_<sysap_serial>` so two bridges against two
  SysAPs on one broker do not evict each other — a genuinely confusing failure to debug.
- MQTT 5 is used when the broker supports it (for `maximumPacketSize` and better disconnect reason
  codes), falling back to 3.1.1 automatically.
- After reconnect, retained messages are republished once (2 s later) for brokers that do not
  persist retained state across restarts. This is unconditional: the bridge does not subscribe
  to its own output topics ([ADR-006](00-overview-and-decisions.md#adr-006)), so it cannot
  detect whether the broker retained them, and a single idempotent republish is cheap
  ([`docs/06 §6`](06-resilience.md#6-failure-matrix) F8).
