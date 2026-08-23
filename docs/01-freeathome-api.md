# 01 — The free@home Local API

Everything in this document is what the implementation must assume about the SysAP. Where a detail
is inferred rather than confirmed from the vendor's own OpenAPI models it is marked
**⚠ verify empirically** — WP1 includes a capture tool precisely so these get confirmed against a
real device.

Authoritative sources used:

- `Busch-Jaeger/node-free-at-home` — vendor library, `src/fhapi/models/*.ts` are OpenAPI-generated
  and are the wire schema.
- `kingsleyadam/local-abbfreeathome` v3.7.0 — battle-tested client behaviour and quirk handling.
- ABB Developer Portal, `developer.eu.mybuildings.abb.com/fah_local/` — prose documentation
  (concepts / samples / prerequisites).

---

## 1. Prerequisites and access

| Requirement | Detail |
|---|---|
| Firmware | SysAP **≥ 2.6.0**. Check via `GET /settings.json` → `flags.version`. |
| Activation | The end user must enable it: *free@home next App → More → free@home Settings → Local API → Activate*. There is no way to enable it remotely. |
| Username | `installer` on current firmware. |
| Password | The user's app/web-interface password. |
| Transport | `http://<host>` or `https://<host>`. The SysAP's TLS certificate is **self-signed**; a CA file can be downloaded from the Local API settings screen. |
| Auth scheme | HTTP **Basic**, on every REST request *and* on the WebSocket upgrade request. |

### 1.1 The unauthenticated settings endpoint

```
GET http://<host>/settings.json
```

Returns, without authentication:

```jsonc
{
  "flags": {
    "version": "2.6.4",
    "hardwareVersion": "...",
    "serialNumber": "ABB700...",
    "name": "My Home"
  },
  "users": [
    { "name": "installer", "jid": "<uuid>@busch-jaeger.de", ... }
  ]
}
```

Use it for three things:

1. **Pre-flight capability check** — refuse to start with a clear message if `version < 2.6.0`,
   rather than failing on a confusing `401`/`404` later.
2. **Identity** — `flags.serialNumber` is the stable identifier of the installation and is the
   right thing to use as the bridge's own device serial in Home Assistant, and as part of the MQTT
   client ID.
3. **Username fallback** — on some firmware the Basic-auth *username* must be the user's `jid`
   rather than the display name. If auth with the configured username returns `401`, retry once
   with the `jid` looked up from `users[]` before reporting bad credentials.
   **⚠ verify empirically** — current firmware accepts `installer` directly; keep the fallback,
   log it at INFO when it triggers.

> Note this endpoint is **not** under `/fhapi/v1` and does **not** take auth.

---

## 2. REST surface

Base path: `/fhapi/v1`. All responses are keyed by the SysAP UUID (§3).

| Method | Path | Purpose | Used by us |
|---|---|---|---|
| `GET` | `/api/rest/configuration` | Whole installation: devices, channels, datapoints **with current values**, floorplan, users | **Yes** — startup, resync, periodic refresh |
| `GET` | `/api/rest/devicelist` | `{ sysap: [serial, ...] }` | Optional cheap liveness probe |
| `GET` | `/api/rest/device/{sysap}/{device}` | One device's full record | Targeted refresh after `devicesAdded` |
| `GET` | `/api/rest/datapoint/{sysap}/{device}.{channel}.{datapoint}` | One value, as `{"values": ["1"]}` | Reconciliation of unconfirmed commands only |
| `PUT` | `/api/rest/datapoint/{sysap}/{device}.{channel}.{datapoint}` | Write a value; body is the **raw string**, not JSON | **Yes** — every command |
| `PUT` | `/api/rest/virtualdevice/{sysap}/{serial}` | Create / refresh a virtual device | Optional feature (WP9) |
| `GET` | `/api/rest/sysap` | SysAP metadata | Diagnostics |
| `GET` | `/api/rest/pairings` | Pairing-ID catalogue as the device knows it | Code generation / diagnostics |
| `POST` | `/api/rest/notification` | Push a notification to the SysAP UI | Out of scope |
| `PUT` | `/api/rest/auxiliarydata/{sysap}/{device}/{channel}/{index}` | Auxiliary data | Out of scope |

### 2.1 Datapoint addressing — the two separators

This trips everyone up:

```
REST:       {device_serial}.{channel_id}.{datapoint_id}     ← dots
WebSocket:  {device_serial}/{channel_id}/{datapoint_id}     ← slashes
```

Example: `ABB7F500E17A.ch0003.idp0000` (REST) vs `ABB7F500E17A/ch0003/odp0000` (WS).

The compiler ([ADR-004](00-overview-and-decisions.md#adr-004)) pre-formats both forms so no string
manipulation happens at runtime.

### 2.2 Writing a datapoint

```http
PUT /fhapi/v1/api/rest/datapoint/00000000-0000-0000-0000-000000000000/ABB7F500E17A.ch0003.idp0000
Authorization: Basic aW5zdGFsbGVyOi4uLg==
Content-Type: text/plain

1
```

Response, HTTP 200:

```json
{ "00000000-0000-0000-0000-000000000000": { "result": "OK" } }
```

**A 200 does not mean success.** Check `result` case-insensitively against `"ok"`. A non-OK result
with a 200 status is a real, observed failure mode.

### 2.3 Error codes

| Status | Meaning | Correct response |
|---|---|---|
| `400` | Malformed request (bad datapoint address, bad value) | Do not retry. Log the offending address and value. Almost always a profile bug. |
| `401` | Bad credentials | Do not retry. Try the `jid` fallback once (§1.1), then fail the bridge with a clear message. |
| `403` | Local API not enabled, or this user lacks permission | Do not retry. Surface the activation instructions. |
| `404` | Unknown device/channel/datapoint | Do not retry. Trigger a debounced config reload — the topology probably changed under us. |
| `502` | **SysAP overloaded or an internal timeout** | Retry with exponential backoff **and reduce concurrency**. This is the primary overload signal. |
| `5xx` other | Transient | Exponential backoff, capped retries. |

Retry only `502` and connection-level errors. Retrying a `400` in a loop is how you end up with a
log full of noise and a masked profile bug.

---

## 3. The SysAP UUID wrapper

Every REST response body and every WebSocket frame is a map keyed by the SysAP's UUID:

```json
{ "00000000-0000-0000-0000-000000000000": { ... actual payload ... } }
```

In every deployment observed it is the all-zero UUID, but **do not hardcode it**. Resolve it once
at startup by taking the single key of the `GET /api/rest/configuration` response, cache it, and
use it for the rest of the process's life. If a frame arrives keyed by a different UUID, log a
warning once and ignore the frame.

---

## 4. Configuration schema

From the vendor's OpenAPI models (`SysAP`, `Device`, `Channel`, `InOutPut`):

```jsonc
{
  "<sysap-uuid>": {
    "sysapName": "My Home",
    "sysap":   { /* SysapSection */ },
    "users":   { /* Users */ },
    "floorplan": {
      "floors": {
        "01": {
          "name": "Ground Floor",
          "rooms": {
            "18": { "name": "Living Room" },
            "0C": { "name": "Kitchen" }
          }
        }
      }
    },
    "devices": {
      "ABB7F500E17A": {
        "displayName": "Kitchen Ceiling",
        "deviceId": "910C",              // hardware type, hex
        "articleNumber": "...",
        "interface": "TP",               // see §4.2
        "nativeId": "...",
        "floor": "01",
        "room": "0C",
        "unresponsive": false,
        "unresponsiveCounter": 0,
        "defect": false,
        "parameters": { "par0010": "3" },
        "channels": {
          "ch0003": {
            "displayName": "Ceiling Light",
            "functionID": "7",           // HEX STRING — see §4.3
            "floor": "01",
            "room": "0C",
            "type": "...",
            "inputs":  { "idp0000": { "pairingID": 1,   "value": "0" } },
            "outputs": { "odp0000": { "pairingID": 256, "value": "0" } },
            "parameters": { "par0011": "100" }
          }
        }
      }
    }
  }
}
```

### 4.1 Inputs vs outputs — get this right or nothing works

- **`idpXXXX` — inputs.** You **write** these to control the device. Reading them tells you the last
  commanded value, not reality.
- **`odpXXXX` — outputs.** The device's **feedback**. You **read** these for state; the WebSocket
  streams changes to these.

Writing to an `odp` fails silently or is ignored. Reading state from an `idp` gives you a value that
does not reflect physical state (e.g. a light switched at the wall). Both are common, hard-to-debug
mistakes.

A pairing ID typically appears on *both* sides with different numbers: `AL_SWITCH_ON_OFF = 1` on the
input, `AL_INFO_ON_OFF = 256` on the output. The profile format ([`docs/03`](03-model-and-profiles.md))
makes this split explicit and mandatory.

### 4.2 Interfaces

`Device.interface` values seen in the wild:

| Value | Meaning |
|---|---|
| `TP` | Wired twisted-pair bus |
| `RF` | Wireless free@home devices |
| `hue` | Philips Hue devices bridged by the SysAP |
| `sonos` | Sonos devices bridged by the SysAP |
| `smokealarm` | Smoke alarm subsystem |
| `VD` | Virtual device (see §4.5) |
| absent / null | Undefined; SysAP-internal pseudo-devices |

Filtering by interface is a supported configuration option. Default: include `TP`, `RF`,
`smokealarm` and undefined; exclude `hue`/`sonos` (users almost always already have a better,
native integration for those and double-bridging causes state loops); exclude `VD` unless
`include_virtual_devices` is set.

### 4.3 `functionID` is a hex string

`"functionID": "7"`, `"functionID": "1B"`, `"functionID": "20A"`. Parse with `int(value, 16)`.
Channels with a missing or unparseable `functionID` are skipped.

`parameters` keys are also hex-suffixed: `par0011` → parameter ID `0x11`.

### 4.4 Floor and room resolution

`floor`/`room` are hex-string IDs referencing `floorplan.floors`. Resolution rules, in order:

1. Use the **channel's** `floor`/`room` if present.
2. Otherwise fall back to the **device's** `floor`/`room`.
3. Otherwise the channel is an **orphan** — not placed on the floorplan.

`floorplan.floors[f].rooms` can be `null`, not just absent. Guard for it.

Orphan channels are typically unused physical inputs, internal channels, or leftovers from
reconfiguration. Default: **exclude** them (`include_orphan_channels: false`); make it configurable,
because some installations legitimately never assigned rooms.

Room names are the natural source for Home Assistant's `suggested_area`.

### 4.5 Virtual devices

Devices whose serial begins with `6000` are virtual, regardless of what `interface` says. They are
created via `PUT /api/rest/virtualdevice/{sysap}/{serial}` with:

```json
{ "type": "SwitchingActuator",
  "properties": { "ttl": "180", "displayname": "My Virtual Switch",
                  "flavor": "...", "capabilities": [ ... ] } }
```

`ttl` is `-1`/`0` (no expiry) or `180..86400` seconds. **A virtual device with a finite TTL that is
not refreshed will be marked stale and eventually removed by the SysAP.** If the bridge creates
virtual devices it owns the keepalive: re-`PUT` at `ttl / 2`, and stop cleanly on shutdown.

---

## 5. The WebSocket

```
ws://<host>/fhapi/v1/api/ws        (or wss:// for https)
Authorization: Basic <...>
```

Notes:

- Authentication is via the same Basic header on the upgrade request.
- **Commands cannot be sent over the WebSocket.** It is read-only. Use REST.
- **There is no application-level heartbeat.** Without protocol-level ping/pong a dead TCP
  connection looks exactly like an idle one, and the bridge silently stops receiving events with no
  error at all. This is the single nastiest failure mode in the system.
  → `aiohttp.ws_connect(..., heartbeat=30)`. Non-negotiable.

### 5.1 Frame schema

From the vendor model `WebsocketMessage`:

```ts
Record<sysapUuid, {
    datapoints:     Record<string, string>;   // "SERIAL/chXXXX/odpXXXX" -> "value"
    devices:        Devices;                  // full records for added/changed devices
    devicesAdded:   Array<string>;            // serials
    devicesRemoved: Array<string>;            // serials
    scenesTriggered: Record<sceneSerial, {
        channels: Record<channelId, {
            outputs: Record<odpId, { value: string; pairingID: number }>
        }>
    }>;
    parameters?:    Record<string, string>;
}>
```

A single frame may carry several of these at once. Handling:

| Key | Handling |
|---|---|
| `datapoints` | Hot path. `dict` lookup in the ingress table; unknown keys are ignored silently (they belong to filtered-out channels), but counted in a metric. |
| `devices` | Topology or metadata changed (including `unresponsive` transitions). Schedule a **debounced** config reload (ADR-007). Do not attempt to merge the partial record into the compiled tables — the diff surface is too large to get right. |
| `devicesAdded` | Same as above; additionally emit `bridge/event` `device_joined`. |
| `devicesRemoved` | Same; emit `device_leave`, and clean up the removed entities' retained topics and discovery. |
| `scenesTriggered` | Publish as bridge/entity **events**, non-retained. Also apply the contained output values to state — a scene trigger is often the only notification you get for the channels it drove. **⚠ verify empirically** whether the corresponding `datapoints` entries also arrive; if they do, state application is a harmless no-op thanks to change detection. |
| `parameters` | Channel/device parameters changed (e.g. a dimmer's min brightness). Debounced config reload. Rare. |

**Both reference implementations handle only `datapoints`.** Devices added, removed or renamed in
the free@home app therefore never appear until a restart. Handling the other keys is a deliberate
differentiator here.

### 5.2 Value encoding

Every value on the wire is a **string**, for every type:

| Logical type | On the wire | Notes |
|---|---|---|
| Boolean | `"0"` / `"1"` | |
| Percent | `"0"` … `"100"` | Brightness is `1..100`, not `0..255`, and not `0..100` — `0` is not a valid brightness, it is "off". |
| Integer | `"42"` | May arrive as `"42.0"`. Always `int(float(v))`, never `int(v)`. |
| Float | `"1669.12"` | |
| Enum | `"0"`, `"2"`, `"4"` … | Meaning is pairing-specific. |
| Unknown / uninitialised | `""` | **Must not be coerced.** Publish JSON `null`, or omit the attribute. Coercing `""` to `0`/`false` produces a phantom "closed window" or "0 °C" that looks entirely plausible and is completely wrong. |

The codec layer ([`docs/03 §5`](03-model-and-profiles.md#5-codecs)) exists to centralise these rules
so no profile author re-implements them.

### 5.3 Datapoint key form in frames

The `datapoints` keys are `SERIAL/chXXXX/odpXXXX`. Some tooling and test fixtures show a numeric
final segment (the pairing ID rather than the datapoint ID). Treat the ingress table as
authoritative for `odp`-form keys, and log-and-count anything that does not match, rather than
guessing. **⚠ verify empirically** with the capture tool against a real SysAP before finalising the
parser.

---

## 6. Semantics worth knowing before writing profiles

These are the ones that cause visible, user-reported bugs.

### 6.1 Covers are inverted

free@home: **`0` = fully open, `100` = fully closed.**
Home Assistant: `0` = closed, `100` = open.

The bridge's own MQTT payload should use a single documented convention —
**`position: 0 = closed, 100 = open`** (matching HA and ordinary intuition) — and the codec does the
inversion once, at the boundary. Never leak the raw free@home orientation into an MQTT payload.

Movement direction (`AL_INFO_MOVE_UP_DOWN`) and slat position (`AL_SET_ABSOLUTE_POSITION_SLATS_PERCENTAGE`)
have their own conventions; confirm each against a real device.

### 6.2 Colour temperature is a percentage

`AL_COLOR_TEMPERATURE` / `AL_INFO_COLOR_TEMPERATURE` are `0..100`, where **`0` is warmest and `100`
is coolest**. Home Assistant wants Kelvin (or mireds).

The physical bounds are per-channel parameters:

- `PID_TEMPERATURE_COLOR_PHYSICAL_COOLEST`
- `PID_TEMPERATURE_COLOR_PHYSICAL_WARMEST`

Read them at compile time, store per entity, and map linearly. Do not hardcode 2700–6500 K.

### 6.3 Brightness floor

`AL_ABSOLUTE_SET_VALUE_CONTROL` accepts `1..100`. Sending `0` is not "off" — clamp to `1` and use
the separate on/off datapoint for off. This is why the reference implementation does
`value = max(1, min(value, 100))`.

### 6.4 Forced position

Several actuator types expose `AL_FORCED` / `AL_INFO_FORCE` — a priority override. Note the
**asymmetric encoding**: the values you *write* differ from the values you *read back*.

| Meaning | Write (`AL_FORCED`) | Read (`AL_INFO_FORCE`) |
|---|---|---|
| Deactivated | `"0"` | `"0"` |
| Forced on | `"3"` | `"4"` |
| Forced off | `"2"` | `"5"` |

The codec must model input and output encodings separately. Assuming symmetry here is a guaranteed
bug.

### 6.5 Placeholder channel names

Channel `displayName` is sometimes the literal circled letters `"Ⓐ"` or `"ⓑ"` (or `null`) for a
multi-channel device's sub-channels. Fall back to the device's `displayName`, then to
`"Channel {channel_id}"`.

### 6.6 Sensors, triggers and switch sensors are events

A wall switch channel (`SwitchSensor`) does not have "state" in a useful sense — its output toggles
when someone presses it. Treating that as a retained state entity produces automations that fire on
restart when the retained value is replayed. Model these as `kind: event`
([ADR-005](00-overview-and-decisions.md#adr-005)) and publish them non-retained.

### 6.7 Multi-channel devices

One physical device commonly carries 2–8 independent channels (a 4-gang switch, a 6-fold actuator).
Each channel becomes its own entity; all of them share one Home Assistant *device* record via
`device.identifiers = [serial]`, so the HA UI groups them correctly. A device with no floor/room but
multiple placed channels is a normal "multi-device" and should not be treated as an orphan.

### 6.8 Not every command echoes

Actuators echo their new state on the WebSocket. Some channel types do not — pure sensors obviously,
but also some trigger/scene channels. Reconciliation ([ADR-012](00-overview-and-decisions.md#adr-012))
must therefore be opt-out per profile, not universal, or you will generate pointless reconciliation
reads for channels that were never going to echo.

---

## 7. Code tables to generate

`tools/gen_codes.py` (WP1) produces `src/freeathome2mqtt/sysap/codes/`:

| File | Source | Content |
|---|---|---|
| `pairings.py` | `node-free-at-home/src/pairingIds.ts`, cross-checked with `GET /api/rest/pairings` | `Pairing` IntEnum, ~340 members |
| `functions.py` | `node-free-at-home` function IDs, cross-checked with `local-abbfreeathome/bin/function.py` | `Function` IntEnum, ~650 members |
| `parameters.py` | `local-abbfreeathome/bin/parameter.py` | `Parameter` IntEnum |
| `interfaces.py` | §4.2 | `Interface` StrEnum |
| `NOTICE` | — | Upstream licences and attribution |

Generated files are committed (so the build is hermetic and the tool is not needed at install time)
and regenerated only deliberately. The generator must be **idempotent** — running it twice produces
a byte-identical file — so it can be enforced in CI.

Use `IntEnum`, not `Enum`: pairing IDs are compared against integers from JSON on the compile path,
and `IntEnum` avoids a `.value` lookup everywhere.

---

## 8. Rate and concurrency limits

Nothing is documented by the vendor. These are the working assumptions, and they should be
configurable because installations vary:

| Limit | Default | Rationale |
|---|---|---|
| Concurrent HTTP requests | **4** | Beyond ~5–10, `502`s begin. The reference client caps its connector at 10/5. |
| Sustained write rate | **≤ 10 /s** | Above this the free@home bus itself becomes the constraint, not the SysAP. |
| Config fetches | **≤ 1 per 30 s**, debounce 2 s | It is the most expensive call on the device by an order of magnitude. |
| Connection reuse | keep-alive, one `ClientSession` | A TLS handshake to the SysAP is expensive; never create a session per request. |

Adaptive behaviour: on a `502`, halve the effective concurrency limit (floor 1) and recover it
linearly on subsequent successes. This turns "SysAP is busy" from an outage into a slowdown.

### 8.1 HTTP versus HTTPS

Plain `http://` avoids TLS handshake and per-request encryption cost on a device that feels it. It
also sends the installer's Basic credentials in cleartext across the LAN.

**Recommendation: `https://` with `verify_ssl: true` and the SysAP's CA file.** The measured
difference is not worth the credential exposure, since connection reuse means the handshake happens
once. `verify_ssl: false` is supported (self-signed certs are the default state of these devices)
but logs a warning at startup, and `http://` logs a stronger one.
