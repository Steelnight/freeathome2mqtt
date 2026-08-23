# 08 — Workflows

Sequence diagrams for every flow with non-obvious ordering. Where a step is load-bearing it is
called out, because these orderings are exactly where the reference implementations have bugs.

## 1. Cold start

```mermaid
sequenceDiagram
    autonumber
    participant CLI
    participant SUP as Supervisor
    participant PR as settings_probe
    participant M as MqttClient
    participant W as WsReader
    participant R as RestClient
    participant C as Compiler
    participant HA as Discovery
    participant P as Publisher

    CLI->>SUP: load config, install uvloop, set up logging
    SUP->>PR: GET /settings.json  (no auth)
    PR-->>SUP: version, serial, name
    alt version below 2.6.0
        SUP-->>CLI: exit 3, upgrade required
    end

    SUP->>M: connect (LWT armed: bridge/state offline, retained)
    M-->>SUP: connected
    Note over SUP: LWT is armed BEFORE anything can fail,<br/>so a crash from here on is visible to consumers

    SUP->>W: connect websocket (heartbeat=30)
    W-->>SUP: connected
    Note over W: ⚠ BUFFERING STARTS HERE — before the config fetch.<br/>Skipping this loses every change made during the fetch.

    SUP->>R: GET /api/rest/configuration
    R-->>SUP: snapshot (all current values)
    SUP->>C: compile(snapshot, profiles, options)
    C-->>SUP: Model(entities, ingress, egress, discovery)

    SUP->>W: drain buffer over the compiled state
    Note over SUP: idempotent — a buffered frame either matches<br/>the snapshot (no-op) or is newer (applied)
    W-->>SUP: live dispatch enabled

    SUP->>M: subscribe +/set, +/set/+, +/get, bridge/request/#, homeassistant/status
    SUP->>HA: publish discovery (retained, QoS1, changed only)
    SUP->>P: publish all entity state (retained, QoS0, sequential)
    SUP->>M: bridge/devices, bridge/info
    SUP->>M: bridge/state = online
    SUP->>SUP: start publisher, dispatcher, reconciler, refresher, availability
```

**Steps 8–12 are the important ones.** Fetching the configuration before opening the WebSocket —
what both reference implementations do — creates a window of several hundred milliseconds during
which a change is lost permanently: the snapshot predates it and the WebSocket did not exist yet.
Nothing re-reads that datapoint until something else touches it, so a light switched at the wall
during startup stays wrong indefinitely.

**Steps 17–19 order matters too.** Discovery before state means Home Assistant has an entity to put
the state into. `bridge/state: online` last means consumers never act on a half-populated tree.

## 2. Datapoint event (the hot path)

```mermaid
sequenceDiagram
    autonumber
    participant S as SysAP
    participant W as WsReader
    participant I as Ingress
    participant ST as StateStore
    participant P as Publisher
    participant B as Broker

    S->>W: {sysap-uuid: {datapoints: {"ABB..../ch0003/odp0001": "60"}}}
    W->>W: orjson.loads (once per frame)
    W->>I: dispatch datapoints dict
    I->>I: ingress["ABB..../ch0003/odp0001"] → Binding(17, 1, decode_percent, STATE)
    I->>I: decode "60" → 60
    I->>ST: values[17][1] == 43 ? no → store
    ST->>ST: clear unconfirmed bit, dirty.add(17)
    ST->>P: wake.set()
    Note over P: sleep(coalesce_ms) — gather the rest of the burst
    P->>P: batch = dirty; dirty = set()
    P->>B: freeathome2mqtt/kueche_deckenlicht<br/>{"id":"ABB..._ch0003","state":true,"brightness":60}<br/>retained, QoS0
```

If `values[17][1] == 60` already, the flow stops at step 5: no dirty mark, no publish, no broker
traffic. That early return is the highest-leverage line in the codebase
([`docs/05 §1`](05-performance.md#1-budgets), budget P12).

## 3. Command

```mermaid
sequenceDiagram
    autonumber
    participant B as Broker
    participant M as mqtt_reader
    participant CQ as CommandCoalescer
    participant D as Dispatcher
    participant R as RestClient
    participant S as SysAP
    participant ST as StateStore
    participant RC as Reconciler

    B->>M: kueche_deckenlicht/set  {"brightness": 55}
    M->>M: by_topic["kueche_deckenlicht"] → 17
    M->>CQ: egress[(17,"brightness")], validate + clamp
    alt invalid
        M->>B: bridge/response/set {"status":"error", ...}
    end
    CQ->>ST: optimistic values[17][1] = 55, set unconfirmed bit, dirty.add(17)
    ST->>B: retained state publish (fast path for the UI)
    CQ->>RC: arm reconcile timer (3 s)

    Note over CQ: continuous:true → leading edge sends now,<br/>further sets within 50 ms only update `pending`

    CQ->>D: flush
    D->>D: await semaphore (max_inflight)
    D->>R: PUT /api/rest/datapoint/{uuid}/ABB....ch0003.idp0002  body "55"
    R->>S: HTTP
    S-->>R: 200 {"result":"OK"}

    S-->>M: WS echo odp0001 = "55"
    M->>ST: value already 55 → clear unconfirmed, no publish
    ST->>RC: cancel reconcile timer
```

Failure branches:

- `result != "OK"` or HTTP `4xx` → error to `bridge/response/set`, reconcile **immediately** rather
  than after the timer, so the optimistic lie is corrected within one round trip.
- No echo before the timer fires → one targeted `GET /api/rest/datapoint/...`, publish the truth
  (possibly rolling back the optimistic value).
- `404` → the topology changed; additionally schedule a debounced config reload.

## 4. Reconnect and resync

```mermaid
sequenceDiagram
    autonumber
    participant W as WsReader
    participant SUP as Supervisor
    participant A as Availability
    participant B as Broker
    participant R as RestClient
    participant C as Compiler
    participant ST as StateStore
    participant P as Publisher

    Note over W: heartbeat timeout OR close frame OR idle watchdog
    W->>SUP: sysap_disconnected
    SUP->>A: start grace timer (10 s)
    Note over A: a fast reconnect never reaches the broker —<br/>no HA availability flap
    A->>B: bridge/state = offline   (only if grace expires)

    loop backoff 1s → 60s, full jitter
        W->>W: ws_connect
    end
    W-->>SUP: connected
    Note over W: ⚠ buffer frames from here, same as cold start

    SUP->>R: GET /api/rest/configuration      (exactly ONE request)
    R-->>SUP: snapshot
    SUP->>SUP: hash; unchanged? skip discovery entirely
    SUP->>C: compile
    C-->>SUP: new Model
    SUP->>ST: diff old vs new; mark only changed entities dirty
    SUP->>W: drain buffer
    SUP->>P: publish deltas only
    SUP->>A: bridge/state = online
```

If nothing changed while disconnected, this publishes **zero** entity messages. Republishing 1 000
retained messages on every WebSocket blip is a real and common bridge failure that floods brokers
and, with Home Assistant's recorder, writes thousands of pointless database rows.

## 5. Device added in the free@home app

```mermaid
sequenceDiagram
    autonumber
    participant S as SysAP
    participant W as WsReader
    participant SUP as Supervisor
    participant C as Compiler
    participant HA as Discovery
    participant B as Broker

    S->>W: {"devicesAdded": ["ABB7F5009999"], "devices": {...}}
    W->>SUP: topology event
    SUP->>B: bridge/event {"type":"device_joined", ...}
    Note over SUP: debounce 2 s — pairing emits a burst of frames
    SUP->>SUP: (further frames coalesce into the same reload)
    SUP->>C: full recompile from a fresh snapshot
    C-->>SUP: new Model
    SUP->>HA: publish discovery for new entities only
    SUP->>B: publish state for new entities
    SUP->>B: bridge/devices, bridge/info (updated counts)
    SUP->>B: bridge/event {"type":"config_reloaded","added":3,"removed":0}
```

The device appears in Home Assistant **without a restart**. Neither reference implementation does
this — both only handle the `datapoints` key — and it is one of the most visible differentiators.

## 6. Device removed

```mermaid
sequenceDiagram
    autonumber
    participant S as SysAP
    participant SUP as Supervisor
    participant B as Broker

    S->>SUP: {"devicesRemoved": ["ABB7F5001234"]}
    SUP->>SUP: debounced recompile; diff finds 4 entities gone
    loop for each removed entity
        SUP->>B: base/entity              "" retained   (clear state)
        SUP->>B: base/entity/availability "" retained
        SUP->>B: homeassistant/.../config    "" retained   (retract discovery)
    end
    SUP->>B: bridge/event {"type":"device_leave","serial":"ABB7F5001234"}
    SUP->>B: bridge/devices (updated)
```

Retraction is what stops entities accumulating in Home Assistant forever. Without the empty retained
payload, HA re-creates the entity from the broker on every restart, and the user has no way to get
rid of it except by manually clearing retained topics.

## 7. Rename

```mermaid
sequenceDiagram
    autonumber
    participant U as User
    participant B as Broker
    participant API as BridgeApi
    participant PS as Persistence
    participant HA as Discovery
    participant P as Publisher

    U->>B: bridge/request/entity/rename<br/>{"id":"ABB..._ch0003","name":"Kitchen Ceiling"}
    B->>API: handle
    API->>API: slug → "kitchen_ceiling"; check collisions
    API->>B: old state topic       "" retained
    API->>B: old availability      "" retained
    API->>B: old discovery topics  "" retained
    API->>PS: entities.json: alias = "kitchen_ceiling"
    API->>API: rebuild the entity's topics; by_topic remap
    API->>HA: republish discovery (unique_id UNCHANGED)
    API->>P: republish state on the new topic
    API->>B: bridge/event {"type":"entity_renamed","from":...,"to":...,"id":...}
    API->>B: bridge/response/entity/rename {"status":"ok"}
```

Because `unique_id` is the immutable entity id and not the name
([ADR-010](00-overview-and-decisions.md#adr-010)), Home Assistant **updates** the existing entity
rather than creating a duplicate. History, automations and the entity registry entry all survive the
rename. This is the concrete payoff for separating identity from naming.

## 8. Home Assistant restart

```mermaid
sequenceDiagram
    autonumber
    participant HA as Home Assistant
    participant B as Broker
    participant BR as Bridge

    HA->>B: homeassistant/status = "online"
    B->>BR: (subscribed)
    Note over BR: wait republish_delay (5 s)<br/>so HA's MQTT integration is fully up
    BR->>B: republish ALL discovery (retained, QoS1)
    BR->>B: republish ALL entity state (retained, QoS0)
```

The delay is not cosmetic: publishing discovery into an HA instance whose MQTT integration is still
loading gets it silently dropped, and the user sees a permanently empty integration.

Note this is a **full** republish, ignoring the changed-only optimisation, because HA may have
purged its own discovery cache and the broker's retained state may not be trusted.

## 9. Broker outage

```mermaid
sequenceDiagram
    autonumber
    participant S as SysAP
    participant W as WsReader
    participant ST as StateStore
    participant M as MqttClient
    participant B as Broker

    Note over M: broker connection lost
    M->>M: reconnect backoff 1s → 60s, jitter
    loop while disconnected
        S->>W: datapoint frames
        W->>ST: apply + mark dirty
        Note over ST: ingestion CONTINUES.<br/>The dirty set accumulates the union of changes —<br/>bounded by the entity count, not by time.
    end
    M-->>B: reconnected
    M->>B: bridge/state = online (retained, QoS1)
    M->>M: re-subscribe (never assume the session survived)
    M->>B: publish the accumulated dirty batch
    Note over M: 2 s later, republish the retained set once,<br/>in case the broker did not persist retained messages
```

Two design consequences worth stating:

- **Ingestion never pauses.** The dirty set's size is bounded by the number of entities, so a
  ten-hour broker outage costs the same memory as a ten-second one, and the bridge is instantly
  correct on reconnect.
- **Always re-subscribe on connect.** Whether the broker preserved the session depends on
  `clean_session`, broker configuration and MQTT version. Re-subscribing is idempotent and cheap;
  assuming persistence and being wrong means the bridge silently stops accepting commands.

## 10. Graceful shutdown

```mermaid
sequenceDiagram
    autonumber
    participant OS
    participant SUP as Supervisor
    participant CQ as CommandCoalescer
    participant P as Publisher
    participant B as Broker
    participant PS as Persistence
    participant S as SysAP

    OS->>SUP: SIGTERM
    SUP->>SUP: cancel mqtt_reader (stop accepting commands)
    SUP->>CQ: flush pending commands (deadline 2 s)
    CQ->>S: final PUTs
    SUP->>P: flush the dirty set
    P->>B: final state publishes
    SUP->>B: bridge/state = offline (retained, QoS1)
    Note over SUP: explicit, not left to the LWT —<br/>the LWT only fires after the broker's keepalive timeout
    SUP->>S: stop virtual-device TTL keepalives
    SUP->>PS: atomic snapshot write
    SUP->>S: close websocket, close HTTP session
    SUP->>B: disconnect
    SUP->>OS: exit 0
```

Total budget 5 s, then hard exit. Flushing pending commands first matters: a user who moved a slider
and immediately restarted the container should not lose that command, and the debouncer is exactly
where it would be sitting.
