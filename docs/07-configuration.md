# 07 — Configuration & Persistence

## 1. Principles

- **One file, fully documented, sane defaults.** A minimal working config is 6 lines. Everything
  else has a default that is right for a typical installation.
- **Validated at load, fatal on error.** A typo in `config.yaml` stops the process with a precise
  path (`mqtt.keepalive: input should be a valid integer`). Never start with a half-understood
  config.
- **Secrets are never in logs, never in `bridge/info`, never in MQTT.** Redaction happens centrally
  in `log.py`.
- **Configuration is not state.** `config.yaml` is user-owned and never written by the bridge.
  Runtime state (aliases, per-entity options, caches) lives in a separate data directory. This is a
  deliberate departure from zigbee2mqtt, whose `configuration.yaml` is rewritten by the application
  — which makes it hostile to version control, config-management tooling, and read-only mounts.

## 2. `config.yaml`

```yaml
# ---------------------------------------------------------------- required
sysap:
  host: https://192.168.1.50        # http:// or https://
  username: installer
  password: !env FAH_PASSWORD       # or !secret, or inline (discouraged)

mqtt:
  server: mqtt://192.168.1.10:1883

# ---------------------------------------------------------------- optional
sysap:
  verify_ssl: true                  # false for the stock self-signed cert
  ssl_ca_file: /data/sysap-ca.crt   # downloadable from the Local API settings screen
  max_inflight: 4                   # concurrent HTTP requests (docs/01 §8)
  request_timeout: 10               # seconds
  ws_heartbeat: 30                  # seconds; do not disable (docs/06 §2)
  ws_idle_timeout: 90               # watchdog; force reconnect after this silence
  config_refresh_interval: 300      # seconds; 0 disables
  reconnect:
    initial: 1
    max: 60
    factor: 2.0
    jitter: 0.25

mqtt:
  base_topic: freeathome2mqtt
  client_id: null                   # default: freeathome2mqtt_<sysap_serial>
  user: null
  password: !env MQTT_PASSWORD
  version: null                     # null = auto (try 5, fall back to 3.1.1)
  keepalive: 60
  ca: null
  cert: null
  key: null
  reject_unauthorized: true
  maximum_packet_size: 1048576
  force_disable_retain: false
  qos_state: 0                      # docs/04 §1.1 — raising this is rarely right
  qos_discovery: 1

homeassistant:
  enabled: true
  discovery_topic: homeassistant
  status_topic: homeassistant/status
  republish_delay: 5                # seconds after an HA birth message
  legacy_entity_attributes: false

entities:
  topic_style: friendly             # friendly | id
  include_orphan_channels: false    # channels with no floor/room (docs/01 §4.4)
  include_virtual_devices: false
  interfaces: [TP, RF, smokealarm, undefined]   # docs/01 §4.2
  publish_last_changed: true
  exclude: []                       # entity ids or glob patterns
  include: []                       # if non-empty, an allowlist applied after exclude

performance:
  coalesce_ms: 20                   # 0 = publish immediately (docs/05 §4.1)
  coalesce_adaptive: false
  coalesce_max_ms: 200
  coalesce_burst_threshold: 25
  command_debounce_ms: 50           # continuous commands only
  optimistic: true                  # ADR-012
  reconcile_after_ms: 3000
  get_rate_limit_s: 5               # per entity

availability:
  enabled: true
  grace_seconds: 10                 # before a SysAP drop flips bridge/state
  per_device: true                  # use unresponsive/defect (docs/06 §5.2)
  stale_after: null                 # informational counter only

advanced:
  data_dir: /data                   # persisted state, caches
  profiles_dir: /data/profiles      # user profiles, merged over built-ins
  log_level: info                   # error | warning | info | debug
  log_to_mqtt: false
  log_format: text                  # text | json
  raw_mode: false                   # false | unsupported_only | true
  cache_config: true
  metrics:
    enabled: false
    port: 9102
```

### 2.1 Secrets

Three mechanisms, in precedence order:

1. **Environment override** — any scalar is settable as
   `FAH2MQTT_<SECTION>_<KEY>` (uppercase, `__` for nesting):
   `FAH2MQTT_SYSAP__PASSWORD`, `FAH2MQTT_MQTT__SERVER`. This is what Docker and Kubernetes users
   want and it means the config file need not contain secrets at all.
2. **`!env VAR`** — a YAML tag resolving to an environment variable. Fails at load with a clear
   error if unset, rather than silently authenticating as an empty string.
3. **`!secret name`** — looked up in `<data_dir>/secrets.yaml`. Familiar to Home Assistant users.

`!file /run/secrets/x` is also supported for Docker/Podman secrets, reading and stripping trailing
whitespace (the trailing newline in a mounted secret file is a classic silent auth failure).

### 2.2 Validation beyond types

pydantic handles shapes. These semantic checks are additional and all fatal:

| Check | Why |
|---|---|
| `homeassistant.discovery_topic != mqtt.base_topic` | Would interleave two incompatible trees |
| `base_topic` contains no `+`, `#`, or leading/trailing `/` | Broker-level breakage |
| `sysap.host` parses as a URL with an `http`/`https` scheme | Bare IPs are a common mistake — accept and normalise with a warning |
| `verify_ssl: true` and no `ssl_ca_file` and scheme is `https` | Warn: the stock cert is self-signed and this will fail |
| `interfaces` members are known values | Typos silently exclude every device |
| `max_inflight` in `1..16` | Above this the SysAP will fail, not go faster |
| `coalesce_ms` in `0..1000` | |
| `exclude`/`include` patterns compile | |

### 2.3 Hot reload

`config.yaml` changes are picked up on `SIGHUP` and via `bridge/request/reload`. What can change at
runtime, and what cannot:

| Changeable live | Requires restart |
|---|---|
| `log_level`, `log_to_mqtt` | `mqtt.*` (server, credentials, TLS, client id) |
| `performance.*` | `sysap.host`, `sysap.username`, `sysap.password` |
| `availability.*` | `mqtt.base_topic` |
| `entities.*` filters (triggers a recompile + diff) | `advanced.data_dir` |
| `homeassistant.enabled` (publishes or retracts discovery) | |
| `profiles_dir` contents (recompile) | |

Anything requiring a restart is detected on reload and reported as an error naming the field,
rather than being silently ignored — silently ignoring a changed broker address is a support
nightmare.

## 3. CLI

```
freeathome2mqtt [--config PATH] [--data-dir PATH] [--log-level LEVEL]
                [--check-config] [--dry-run] [--discover] [--capture PATH]
```

| Flag | Purpose |
|---|---|
| `--check-config` | Validate and exit. For CI and pre-upgrade checks. |
| `--dry-run` | Full startup — connect, fetch, compile — then print the entity table and exit **without publishing anything.** The single most useful flag for a new user: it answers "what will this create?" before it creates it. |
| `--discover` | mDNS-scan for SysAPs (`_http._tcp.local.`, name `free@home*`) and print candidates |
| `--capture PATH` | Record the live configuration and a WebSocket session into a test fixture ([`docs/10 §3`](10-testing.md#3-fixtures-and-the-fake-sysap)), with serials and names pseudonymised |

`--capture` is what turns a user's bug report into a regression test, and pseudonymisation is what
makes them willing to send it.

## 4. Persisted state

Everything under `advanced.data_dir`. All writes are atomic (temp file in the same directory,
`fsync`, `os.replace`) and go through the executor so they never block the loop.

```
/data
├── entities.json          aliases + per-entity options  (bridge API writes this)
├── discovery.json         last published discovery payload hashes per topic
├── secrets.yaml           optional, user-managed
└── cache/
    └── config.<hash>.zst  last configuration snapshot (docs/05 §5)
```

### 4.1 `entities.json`

```json
{
  "version": 1,
  "entities": {
    "ABB7F500E17A_ch0003": {
      "alias": "kitchen_ceiling",
      "options": { "optimistic": false, "debounce_ms": 100 },
      "first_seen": "2026-01-14T09:12:00Z"
    }
  }
}
```

Written only by the bridge API ([`docs/02 §6`](02-architecture.md#6-control-plane)). Has a `version`
field from day one, with a migration function per bump — retrofitting versioning onto a state file
in the field is painful.

Unknown entity ids are retained, not pruned: a device temporarily off the bus must not lose its
alias. Prune only on an explicit `entity/remove`.

### 4.2 `discovery.json`

Maps discovery topic → payload hash. Lets a restart with an unchanged installation publish **zero**
discovery messages ([`docs/05 §5`](05-performance.md#5-startup-optimisation)) and lets the bridge
retract topics it published in a previous run but no longer would — the mechanism that stops
orphaned entities accumulating in Home Assistant across upgrades.

### 4.3 State values are not persisted

Deliberately. The configuration snapshot at startup contains every current value, so persisting
state buys nothing and risks publishing stale values before the first fetch completes. This is the
opposite of zigbee2mqtt's `state.json`, and the difference is entirely due to free@home having a
"read the whole world" endpoint that Zigbee does not.

## 5. Deployment

### 5.1 Container

```yaml
services:
  freeathome2mqtt:
    image: ghcr.io/steelnight/freeathome2mqtt:1
    restart: unless-stopped
    volumes:
      - ./data:/data
      - ./config.yaml:/config.yaml:ro          # read-only: the bridge never writes it
    environment:
      FAH2MQTT_SYSAP__PASSWORD: ${FAH_PASSWORD}
      FAH2MQTT_MQTT__PASSWORD: ${MQTT_PASSWORD}
    healthcheck:
      test: ["CMD", "freeathome2mqtt", "--health"]
      interval: 30s
      timeout: 5s
      retries: 3
```

Image requirements:

- Multi-arch `linux/amd64`, `linux/arm64`, `linux/arm/v7` — a Pi next to the SysAP is the primary
  target, and armv7 still matters for older Pis.
- Non-root by default; `/data` owned by the runtime user.
- `python:3.13-slim` base. Avoid Alpine: musl has no `uvloop` wheel and no `orjson` wheel for every
  arch, so you trade 30 MB of image for a source build and a slower runtime.
- `TINI` or `--init` so `SIGTERM` reaches the process and the graceful shutdown in
  [`docs/02 §8`](02-architecture.md#8-shutdown) actually runs.
- Health check exits 0 only when `bridge/state` would be `online`.

### 5.2 Native

`uv tool install freeathome2mqtt`, plus a systemd unit with `Restart=on-failure`,
`RestartSec=5`, and `Type=notify` if `sd_notify` support is added (WP12, optional).

### 5.3 Home Assistant add-on

A thin wrapper: add-on options map onto `config.yaml`, the MQTT service discovery API supplies
broker credentials automatically, and `/data` is the add-on's persistent volume. Worth doing (WP12)
because it removes every configuration step for the largest user group.
