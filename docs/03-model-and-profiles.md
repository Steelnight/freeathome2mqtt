# 03 — Domain Model & Channel Profiles

## 1. The model

```
Installation (SysAP)
└── Device            ← physical hardware, identified by serial (e.g. ABB7F500E17A)
    └── Channel       ← one function of that hardware (e.g. ch0003), has a functionID
        ├── inputs    idpXXXX  → write here to command
        ├── outputs   odpXXXX  → read here for state
        └── parameters parXXXX → static configuration
```

maps to

```
Bridge
└── Entity            ← exactly one per supported Channel
    ├── attributes    ← readable values, each bound to one output datapoint
    └── commands      ← writable actions, each bound to one input datapoint
```

**One channel = one entity.** Not one device = one entity: a 4-gang switch is four entities that
share a Home Assistant device record. This matches how users think about free@home (they name and
place *channels* in the app, not devices) and it keeps the mapping bijective, which makes
diffing after a config reload trivial.

### 1.1 Entity identity and naming

| Field | Value | Stability |
|---|---|---|
| `id` | `{device_serial}_{channel_id}` — e.g. `ABB7F500E17A_ch0003` | **Immutable.** Present in every payload. |
| `name` | The channel's `displayName`, with the fallbacks in [`docs/01 §6.5`](01-freeathome-api.md#65-placeholder-channel-names) | Changes when the user renames in the free@home app |
| `alias` | User override set via `bridge/request/entity/rename` | Changes on request |
| `topic segment` | `slug(alias or name)`, or `id` when `topic_style: id` | Changes on rename → managed migration ([ADR-010](00-overview-and-decisions.md#adr-010)) |
| `area` | Resolved room name; falls back to the device's room | Follows the floorplan |

**Slugification** (`model/naming.py`): NFKD-normalise, transliterate German umlauts explicitly
(`ä→ae`, `ö→oe`, `ü→ue`, `ß→ss` — *not* the lossy `ä→a` that NFKD alone gives, because
"Küche"→"kche" is unusable), lowercase, replace any run of non-`[a-z0-9]` with `_`, strip leading and
trailing `_`, collapse repeats, truncate to 64 chars.

**Collisions.** Two channels named "Deckenlicht" in different rooms both slug to `deckenlicht`.
Resolution order, applied deterministically so restarts are stable:

1. Prefix with the area: `wohnzimmer_deckenlicht`.
2. Still colliding → suffix with the channel id: `wohnzimmer_deckenlicht_ch0003`.
3. Still colliding (impossible, but assert it) → fall back to `id`.

Collisions are logged at WARNING with both entity IDs so the user can rename in the app.

## 2. Runtime representation

Optimised for the hot path ([`docs/02 §4`](02-architecture.md#4-the-hot-path-step-by-step)).
Attribute access is by **integer index into a list**, never by dict key or attribute name.

```python
@dataclass(slots=True)
class Entity:
    idx: int                        # position in the entities list; used everywhere hot
    id: str                         # "ABB7F500E17A_ch0003"
    profile: str                    # "switch_actuator"
    name: str
    area: str | None
    device_serial: str
    channel_id: str
    attr_names: tuple[str, ...]     # positional, parallel to values
    attr_kinds: tuple[int, ...]     # STATE | EVENT
    state_topic: str                # pre-built
    set_topic: str                  # pre-built
    get_topic: str                  # pre-built
    availability_topic: str | None
    optimistic: bool
    discovery: tuple[tuple[str, bytes], ...]   # (topic, pre-serialised payload)


@dataclass(slots=True, frozen=True)
class Binding:                      # ingress: what a WS datapoint key means
    entity_idx: int
    attr_idx: int
    decode: Callable[[str], Any]    # bound codec, no dispatch at runtime
    kind: int                       # STATE | EVENT
    attr_bit: int                   # bitmask slot for the unconfirmed tracker


@dataclass(slots=True, frozen=True)
class EgressBinding:                # egress: how to execute a command
    entity_idx: int
    rest_path: str                  # "ABB7F500E17A.ch0003.idp0000" — pre-formatted
    encode: Callable[[Any], str]
    continuous: bool                # debounce this one
    optimistic_attr: int | None     # which attribute to update optimistically
    validate: Callable[[Any], Any]  # range/enum check; raises CommandError
```

The tables:

```python
ingress: dict[str, Binding]                 # "SERIAL/chXXXX/odpXXXX" -> Binding
egress:  dict[tuple[int, str], EgressBinding]   # (entity_idx, command_name) -> binding
entities: list[Entity]                      # index == Entity.idx
by_id:   dict[str, int]                     # entity id -> idx
by_topic: dict[str, int]                    # topic segment -> idx  (for /set routing)
```

`StateStore` keeps values in a parallel structure so nothing is stored on `Entity` itself:

```python
values:      list[list[Any]]    # values[entity_idx][attr_idx]
unconfirmed: list[int]          # bitmask per entity, for ADR-012
dirty:       set[int]
```

Using `list[list]` rather than dicts-of-dicts halves the memory for 1 000 entities and removes a
hash per access. At 6 000 datapoints this is on the order of a few hundred kilobytes.

## 3. Profile format

A profile describes **one channel type**. Profiles live in `src/freeathome2mqtt/profiles/*.yaml`
and are validated against `profiles/_schema.json` at load. User profiles from `profiles_dir` are
merged **over** the built-ins, keyed by `id`, so a user can override a shipped profile without
forking.

```yaml
# profiles/lighting.yaml
- id: dimming_actuator
  description: Dimmable light actuator
  functions:
    - FID_DIMMING_ACTUATOR
    - FID_DIMMING_ACTUATOR_TYPE1
    - FID_DIMMING_ACTUATOR_TYPE2
  homeassistant:
    component: light
    device_class: null

  attributes:
    state:
      pairing: AL_INFO_ON_OFF          # OUTPUT pairing
      codec: bool01
      kind: state
    brightness:
      pairing: AL_INFO_ACTUAL_DIMMING_VALUE
      codec: percent_int
      kind: state
      unit: "%"
    forced_position:
      pairing: AL_INFO_FORCE
      codec: enum
      kind: state
      values: { "0": deactivated, "4": forced_on, "5": forced_off }
      default: unknown
      entity_category: diagnostic

  commands:
    state:
      pairing: AL_SWITCH_ON_OFF        # INPUT pairing
      codec: bool01
      continuous: false
      optimistic: state                # which attribute to update optimistically
    brightness:
      pairing: AL_ABSOLUTE_SET_VALUE_CONTROL
      codec: percent_int
      range: [1, 100]                  # clamped, not rejected — see §6
      continuous: true
      optimistic: brightness
    forced_position:
      pairing: AL_FORCED
      codec: enum
      values: { deactivated: "0", forced_on: "3", forced_off: "2" }  # ASYMMETRIC, see docs/01 §6.4
      continuous: false
      optimistic: forced_position

  # Optional. Channel parameters read once at compile time and attached to the entity.
  parameters:
    min_brightness: PID_DIMMER_MIN_BRIGHTNESS

  requires:
    # A channel only matches this profile if it actually has these datapoints.
    outputs: [AL_INFO_ON_OFF, AL_INFO_ACTUAL_DIMMING_VALUE]
```

### 3.1 Schema (normative)

| Key | Type | Required | Meaning |
|---|---|---|---|
| `id` | slug | yes | Profile identifier; also the merge key and the value published as `profile` |
| `description` | string | no | Human-readable; surfaced in `bridge/devices` |
| `functions` | list of `Function` names | yes | Which `functionID`s this profile claims |
| `requires.outputs` / `requires.inputs` | list of `Pairing` names | no | Discriminator when several profiles claim a function |
| `attributes.<name>` | object | yes (≥1) | See §3.2 |
| `commands.<name>` | object | no | See §3.3 |
| `parameters.<name>` | `Parameter` name | no | Static values copied onto the entity |
| `homeassistant` | object | no | Component mapping; see [`docs/04 §6`](04-mqtt-interface.md#6-home-assistant-discovery) |
| `transform` | string | no | Named escape hatch; see §7 |
| `availability` | `device` \| `none` | no | Default `device` — use the device's `unresponsive` flag |

### 3.2 Attribute object

| Key | Required | Meaning |
|---|---|---|
| `pairing` | yes | Output pairing to bind to |
| `codec` | yes | See §5 |
| `kind` | no, default `state` | `state` (retained, coalesced, deduped) or `event` (immediate, not retained, not deduped) |
| `unit` | no | Published in `bridge/devices`; becomes HA `unit_of_measurement` |
| `values` | when `codec: enum` | Raw string → symbolic name |
| `default` | no | Value when the datapoint is `""` or unmapped. Prefer `null`. |
| `entity_category` | no | `diagnostic` / `config`; passed to HA |
| `precision` | no | Decimal places for float codecs; used for change detection too |
| *(any other key)* | no | Passed to the codec factory verbatim as a keyword argument — e.g. `scaled`'s `factor`/`offset` (§5). Not enumerated here because the set is open-ended over the codec registry; `model/entity.py`'s `AttributeSpec.codec_params` holds these unmodified. |

### 3.3 Command object

| Key | Required | Meaning |
|---|---|---|
| `pairing` | yes | **Input** pairing to write |
| `codec` | yes | |
| `values` | when `codec: enum` | Symbolic name → raw string (note: the *inverse* direction of an attribute's map) |
| `range` | no | `[min, max]`, clamped |
| `continuous` | no, default `false` | Debounce this command |
| `optimistic` | no | Attribute name to update optimistically; omit to disable optimism for this command |
| `confirm` | no, default `true` | Whether to expect a WS echo and reconcile if absent |
| *(any other key)* | no | Passed to the codec factory verbatim, as for the attribute object above — `CommandSpec.codec_params` in `model/entity.py`. |

### 3.4 Profile matching

For each channel, at compile time:

1. Parse `functionID` as hex → `Function`. Unknown → skip the channel, count it, and log once per
   distinct function ID at INFO with the device model, so unsupported hardware is *discoverable*
   from the logs rather than silently invisible.
2. Collect all profiles claiming that `Function`.
3. Filter by `requires` — a profile only matches if the channel really has those pairings.
4. **Exactly one match** → use it. **Several** → pick the one with the most `requires` entries
   (most specific wins); tie → deterministic order by `id`, and log a WARNING, because that is a
   profile-authoring bug. **None** → skip the channel.

Rule 4's tie-break must be deterministic, or entity sets differ between restarts and Home Assistant
grows duplicates.

### 3.5 Unsupported channels are still visible

Channels with no matching profile are published in `bridge/devices` with
`"supported": false` and their raw function ID, so a user can open a well-formed issue. With
`raw_mode: true` they additionally get a raw entity ([`docs/04 §7`](04-mqtt-interface.md#7-raw-mode)).

## 4. Compilation

`model/compiler.py`, pure function:

```python
def compile(config: Configuration, profiles: ProfileRegistry, options: CompileOptions) -> Model
```

`config` is already unwrapped from its SysAP-UUID key (`sysap.schema.Configuration`, docs/01 §4)
— that unwrap happens once, in `sysap.rest.RestClient` (docs/01 §3), not here. Repeating it on
every recompile would defeat the "resolve once, cache for the process's life" rule that section
states.

Steps:

1. Resolve the floorplan into `{floor_id: {room_id: name}}`.
2. For each device: apply the interface filter; record `unresponsive`/`defect` for availability.
3. For each channel: resolve floor/room (channel first, then device); apply the orphan filter;
   match a profile (§3.4).
4. Resolve names, slugs and collisions (§1.1), consulting the persisted alias map.
5. For each attribute: find the output datapoint whose `pairingID` matches; if absent, drop the
   attribute (a channel legitimately may not expose all of a profile's attributes) — unless it is
   listed in `requires`, in which case the profile should not have matched.
6. For each command: find the **input** datapoint by pairing; pre-format the REST path.
7. Seed `values` from the datapoints' current `value` fields — the snapshot already has them, so no
   extra request is needed.
8. Pre-render Home Assistant discovery payloads and `orjson.dumps` them once.
9. Emit `Model(entities, ingress, egress, by_id, by_topic, discovery, initial_values, stats)` —
   `initial_values` is step 7's output: a tuple parallel to `entities`, each entry itself a tuple
   parallel to that entity's `attr_names`, so seeding a `StateStore` is a direct walk with no
   second pass over the config.

**Determinism is a hard requirement.** Same input → byte-identical output, including ordering. Test
it directly (`test_compiler_is_deterministic`), because non-determinism here manifests as entities
mysteriously changing topics between restarts.

### 4.1 Recompilation and diffing

A config reload recompiles from scratch into a *new* `Model` and diffs against the current one:

| Diff | Action |
|---|---|
| Entity added | Publish discovery + state; `bridge/event: entity_added` |
| Entity removed | Publish empty retained to state, availability, and each discovery topic; `bridge/event: entity_removed` |
| Discovery payload changed | Republish that topic only (payloads are compared as bytes) |
| Attribute value changed | Mark dirty; publish normally |
| Nothing changed | Publish nothing. This is the common case and must be cheap. |

Recompiling wholesale rather than patching in place is deliberate: patching a live model from a
partial `devices` frame has a large, badly-specified diff surface, and the full recompile of 1 000
channels costs ~50 ms of CPU. Correctness is worth 50 ms.

## 5. Codecs

`model/codecs.py`. Each codec is a `(decode, encode)` pair of small pure functions. The registry
maps a name to a factory so parameterised codecs (`enum`, `scaled`) can be built at compile time and
**bound** into the `Binding`, so runtime has no dispatch.

| Codec | Decode (`str` → JSON) | Encode (JSON → `str`) | Notes |
|---|---|---|---|
| `bool01` | `"1"`→`true`, `"0"`→`false`, `""`→`null` | `true`→`"1"` | The workhorse |
| `int` | `int(float(v))` | `str(int(v))` | `float()` first — values arrive as `"42.0"` |
| `float` | `round(float(v), precision)` | `str(v)` | `precision` also governs change detection |
| `percent_int` | `int(float(v))` clamped `0..100` | clamped to `range` | |
| `percent_inverted` | `100 - int(float(v))` | `100 - v` | **Covers.** See [`docs/01 §6.1`](01-freeathome-api.md#61-covers-are-inverted) |
| `enum` | map lookup, unmapped → `default` | inverse map; unknown → `CommandError` | Input and output maps are independent |
| `scaled` | `float(v) * factor + offset` | inverse | Sensor unit conversion |
| `color_temp_pct` | `pct` → Kelvin using the channel's `PHYSICAL_WARMEST`/`COOLEST` parameters | Kelvin → pct | Bound per entity at compile time |
| `string` | passthrough, `""` → `null` | `str(v)` | Escape hatch |
| `trigger` | any value → `true` | — | Event-only; the value is meaningless, the edge is the signal |

**Every decoder must map `""` to `null`.** This is centralised here specifically so no profile author
can forget it ([`docs/01 §5.2`](01-freeathome-api.md#52-value-encoding)).

Decoders must not raise on malformed input: they return `null` and increment
`codec_errors{codec, entity}`. A single misbehaving sensor must not kill the ingress task.

## 6. Validation and clamping policy

For commands:

- **Clamp** values that are merely out of range (`brightness: 150` → `100`). Users script things;
  rejecting is unhelpful and produces no visible effect.
- **Reject** structurally wrong values (`brightness: "bright"`, unknown enum member, unknown
  attribute) with a `bridge/response` error and a WARNING log. These are bugs, and silently
  swallowing them wastes hours of debugging.

The asymmetry is deliberate: clamping preserves intent, rejecting surfaces mistakes.

## 7. Complex profiles and the `transform` escape hatch

A handful of channels need logic that spans several datapoints. For these a profile names a
function in `model/transforms.py`:

```yaml
- id: room_temperature_controller
  functions: [FID_ROOM_TEMPERATURE_CONTROLLER_MASTER_WITH_FAN, ...]
  transform: room_temperature_controller
```

```python
@transform("room_temperature_controller")
class RoomTemperatureControllerTransform(Transform):
    def derive(self, values: list[Any]) -> dict[str, Any]:
        """Derive composite attributes from raw ones after any change."""
    def command(self, name: str, value: Any, values: list[Any]) -> list[tuple[str, Any]]:
        """Return (sub_command_name, symbolic_value) writes for one logical command."""
```

`command()` returns the *profile's own command names* and each command's own *symbolic*
(pre-encode) value, not REST paths or raw wire strings: a transform is a pure function with no
compile-time knowledge of a specific entity's compiled paths or codec encodings, so it cannot
fabricate either. The caller (`bus/commands.py`, WP7) resolves each `sub_command_name` to that
entity's compiled `EgressBinding` and calls its `encode()`, exactly as it would for a
non-transformed command.

The full expected list — do not grow it without justification:

| Transform | Why it needs one |
|---|---|
| `room_temperature_controller` | HVAC mode is derived from `AL_CONTROLLER_ON_OFF` + `AL_ECO_ON_OFF` + `AL_INFO_HEATING_COOLING_MODE`; setting a mode writes 2–3 datapoints |
| `cover_with_slats` | Position and slat angle interact; `stop` means different things while moving vs stopped |
| `color_light` | HSV/RGB packed into one datapoint; brightness interacts with on/off |
| `energy_meter` | Multi-phase totals derived from per-phase datapoints |
| `des_door_station` | Ring events, unlock, and mute are separate channels that users expect as one entity |

Transforms run **after** change detection and only for entities that actually changed, so they are
off the common hot path. They must be pure functions of `values` — no I/O, no clock, no globals —
so they stay testable and deterministic.

## 8. Worked example — end to end

Channel from the configuration snapshot:

```json
"ch0003": {
  "displayName": "Deckenlicht",
  "functionID": "12",
  "floor": "01", "room": "0C",
  "inputs":  { "idp0000": {"pairingID": 1,   "value": "0"},
               "idp0002": {"pairingID": 17,  "value": "0"} },
  "outputs": { "odp0000": {"pairingID": 256, "value": "1"},
               "odp0001": {"pairingID": 272, "value": "43"} }
}
```

Compiles (device `ABB7F500E17A`, room "Küche") to:

```python
Entity(idx=17, id="ABB7F500E17A_ch0003", profile="dimming_actuator",
       name="Deckenlicht", area="Küche",
       attr_names=("state", "brightness"),
       attr_kinds=(STATE, STATE),
       state_topic="freeathome2mqtt/kueche_deckenlicht",
       set_topic="freeathome2mqtt/kueche_deckenlicht/set", ...)

ingress["ABB7F500E17A/ch0003/odp0000"] = Binding(17, 0, decode_bool01,   STATE, 0b01)
ingress["ABB7F500E17A/ch0003/odp0001"] = Binding(17, 1, decode_percent,  STATE, 0b10)

egress[(17, "state")]      = EgressBinding(17, "ABB7F500E17A.ch0003.idp0000",
                                           encode_bool01, continuous=False, optimistic_attr=0)
egress[(17, "brightness")] = EgressBinding(17, "ABB7F500E17A.ch0003.idp0002",
                                           encode_percent, continuous=True, optimistic_attr=1)

values[17] = [True, 43]
```

Published state:

```json
{ "id": "ABB7F500E17A_ch0003", "state": true, "brightness": 43 }
```

A WS frame `{"ABB7F500E17A/ch0003/odp0001": "60"}` becomes: one dict hit, `int(float("60"))`,
`60 != 43`, store, `dirty.add(17)` — then one retained publish of the whole entity payload.

## 9. Profile coverage targets

Derived from the reference implementation's supported set. WP4 delivers tier 1; WP8 tiers 2–3.

**Tier 1 — must have**
switch actuator · dimming actuator · colour-temperature actuator · shutter/blind/awning/attic-window
actuator · window-door sensor · movement detector (+ blockable) · brightness sensor · temperature
sensor · switch sensor (event) · trigger (event) · smoke detector · room temperature controller ·
heating/cooling/valve actuator

**Tier 2 — should have**
air-quality sensor (CO₂/VOC/humidity) · carbon-monoxide sensor · rain sensor · wind sensor · blind
sensor · dimming sensor · staircase-light sensor · force on/off sensor · DES door opener · DES door
ringing sensor · Welcome IP mute · M-Wire switch actuator

**Tier 3 — virtual devices**
virtual switch · virtual temperature/brightness/rain/wind/window-door sensors · virtual RTC ·
virtual trigger · virtual energy battery / inverter / two-way meter

Coverage is measured by a test that loads a captured real-world configuration fixture and asserts
the percentage of channels that match a profile, with a floor that CI enforces.
