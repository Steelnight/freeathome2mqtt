# Pedantic review of `docs/` (design plan)

Reviewer pass over `docs/00`–`docs/11`. The plan is strong: internally cross-referenced,
honest about unknowns (`⚠ verify empirically`), and unusually concrete about failure modes.
The items below are the things a pedantic reading turns up anyway — real internal
contradictions first, then gaps, then nits. Each cites file and line so it can be actioned
directly. None of these block the plan; they should be resolved before the WP they touch.

Severity: **[A]** substantive contradiction (numbers/types that cannot all be true) ·
**[B]** gap / underspecified (referenced but never defined) · **[C]** nit / wording.

**Resolution status.** ✅ **Resolved in the plan:** A1, A2, A3, A4, B1, B2, C1–C7.
⬜ **Still open (fold in later):** B3, B4, B5.

Decisions taken on the three that needed a design call:
- **A1** — command throttling is a leading-edge + per-window throttle (not a quiescence-reset
  debounce); default window raised 50 ms → **350 ms** so P5 (≤ 6 writes/2 s) holds, the ≤ 10/s
  sustained-write cap is respected, and intermediate drag values are never dropped.
- **B1** — added an explicit `primary:` profile key (defaults to the first-declared command).
- **B2** — added a declarative `on_zero:` command hook so "brightness 0 = off" needs no transform.

---

## A — Substantive contradictions

### A1. The command-debounce budget, the prose, and the property bound are mutually inconsistent
- `docs/05 §1` P5 (`05:19`): 60 `/set` over 2 s → **≤ 6** SysAP writes.
- `docs/05 §4.2` (`05:157`): a 2 s drag at 30 Hz produces **2 writes**.
- `docs/02 §5` (`02:273`): "produces **2 writes**, not 60 and not 1-arriving-late."
- `docs/10 §5` (`10:144`): debounce bound = **writes ≤ 2 + duration/window**.
- Default window is **50 ms** (`docs/05 §4.2`, `docs/07`).

These cannot all hold. The algorithm as written in `05 §4.2` (send leading edge, buffer into
`pending`, **on window close send if `pending` changed and restart the window**) is a
*throttle*, not a quiescence-reset debounce. For a continuous 2 s drag with a 50 ms window it
emits one send per window boundary → **~40 writes**, which is exactly what the property bound
`2 + duration/window = 2 + 2000/50 = 42` predicts. So:
  - the property bound (≈42) is right for the stated algorithm,
  - the "2 writes" prose describes a *different* algorithm (reset the timer on every new
    `/set`, fire only after motion stops) — which `02 §5` explicitly rejects ("not
    1-arriving-late"),
  - the "≤ 6" budget matches neither at 50 ms (it needs a window of ~400 ms).

**Fix:** pick one semantics and make all four agree. If you keep the throttle, P5 must be
`≤ 2 + duration/window` (~42 at 50 ms), and the "2 writes" claims are wrong. If you want ~6
writes, the window must be ~350–400 ms (and the leading edge keeps it responsive). If you
truly want "2 writes," it must be a reset-on-event debounce, and you have to accept the
"1-arriving-late" property for a slow continuous drag that `02 §5` currently disowns.

### A2. `egress` key type is declared two different ways
- `docs/00 §ADR-004` (`00:161`): `dict[tuple[int, int]]` keyed by `(entity_index, command_index)`.
- `docs/03 §2` (`03:100`): `dict[tuple[int, str]]` keyed by `(entity_idx, command_name)`.
- The worked example (`03 §8`) and both sequence diagrams (`02 §5`, `08 §3`) use the **string**
  form: `egress[(17, "state")]`, `egress[(17,"brightness")]`.

ADR-004 is stale. **Fix:** change ADR-004 to `(entity_index, command_name)` / `tuple[int, str]`.

### A3. Milestone M2 claims tier-1 profiles, but the profiles land in M3
- `docs/11` milestone table (`11:250`): **M2 = WP0–WP2 + WP3, WP5, WP6**, described as
  "Live state on MQTT **for tier-1 profiles**. Genuinely useful already."
- But tier-1 profile YAML is delivered by **WP4** (`11:96`), which the table puts in **M3**
  (`11:251`).

With no WP4, the compiler (WP3 ships only `_schema.json` + codecs + naming, not profile
content) matches zero channels, so M2 publishes zero entities. **Fix:** either pull WP4 into
M2, or reword M2 to "live state for whatever profiles exist" and move the "tier-1 / genuinely
useful" claim to M3. The dependency graph already has no `WP4 → WP5/WP6` edge, so the graph is
fine; only the milestone description overreaches.

### A4. Retained-persistence detection contradicts the narrow-subscription policy
- `docs/04 §8` (`04:393`): after reconnect, republish retained 2 s later, "then cancelled if
  the broker proves it did retain **by echoing `bridge/info` back**."
- But `ADR-006` / `docs/04 §1` subscriptions are exactly `+/set`, `+/set/+`, `+/get`,
  `bridge/request/#`, `homeassistant/status`. The bridge is **not** subscribed to
  `bridge/info` (or anything under `bridge/#` outbound), so it can never receive its own
  retained `bridge/info` to "prove" retention.

`docs/06 §6` F8 and `docs/09` P-42 describe the simpler, consistent behaviour (republish once,
unconditionally, 2 s after connect). **Fix:** drop the "echoing `bridge/info` back" clause in
`04 §8`, or specify the extra self-subscription it requires (and reconcile with ADR-006 —
subscribing to your own retained topic reintroduces exactly the echo ADR-006 forbids).

---

## B — Gaps / underspecified

### B1. The "primary command / primary object" concept is used but never defined
- `docs/04 §3.3` (`04:121`): a bare `ON/OFF/…` on `/set` maps to the entity's "**primary**
  command (declared in the profile; `state` for most)."
- `docs/04 §6.1` (`04:293`): `object_id` = "the attribute or the **profile's primary object**."

The profile schema (`03 §3.1`–`3.3`) has no `primary:` field, and nothing else names how the
primary attribute/command is chosen. **Fix:** add an explicit `primary:` key to the profile
schema (or state the rule: "first declared command/attribute is primary") and reference it
from both places.

### B2. "brightness `0` → off" has nowhere to live in the profile model
- `docs/09` P-08 (`09:70`): "HA's JSON light schema is handled by the profile mapping
  `brightness: 0` to `state: false`."
- The `dimming_actuator` profile (`03 §3`) clamps `brightness` to `[1,100]` (a *codec* concern)
  but has no cross-command rule turning `brightness:0` into the on/off command. That is
  cross-datapoint logic — the very thing `03 §7` reserves for `transform:` — yet
  `dimming_actuator` is deliberately **not** in the transform list (`03 §7` table). So the
  mapping asserted in P-08 is not expressible by any mechanism the plan defines.

**Fix:** either add a small declarative hook (e.g. `on_zero: state=false` on a command), or
acknowledge that plain dimmers need the `color_light`-style handling / a lightweight transform,
and adjust the "no more than a dozen transforms" expectation accordingly.

### B3. `--health` CLI flag is used in deployment but absent from the CLI spec
- `docs/07 §5.1` healthcheck (`07:236`): `["CMD", "freeathome2mqtt", "--health"]`, and `07:251`
  "Health check exits 0 only when `bridge/state` would be `online`."
- The CLI section (`07 §3`, `07:156`) lists `--check-config`, `--dry-run`, `--discover`,
  `--capture` — **no `--health`**. (There is an MQTT `health` bridge command in `04 §5`, but a
  container healthcheck needs a local process exit code, not an MQTT round-trip.)

**Fix:** add `--health` to the CLI table and specify how it determines online-ness without a
running second process (connect to the broker, read retained `bridge/state`, exit accordingly),
plus its timeout budget vs. the compose `timeout: 5s`.

### B4. HA JSON light state encoding — verify `true` vs `"ON"`
- `docs/04 §6.1` discovery uses `"schema": "json"` for the light, but the state payload
  (`04 §2`, and the worked example) publishes `"state": true` (JSON boolean).
- HA's MQTT JSON light schema conventionally expects `state` as `"ON"`/`"OFF"` strings. If that
  holds, the entity would need a `state_value_template` or the generic MQTT contract and the HA
  layer diverge on this field.

**Fix:** verify against HA's `light` JSON schema and, if needed, note that the HA discovery
layer adds the template — keeping the divergence inside `homeassistant/` as ADR-009 intends,
rather than leaking into the documented MQTT contract.

### B5. Captured-fixture CI gate is unenforceable until real hardware exists
- `docs/10 §9` CI (`10:207`): "Profile coverage ≥ 85 % of channels in `captured/*.json`."
- `captured/*.json` requires `--capture` against a real SysAP (`10 §10`, P-60); WP1 ships only
  synthetic `minimal/typical/nasty`. So this gate cannot run until after M2 when a real capture
  is contributed.

Not a contradiction, but the plan should say the captured-coverage gate is **conditional**
(skipped/soft until ≥1 capture is committed), so early CI isn't red on a missing fixture. WP4's
gate already sensibly uses `typical.json`; just note `typical.json` coverage is somewhat
self-referential (profiles authored against a synthetic fixture).

---

## C — Nits / wording

### C1. `tools/` location is ambiguous (package vs repo root)
`docs/02 §2` places `capture.py`/`gen_codes.py` under `src/freeathome2mqtt/tools/`, but
`CLAUDE.md §3` and `docs/11` invoke them as `tools/gen_codes.py` (repo root). Pick one and make
the module tree, the commands, and WP1's deliverable paths agree.

### C2. The `config.yaml` example is not valid YAML as printed
`docs/07 §2` shows top-level `sysap:` and `mqtt:` **twice each** (a "required" block then an
"optional" block) for exposition. Copy-pasted verbatim that is a duplicate-key error. Add a
one-line note that the two blocks are illustrative and must be merged, or merge them.

### C3. "`last_changed` is opt-in … default `true`"
`docs/04 §2` (`04:71`): default `true` makes it opt-**out**, not opt-in. Reword.

### C4. P7 scope is ambiguous
`docs/05 §1` P7 is "config parse + compile ≤ 400 ms" measured by `bench_compile` ("compile
only", `05:248`), yet `05 §5` (`05:177`) counts "parse + compile + **discovery rendering**" as
"roughly 400 ms". State whether discovery pre-render is inside P7 or a separate line; the cold
-start table (`05 §2`) sums to ~410 ms if it's included, which would already breach P7.

### C5. `homeassistant.legacy_entity_attributes` is defined but never used
Appears only in `docs/07 §2` (`07:66`); no other doc explains it. Either document its effect or
drop it.

### C6. `scenesTriggered` → state application missing from the architecture diagram
`docs/01 §5.1` says a `scenesTriggered` frame is both published as an event **and** applied to
state, but the `02 §1` component diagram routes `scenesTriggered` only to `EVT` (event path),
not to ingress/state. Add the second edge (or a note) so the diagram matches the prose.

### C7. Python version: `3.12+` vs `3.13-slim` image
`docs/00 §5` lists language "Python 3.12+" and the container as `python:3.13-slim`; budgets are
measured on 3.13. Consistent if read as "min 3.12, ship/measure on 3.13", but a one-line note
would remove the apparent mismatch.

---

## What's genuinely good (so it isn't lost in refactoring)
- The internal arithmetic that *does* check out: `bridge/info` counts (`04 §4.2`) —
  164 entities + 11 unsupported + 12 orphan = 187 channels. The worked example (`03 §8`)
  correctly drops `forced_position` because the channel lacks `AL_INFO_FORCE`, demonstrating the
  "drop absent non-`requires` attribute" rule end-to-end.
- The pitfall catalogue ↔ test-name ↔ WP "Closes" chain is complete and traceable; every
  `⚠ verify empirically` marker maps to a `docs/10 §10` question. Keep that discipline when
  resolving the items above — each fix should update the doc *and* its named test.

---

### Suggested resolution order
1. A1 (debounce) — it touches a budget, a property test, and two prose claims; resolving it
   changes what WP7 must implement and measure.
2. A2, A3, A4 — one-line-to-one-paragraph doc fixes, no design change.
3. B1, B2, B3 — small schema/spec additions; do before WP4 (B1/B2) and WP12 (B3).
4. B4, B5 and all of C — clarifications, fold in opportunistically.
