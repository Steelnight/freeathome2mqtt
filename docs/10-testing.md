# 10 — Testing Strategy

The hard part of testing this project is that neither dependency is available in CI: there is no
SysAP and no broker. Everything below exists to make the whole system testable without either, so
that an implementing agent can iterate quickly and CI can actually gate the budgets in
[`docs/05`](05-performance.md).

## 1. Shape of the suite

| Layer | Scope | Speed | Count |
|---|---|---|---|
| **Unit** | Codecs, slugify, profile validation, backoff, debounce, topic building | < 1 s total | ~200 |
| **Compiler** | Config fixture → expected `Model`, golden-file compared | < 5 s | ~60 |
| **Integration** | Fake SysAP + embedded broker + the real application | < 60 s | ~80 |
| **Benchmark** | The budgets in `docs/05 §1` | ~5 min | 10 |
| **Soak** | 24 h with induced failures; nightly, not per-PR | 24 h | 1 |

Coverage floor: **90 % lines, 85 % branches**, with `model/` and `bus/` at **95 %** — those two are
where correctness lives and they are pure enough to have no excuse.

## 2. What makes this testable

Three deliberate design properties, all from [`docs/02`](02-architecture.md):

1. **`model/` is pure.** `compile(config, profiles, options) -> Model` has no I/O, no clock and no
   randomness. Most of the correctness risk is therefore reachable by a plain function call with a
   JSON fixture.
2. **`sysap/` and `mqtt/` never import each other.** Each can be exercised against a fake of the
   other.
3. **Time is injected.** Every timer takes a clock (`loop.time` by default). Tests use a fake clock
   and never `sleep`. A test suite that sleeps for debounce windows is a test suite nobody runs.

## 3. Fixtures and the fake SysAP

### 3.1 `tests/fakes/fake_sysap.py`

An `aiohttp` application implementing the real endpoints, plus a scripting interface:

```python
async def test_reconnect_resyncs(fake_sysap, bridge):
    await bridge.started()

    fake_sysap.set_datapoint("ABB7F500E17A", "ch0003", "odp0000", "1")   # change while down
    await fake_sysap.drop_websocket()                                     # silent drop, no close
    await fake_sysap.restore_websocket()

    await bridge.resynced()
    assert fake_sysap.request_count("/api/rest/configuration") == 2       # exactly one resync
    assert bridge.state_of("ABB7F500E17A_ch0003")["state"] is True
```

Capabilities it must have — each exists to cover a specific pitfall:

| Capability | Covers |
|---|---|
| Serve a configuration fixture; mutate it at runtime | P-13, P-23 |
| Push arbitrary WS frames, including all five keys | P-13 |
| `drop_websocket()` — close cleanly | P-21 |
| `hang_websocket()` — stop responding **without** closing (the nasty one) | P-21 |
| `set_latency(ms)`, `set_error(path, status)` | P-20, F3 |
| Return `502` above N concurrent requests | P-46, P-52, budget P11 |
| Return `200` with a non-OK `result` | P-12 |
| Count requests per path; assert concurrency never exceeded `max_inflight` | budget P11 |
| Serve a non-default SysAP UUID | P-10 |
| Serve `""` values, `"42.0"` integers, null `rooms` | P-04, P-05, P-14 |

### 3.2 Configuration fixtures

| Fixture | Purpose |
|---|---|
| `minimal.json` | 1 device, 1 switch channel. The smoke test. |
| `typical.json` | ~40 devices, ~180 channels, realistic mix. The default for integration tests. |
| `large.json` | 250 devices, 1 000 channels. Generated, used by the benchmarks. |
| `stress.json` | 500 devices, 2 500 channels. |
| `nasty.json` | Every quirk at once: empty values, `"42.0"`, placeholder names, null rooms, orphans, unknown function IDs, colliding names, umlauts, a non-default UUID. |
| `captured/*.json` | **Real, pseudonymised captures.** CI runs against at least one. |

`nasty.json` is worth building deliberately rather than incrementally: it converts most of
[`docs/09 §A`](09-pitfalls.md#a-freehome-protocol) into a single compile-and-assert test.

### 3.3 The capture tool

`freeathome2mqtt --capture out.json` against a real SysAP records the configuration plus a
WebSocket session, and **pseudonymises** consistently: serials remapped to a stable synthetic
namespace, display names replaced with generated ones, room and floor names replaced, credentials
never written.

This is what turns "my blinds don't work" into a regression test. It only works if users trust it,
so the redaction is tested directly:

```python
def test_capture_contains_no_identifying_data(real_config_sample):
    out = capture.pseudonymise(real_config_sample)
    blob = orjson.dumps(out).decode()
    for secret in ("installer", PASSWORD, "ABB7F500E17A", "Küche", "Familie Müller"):
        assert secret not in blob
```

### 3.4 The broker

`pytest-mosquitto` or a Mosquitto container via `testcontainers`, with an in-process fallback so the
suite runs without Docker. Tests assert on **actual broker state** — retained payloads, subscription
sets, QoS flags — not on mock call arguments. Mocking the MQTT client would let every retain/QoS
pitfall in [`docs/09 §C`](09-pitfalls.md#c-mqtt-and-home-assistant) through untested.

## 4. Table-driven profile tests

Since profiles are data ([ADR-003](00-overview-and-decisions.md#adr-003)), one parametrised suite
covers all of them:

```python
@pytest.mark.parametrize("profile", ALL_PROFILES, ids=lambda p: p.id)
def test_profile_wellformed(profile):
    assert profile.functions
    assert profile.attributes
    for attr in profile.attributes.values():
        assert attr.codec in CODEC_REGISTRY
        assert attr.pairing.name.startswith(("AL_INFO", "AL_")), "attributes bind to outputs"
        if attr.codec == "enum":
            assert attr.values
    for cmd in profile.commands.values():
        assert cmd.optimistic is None or cmd.optimistic in profile.attributes
    assert not (set(profile.commands) - set(profile.attributes)) or profile.transform, \
        "a command with no corresponding attribute needs a transform to be observable"
```

Plus, per profile, a **round-trip fixture**: a synthetic channel → compile → assert the ingress and
egress tables → feed each attribute a value → assert the published payload. Adding a profile means
adding ~15 lines of YAML and one fixture, which is the whole point of the declarative design.

## 5. Property-based tests

`hypothesis` where the input space is large and the invariants are crisp:

| Property | Statement |
|---|---|
| Codec round-trip | For every codec and every valid value, `decode(encode(v)) == v` |
| Codec totality | No decoder raises for **any** string input; malformed input yields `None` |
| Slug validity | For any Unicode string, the slug matches `^[a-z0-9_]{1,64}$` |
| Slug stability | The same name always yields the same slug |
| Collision resolution | For any set of names, resolved slugs are unique and order-independent |
| Compile determinism | Shuffling device/channel iteration order yields a byte-identical `Model` |
| Dirty-set equivalence | Any sequence of updates then a flush yields the same final published state as publishing each update individually |
| Debounce bound | For any `/set` sequence, writes ≤ `2 + duration/window` |

The last two are the formal statements of budgets P4 and P5, and they catch the coalescing bugs that
example-based tests miss.

## 6. Integration scenarios

Each maps to a workflow in [`docs/08`](08-workflows.md) and a pitfall.

| Test | Asserts |
|---|---|
| `test_cold_start_publishes_discovery_then_state` | Ordering; `bridge/state: online` last (§08.1) |
| `test_no_events_lost_during_startup_window` | Fake SysAP changes a datapoint between WS connect and config response; final state is correct (P-22) |
| `test_command_roundtrip_optimistic_then_confirmed` | Optimistic publish, PUT, echo, unconfirmed cleared (§08.3) |
| `test_command_failure_rolls_back` | Non-OK result → error response + reconciliation → correct state (P-12, P-53) |
| `test_ws_hang_triggers_reconnect` | `hang_websocket()` → reconnect within `ws_idle_timeout` (P-21) |
| `test_resync_publishes_only_deltas` | 1 change during a 60 s outage → exactly 1 state publish (P-23, §08.4) |
| `test_device_added_appears_without_restart` | `devicesAdded` → discovery + state (P-13, §08.5) |
| `test_device_removed_is_retracted` | Empty retained on state, availability, discovery (P-35, §08.6) |
| `test_rename_preserves_unique_id` | Old topics cleared, `unique_id` unchanged (P-33, P-34, §08.7) |
| `test_ha_birth_republishes_after_delay` | (P-36, P-37, §08.8) |
| `test_broker_outage_state_correct_on_reconnect` | Ingestion continued; deltas published (P-26, §08.9) |
| `test_shutdown_flushes_pending_commands` | A command in the debouncer at SIGTERM still reaches the SysAP (§08.10) |
| `test_sysap_502_reduces_concurrency` | Adaptive limiter halves and recovers (F3) |
| `test_no_secrets_in_logs_or_bridge_info` | Sentinel secrets absent from all output (P-45) |
| `test_bridge_subscribes_only_to_command_topics` | Subscription set exactly as specified (P-31) |

## 7. Benchmarks

Per [`docs/05 §8`](05-performance.md#8-benchmarks). Run against the fake SysAP and a real embedded
broker so they measure the whole path, not a microbenchmark of one function.

Results go to `bench/results.json`; CI compares against a committed baseline (`bench/baseline.json`)
and fails on a regression beyond 25 %, via `tools/compare_bench.py`. Because CI runners are noisy,
the gate is on **relative** regression, and the absolute budgets in `docs/05 §1` are verified on the
reference Pi 4 before each release.

**As implemented (WP12), this relative-baseline comparison covers only `test_bench_compile`.** It
is the sole bench test that is synchronous, CPU-bound work over pure data structures, which is why
it alone uses pytest-benchmark's `benchmark` fixture (the thing that actually produces the
JSON-diffable, storage-backed stats this comparison needs). Every other `tests/bench/` module
benchmarks an async path against the fake SysAP/broker — real socket I/O — where the fixture's
synchronous `benchmark()` call doesn't apply; those keep asserting directly against their own
absolute `docs/05 §1` budget with manual `time.perf_counter()` timing, as they always have. CI's
`bench` job (main branch only, per the table below) runs the **full** `pytest -m bench` suite
either way, so every budget is still checked on every merge to `main` — it just wasn't run in CI
at all before WP12. Extending the relative-regression comparison to the async benchmarks (e.g. via
a hand-rolled timer wrapper matching this project's own "minimal dependency surface" bent) is a
tracked future improvement, not attempted here.

## 8. Soak test

Nightly, 24 hours, against the fake SysAP with a chaos script:

- Random WS drops and hangs every 5–30 minutes.
- Broker restarts every 2 hours.
- `502` bursts.
- Config changes (device add/remove/rename) every 30 minutes.
- Continuous synthetic traffic at 50 dp/s with periodic 500-frame bursts.

Assertions at the end:

- RSS growth < 10 % over 24 h (catches P-51 and every unbounded-collection bug).
- Zero unhandled exceptions in the log.
- Final state matches the fake SysAP's ground truth for **every** entity — the strongest possible
  statement that no resync path loses data.
- `task_restarts` == expected; no escalations.
- The chaos phase actually consumed the wall-clock duration it was given. This reads like a
  tautology and is not one: the loop originally advanced a nominal counter by `duration_s / 10`
  per cycle rather than reading the clock, so it ran exactly ten cycles for *any* duration and
  returned in under a second. Every assertion above still passed, on a run that had soaked for no
  time at all. A soak with no elapsed time cannot observe the things a soak exists to observe.

**Duration in CI.** `FAH2MQTT_SOAK_DURATION_S` sets the run length: 20 s by default so the harness
is exercised locally, **18000 s (5 h)** on the nightly cron. Not the literal 24 h above: a job on a
GitHub-hosted runner is terminated at 6 hours of execution time, a hard platform cap that
`timeout-minutes` can lower but never raise. The 24 h figure is reachable only on a self-hosted
runner, by dispatching `soak.yml` with `duration_seconds=86400`. Chaos *frequency* still scales
with the run and is capped at one cycle per 5 minutes, so the 5 h run produces a WS drop or hang
every 5 minutes and a config change every 20 — within the ranges this section specifies — rather
than the same ten events spread thinner. This is the same class of documented deviation as
`bench_ingest`'s traffic window (§7).

## 9. CI pipeline

| Stage | Gate |
|---|---|
| `ruff check` + `ruff format --check` | Includes `G004` (no f-strings in logging) for P-50 |
| `mypy --strict` on `src/` | |
| `pytest -m "not bench and not soak"` | Coverage floors |
| Profile schema validation | Every YAML validates against `_schema.json` |
| `gen_codes.py --check` | Generated files are byte-identical to committed ones (P-58) |
| Profile coverage | ≥ 85 % of channels in `captured/*.json` match a profile — **conditional**: skipped (not failed) until at least one real capture is committed, since captures require hardware (P-60) and none exists before M2. WP4/WP11 gate coverage against the authored `typical.json`/`tier2_tier3.json` fixtures in the meantime; the gate becomes blocking the moment a `captured/*.json` lands. |
| Docs links | Every relative link (and `#fragment`) in `docs/` resolves — `tools/check_docs_links.py`, gated by `test_real_docs_links_resolve` inside the `pytest -m "not bench and not soak"` row above rather than a separate CI job, the same way profile coverage above is |
| `pytest -m bench` | Regression gate vs. baseline (main branch only, §7) |
| Container build | Multi-arch, plus a smoke test that the image starts and `--check-config` passes |

## 10. Manual verification against real hardware

Some things cannot be faked, and the plan should say so honestly. Before release, on a real
installation, verify every **⚠ verify empirically** marker in
[`docs/01`](01-freeathome-api.md):

1. Does an `unresponsive` transition arrive as a `devices` WS frame, or only via config polling?
   (Determines the `config_refresh_interval` default — [`docs/06 §4.1`](06-resilience.md#41-when-to-resync).)
2. Do `scenesTriggered` frames duplicate the corresponding `datapoints` entries?
3. Are WS `datapoints` keys always in `odpXXXX` form?
4. Does the current firmware accept `installer` as the Basic-auth username, or is the `jid` needed?
5. What is the actual concurrency threshold at which `502`s begin? (Calibrates `max_inflight`.)
6. Does every actuator type echo its command on the WebSocket? (Sets `confirm` per profile.)
7. Cover and slat orientation on real hardware, per actuator type.
8. Cross-check the generated `pairings.py` (docs/01 §7) against a live `GET /api/rest/pairings` —
   the vendored snapshot has not been verified against a real SysAP's own pairing catalogue.

Each answer updates `docs/01` and adds a fixture. Until then the defaults are conservative, and the
markers stay in the document rather than being quietly dropped.
