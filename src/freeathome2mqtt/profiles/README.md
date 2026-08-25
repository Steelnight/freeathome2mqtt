# profiles/

Declarative, schema-validated channel profiles (ADR-003). **Data only — no code.**

- `_schema.json` — the JSON Schema every profile (built-in or user-supplied via `profiles_dir`)
  must validate against. Landed in WP3; loaded and enforced by `model/profiles.py`.
- `lighting.yaml`, `covers.yaml`, `climate.yaml`, `sensors.yaml` — the tier-1 profiles (docs/03
  §9): switch/dimming/colour-temperature actuators, plain and slatted covers, room temperature
  controller and heating actuator, and the common sensor types. Landed in WP4, each with a
  round-trip fixture in `tests/test_tier1_profiles.py`.

Populated in WP11 (tier 2/3 and raw mode):

- `security.yaml`, `access.yaml`, `energy.yaml`, `inputs.yaml` — the remaining built-in profiles
  (docs/03 §9).

See [`docs/03-model-and-profiles.md`](../../../docs/03-model-and-profiles.md).
