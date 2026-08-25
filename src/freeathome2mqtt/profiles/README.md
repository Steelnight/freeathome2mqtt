# profiles/

Declarative, schema-validated channel profiles (ADR-003). **Data only — no code.**

- `_schema.json` — the JSON Schema every profile (built-in or user-supplied via `profiles_dir`)
  must validate against. Landed in WP3; loaded and enforced by `model/profiles.py`.

Populated in WP4/WP11:

- `lighting.yaml`, `covers.yaml`, `climate.yaml`, `sensors.yaml`, `security.yaml`, `access.yaml`,
  `energy.yaml`, `inputs.yaml` — the built-in tier-1/2/3 profiles (docs/03 §9).

See [`docs/03-model-and-profiles.md`](../../../docs/03-model-and-profiles.md).
