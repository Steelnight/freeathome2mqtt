# profiles/

Declarative, schema-validated channel profiles (ADR-003). **Data only — no code.**

Populated in WP3/WP4/WP11:

- `_schema.json` — the JSON Schema every profile (built-in or user-supplied via `profiles_dir`)
  must validate against.
- `lighting.yaml`, `covers.yaml`, `climate.yaml`, `sensors.yaml`, `security.yaml`, `access.yaml`,
  `energy.yaml`, `inputs.yaml` — the built-in tier-1/2/3 profiles (docs/03 §9).

See [`docs/03-model-and-profiles.md`](../../../docs/03-model-and-profiles.md).
