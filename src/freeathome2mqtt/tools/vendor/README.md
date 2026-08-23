# vendor/

Frozen, verbatim snapshots of upstream enum sources used by `tools/gen_codes.py` (docs/11 WP1) to
generate `sysap/codes/`. See [ADR-002](../../../../docs/00-overview-and-decisions.md#adr-002) and
[`docs/01 §7`](../../../../docs/01-freeathome-api.md#7-code-tables-to-generate).

**Not hand-edited, and not auto-refreshed.** Updating a snapshot is a deliberate act: re-fetch the
file, review the diff for renamed/removed/changed members, update the table below, then regenerate
`sysap/codes/` and commit both together.

`tools/gen_codes.py` imports these files directly (`importlib.util.spec_from_file_location`) rather
than re-parsing their text, because they are reviewed, committed, trusted input — not runtime data
from the SysAP or MQTT (CLAUDE.md rule 1 governs the latter, not this).

| File | Upstream source | Licence | Retrieved |
|---|---|---|---|
| `local_abbfreeathome_pairing.py` | [`kingsleyadam/local-abbfreeathome@main:src/abbfreeathome/bin/pairing.py`](https://github.com/kingsleyadam/local-abbfreeathome/blob/main/src/abbfreeathome/bin/pairing.py) | MIT | 2026-08-23 |
| `local_abbfreeathome_function.py` | [`kingsleyadam/local-abbfreeathome@main:src/abbfreeathome/bin/function.py`](https://github.com/kingsleyadam/local-abbfreeathome/blob/main/src/abbfreeathome/bin/function.py) | MIT | 2026-08-23 |
| `local_abbfreeathome_parameter.py` | [`kingsleyadam/local-abbfreeathome@main:src/abbfreeathome/bin/parameter.py`](https://github.com/kingsleyadam/local-abbfreeathome/blob/main/src/abbfreeathome/bin/parameter.py) | MIT | 2026-08-23 |
| `local_abbfreeathome_interface.py` | [`kingsleyadam/local-abbfreeathome@main:src/abbfreeathome/bin/interface.py`](https://github.com/kingsleyadam/local-abbfreeathome/blob/main/src/abbfreeathome/bin/interface.py) | MIT | 2026-08-23 |

Each of `pairing.py`/`function.py`/`parameter.py` documents itself as converted from the
corresponding `Busch-Jaeger/node-free-at-home` TypeScript source
(`src/pairingIds.ts`/`src/functionIds.ts`/`src/parameterIds.ts`, ISC-licensed per that repository's
`package.json`) — see each file's own module docstring. `gen_codes.py` excludes
`FID_SWITCH_ACTUATOR_PYCUSTOM0` from `function.py`, which that file's own comment marks as
"custom to this python package... not in the official API-documentation."

`interfaces.py` has no upstream file to snapshot — its members come directly from
[`docs/01 §4.2`](../../../../docs/01-freeathome-api.md#42-interfaces); `gen_codes.py` still
generates it, from a literal table in the generator itself, so all four files share one
byte-identical-on-regeneration guarantee.

SHA-256 of the snapshots above, at the retrieval date:

```
cc87ead237472c93871b25c24c3c1b031d1d9411530be6698348847fa6c061c3  local_abbfreeathome_pairing.py
c7db17958ba9db8c9a2e3abcb208a94ad154aa26ad4fc0ef9a84ef0dcc0d9a1c  local_abbfreeathome_function.py
0db122d0d079834de8b1fdd332d79c8d4f5aa099c02ad9bc1d46f0f4a1cff7b0  local_abbfreeathome_parameter.py
d1a95303adbd03e39e3047b647481c4fa84e724979a91fb9c4b96997e3074b69  local_abbfreeathome_interface.py
```
