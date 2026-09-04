"""Regenerate ``sysap/codes/`` from vendored upstream enum snapshots (docs/01 §7; docs/11 WP1).

Run ``uv run python -m freeathome2mqtt.tools.gen_codes`` to write; add ``--check`` (used by CI) to
regenerate in memory and fail if any committed file in ``sysap/codes/`` would change. Idempotent:
running it twice produces byte-identical output.

The vendor snapshots under ``tools/vendor/`` are reviewed, committed, trusted input frozen at a
point in time (see ``tools/vendor/README.md``) — not runtime data from the SysAP or MQTT, so
importing them here is not the dynamic-execution risk CLAUDE.md rule 1 addresses.
"""

from __future__ import annotations

import argparse
import enum
import importlib.util
import sys
import textwrap
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_VENDOR_DIR = Path(__file__).parent / "vendor"
DEFAULT_CODES_DIR = Path(__file__).parent.parent / "sysap" / "codes"

_WRAP_WIDTH = 96
_REGENERATE_NOTE = (
    "Do not hand-edit; regenerate with ``uv run python -m freeathome2mqtt.tools.gen_codes`` "
    "(docs/11 WP1). See ``sysap/codes/NOTICE`` for licence attribution."
)


def _wrap(text: str) -> str:
    return "\n".join(textwrap.wrap(text, width=_WRAP_WIDTH))


def _render_header(*, summary: str, source: str, base: str, class_name: str) -> str:
    return (
        f'"""{_wrap(f"GENERATED — {summary}.")}\n'
        f"\n"
        f"{_wrap(_REGENERATE_NOTE)}\n"
        f"\n"
        f"{_wrap(f'Source: {source}.')}\n"
        f'"""\n'
        f"\n"
        f"from enum import {base}\n"
        f"\n"
        f"\n"
        f"class {class_name}({base}):\n"
        f'    """{summary}."""\n'
        f"\n"
    )


@dataclass(frozen=True, slots=True)
class _IntEnumSource:
    """One vendor-snapshot-backed IntEnum that gen_codes.py renders into sysap/codes/."""

    output: str
    class_name: str
    summary: str
    source: str
    vendor_file: str
    vendor_class: str
    exclude: frozenset[str] = field(default_factory=frozenset)


_INT_ENUM_SOURCES = (
    _IntEnumSource(
        output="pairings.py",
        class_name="Pairing",
        summary="A Free@Home pairing ID: the meaning of a value on an input or output datapoint",
        source=(
            "local-abbfreeathome's src/abbfreeathome/bin/pairing.py (MIT), itself converted "
            "from Busch-Jaeger/node-free-at-home's src/pairingIds.ts (ISC)"
        ),
        vendor_file="local_abbfreeathome_pairing.py",
        vendor_class="Pairing",
    ),
    _IntEnumSource(
        output="functions.py",
        class_name="Function",
        summary="A Free@Home channel functionID: what kind of channel this is",
        source=(
            "local-abbfreeathome's src/abbfreeathome/bin/function.py (MIT), itself converted "
            "from Busch-Jaeger/node-free-at-home's src/functionIds.ts (ISC)"
        ),
        vendor_file="local_abbfreeathome_function.py",
        vendor_class="Function",
        # Not in the official API-documentation; see the vendor file's own comment.
        exclude=frozenset({"FID_SWITCH_ACTUATOR_PYCUSTOM0"}),
    ),
    _IntEnumSource(
        output="parameters.py",
        class_name="Parameter",
        summary="A Free@Home channel/device parameter ID (e.g. a dimmer's minimum brightness)",
        source=(
            "local-abbfreeathome's src/abbfreeathome/bin/parameter.py (MIT), itself converted "
            "from Busch-Jaeger/node-free-at-home's src/parameterIds.ts (ISC)"
        ),
        vendor_file="local_abbfreeathome_parameter.py",
        vendor_class="Parameter",
    ),
)

_INTERFACE_MEMBERS = (
    ("WIRED_BUS", "TP"),
    ("WIRELESS_RF", "RF"),
    ("HUE", "hue"),
    ("SONOS", "sonos"),
    ("SMOKEALARM", "smokealarm"),
    ("VIRTUAL_DEVICE", "VD"),
)

_NOTICE_TEXT = """freeathome2mqtt vendors generated enumerations derived from two upstream projects.
Neither is a runtime dependency (ADR-002) -- their function/pairing/parameter tables and
interface values are extracted once, offline, by tools/gen_codes.py, from the frozen snapshots
in tools/vendor/ (see tools/vendor/README.md for exact source URLs and retrieval dates).

================================================================================
1. kingsleyadam/local-abbfreeathome (MIT)
================================================================================

sysap/codes/pairings.py, functions.py and parameters.py are generated from that project's
src/abbfreeathome/bin/{pairing,function,parameter}.py (member names and values only).

MIT License

Copyright (c) 2024 Adam Kingsley

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

================================================================================
2. Busch-Jaeger/node-free-at-home (ISC)
================================================================================

The pairing/function/parameter IDs above originate from this project's
src/{pairingIds,functionIds,parameterIds}.ts, which local-abbfreeathome's snapshots were
themselves converted from (see each snapshot's own module docstring). Its package.json declares:

    "author": "Stefan Guelland <Stefan.Guelland@de.abb.com>",
    "license": "ISC"

No separate LICENSE file is published in that repository; the standard ISC licence text is
reproduced below per the declared licence type.

ISC License

Copyright (c) Busch-Jaeger Elektro GmbH and contributors to
https://github.com/Busch-Jaeger/node-free-at-home

Permission to use, copy, modify, and/or distribute this software for any
purpose with or without fee is hereby granted, provided that the above
copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH
REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY
AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM
LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR
OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR
PERFORMANCE OF THIS SOFTWARE.

================================================================================
3. sysap/codes/interfaces.py
================================================================================

Authored directly from docs/01 §4.2 (this project's own protocol documentation); no upstream
source, no additional licence obligations.
"""


def load_vendor_enum(path: Path, class_name: str) -> type[enum.Enum]:
    """Import a frozen vendor snapshot and return its named ``enum.Enum`` class."""
    spec = importlib.util.spec_from_file_location(f"_vendor_{path.stem}", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load vendor snapshot: {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, class_name)  # type: ignore[no-any-return]


def _render_int_enum_file(spec: _IntEnumSource, *, vendor_dir: Path) -> str:
    vendor_enum = load_vendor_enum(vendor_dir / spec.vendor_file, spec.vendor_class)
    header = _render_header(
        summary=spec.summary, source=spec.source, base="IntEnum", class_name=spec.class_name
    )
    members = "\n".join(
        f"    {member.name} = {int(member.value)}"
        for member in vendor_enum
        if member.name not in spec.exclude
    )
    return f"{header}{members}\n"


def _render_interfaces_file() -> str:
    header = _render_header(
        summary="A Device.interface value (docs/01 §4.2); absent/null means undefined",
        source="docs/01 §4.2 (no upstream file to snapshot)",
        base="StrEnum",
        class_name="Interface",
    )
    members = "\n".join(f'    {name} = "{value}"' for name, value in _INTERFACE_MEMBERS)
    return f"{header}{members}\n"


def generate_sources(*, vendor_dir: Path = DEFAULT_VENDOR_DIR) -> dict[str, str]:
    """Render every file ``sysap/codes/`` should contain, without touching disk."""
    sources = {
        spec.output: _render_int_enum_file(spec, vendor_dir=vendor_dir)
        for spec in _INT_ENUM_SOURCES
    }
    sources["interfaces.py"] = _render_interfaces_file()
    sources["NOTICE"] = _NOTICE_TEXT
    return sources


def check_sources(sources: dict[str, str], *, codes_dir: Path = DEFAULT_CODES_DIR) -> list[str]:
    """Return the filenames whose on-disk content differs from `sources`, or is missing."""
    stale = []
    for filename, content in sources.items():
        path = codes_dir / filename
        if not path.exists() or path.read_text(encoding="utf-8") != content:
            stale.append(filename)
    return sorted(stale)


def write_sources(sources: dict[str, str], *, codes_dir: Path = DEFAULT_CODES_DIR) -> None:
    """Write every rendered file to `codes_dir`, creating it if needed."""
    codes_dir.mkdir(parents=True, exist_ok=True)
    for filename, content in sources.items():
        (codes_dir / filename).write_text(content, encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point: write by default, or verify with ``--check`` (used by CI)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="verify without writing")
    parser.add_argument("--vendor-dir", type=Path, default=DEFAULT_VENDOR_DIR)
    parser.add_argument("--codes-dir", type=Path, default=DEFAULT_CODES_DIR)
    args = parser.parse_args(argv)

    sources = generate_sources(vendor_dir=args.vendor_dir)
    if args.check:
        stale = check_sources(sources, codes_dir=args.codes_dir)
        if stale:
            print(f"stale or missing generated files: {', '.join(stale)}", file=sys.stderr)
            return 1
        print(f"{args.codes_dir} is up to date.")
        return 0

    write_sources(sources, codes_dir=args.codes_dir)
    print(f"wrote {len(sources)} files to {args.codes_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
