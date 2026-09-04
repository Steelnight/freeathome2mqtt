"""Checks that every relative markdown link inside a docs tree resolves -- both the target file
and, for a link carrying a `#fragment`, a matching heading anchor within that file (docs/10 §9's
"Docs links" CI gate; docs/11 WP12).

Only relative links are checked: an `http(s)://`/`mailto:`/protocol-relative (`//`) URL is left to
a human (or a real link checker with network access) -- out of scope here, since this project's own
egress is not guaranteed at CI time. Anchor matching follows GitHub's own heading-slug rules:
lowercase, spaces become hyphens, everything but word characters/hyphens/underscores is stripped,
and the Nth repeat of a slug in one file gets `-N` appended -- the same rules GitHub renders
`#anchor` links against, so a link that resolves here resolves on GitHub too.

Run via ``uv run python -m freeathome2mqtt.tools.check_docs_links`` (defaults to ``docs/``).
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Sequence
from pathlib import Path

_LINK_RE = re.compile(r"\[[^\]]*\]\(([^)]+)\)")
_HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*$", re.MULTILINE)
_INLINE_CODE_RE = re.compile(r"`([^`]*)`")
_INLINE_LINK_RE = re.compile(r"\[([^\]]*)\]\([^)]*\)")
_NON_SLUG_RE = re.compile(r"[^\w\- ]")


def _slugify_heading(heading: str) -> str:
    text = _INLINE_CODE_RE.sub(r"\1", heading)
    text = _INLINE_LINK_RE.sub(r"\1", text)
    slug = _NON_SLUG_RE.sub("", text.strip().lower())
    return slug.replace(" ", "-")


def _heading_slugs(markdown: str) -> set[str]:
    slugs: set[str] = set()
    seen: dict[str, int] = {}
    for _hashes, heading in _HEADING_RE.findall(markdown):
        base = _slugify_heading(heading)
        count = seen.get(base, 0)
        seen[base] = count + 1
        slugs.add(base if count == 0 else f"{base}-{count}")
    return slugs


def _is_external(target: str) -> bool:
    return target.startswith(("http://", "https://", "mailto:", "//"))


def _check_link(md_file: Path, target: str) -> str | None:
    path_part, _, fragment = target.partition("#")
    resolved = md_file if not path_part else (md_file.parent / path_part).resolve()
    if path_part and not resolved.is_file():
        return f"{md_file}: '{target}' -> {resolved} does not exist"
    if fragment and resolved.suffix == ".md":
        slugs = _heading_slugs(resolved.read_text(encoding="utf-8"))
        if fragment not in slugs:
            return f"{md_file}: '{target}' -> no heading '#{fragment}' in {resolved}"
    return None


def find_broken_links(docs_dir: Path) -> list[str]:
    """One message per relative link under `docs_dir` whose target file or anchor is missing."""
    broken: list[str] = []
    for md_file in sorted(docs_dir.rglob("*.md")):
        text = md_file.read_text(encoding="utf-8")
        for match in _LINK_RE.finditer(text):
            target = match.group(1).strip()
            if not target or _is_external(target):
                continue
            message = _check_link(md_file, target)
            if message is not None:
                broken.append(message)
    return broken


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entry point (used by the fast suite's `test_real_docs_links_resolve`, and standalone)."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docs-dir", type=Path, default=Path("docs"))
    args = parser.parse_args(argv)

    broken = find_broken_links(args.docs_dir)
    if broken:
        for message in broken:
            print(message, file=sys.stderr)
        print(f"{len(broken)} broken relative link(s) in {args.docs_dir}", file=sys.stderr)
        return 1
    print(f"all relative links in {args.docs_dir} resolve")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
