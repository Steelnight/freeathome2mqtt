"""tools/check_docs_links.py: docs/10 §9's "Docs links" CI gate -- every relative link (and
`#fragment`) inside docs/ resolves (docs/11 WP12).
"""

from __future__ import annotations

from pathlib import Path

import pytest

from freeathome2mqtt.tools.check_docs_links import find_broken_links, main

REAL_DOCS_DIR = Path(__file__).resolve().parent.parent / "docs"


def test_relative_link_to_missing_file_is_reported(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text("see [b](b.md)\n", encoding="utf-8")
    broken = find_broken_links(tmp_path)
    assert len(broken) == 1
    assert "b.md" in broken[0]


def test_relative_link_to_existing_file_is_clean(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text("see [b](b.md)\n", encoding="utf-8")
    (tmp_path / "b.md").write_text("# B\n", encoding="utf-8")
    assert find_broken_links(tmp_path) == []


def test_external_links_are_ignored(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text(
        "see [x](https://example.com) and [y](mailto:a@example.com)\n", encoding="utf-8"
    )
    assert find_broken_links(tmp_path) == []


def test_link_with_valid_anchor_is_clean(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text("see [b](b.md#section-one)\n", encoding="utf-8")
    (tmp_path / "b.md").write_text("# B\n\n## Section One\n", encoding="utf-8")
    assert find_broken_links(tmp_path) == []


def test_link_with_missing_anchor_is_reported(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text("see [b](b.md#nonexistent)\n", encoding="utf-8")
    (tmp_path / "b.md").write_text("# B\n\n## Section One\n", encoding="utf-8")
    broken = find_broken_links(tmp_path)
    assert len(broken) == 1
    assert "nonexistent" in broken[0]


def test_same_file_anchor_is_checked(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text(
        "# A\n\n## Real Section\n\nsee [above](#real-section)\n", encoding="utf-8"
    )
    assert find_broken_links(tmp_path) == []


def test_same_file_missing_anchor_is_reported(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text("# A\n\nsee [above](#missing)\n", encoding="utf-8")
    broken = find_broken_links(tmp_path)
    assert len(broken) == 1


def test_duplicate_headings_get_disambiguated_slugs(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text(
        "# A\n\n## Notes\n\ntext\n\n## Notes\n\nsee [second](#notes-1)\n", encoding="utf-8"
    )
    assert find_broken_links(tmp_path) == []


def test_real_docs_links_resolve() -> None:
    assert find_broken_links(REAL_DOCS_DIR) == []


def test_main_exits_zero_and_prints_summary_when_clean(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "a.md").write_text("# A\n", encoding="utf-8")

    exit_code = main(["--docs-dir", str(tmp_path)])

    assert exit_code == 0
    assert "resolve" in capsys.readouterr().out


def test_main_exits_one_and_prints_broken_links_to_stderr(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    (tmp_path / "a.md").write_text("see [b](b.md)\n", encoding="utf-8")

    exit_code = main(["--docs-dir", str(tmp_path)])

    assert exit_code == 1
    assert "b.md" in capsys.readouterr().err
