"""Tests for tools/gen_codes.py, the sysap/codes/ generator (docs/01 §7; docs/11 WP1)."""

import ast
from pathlib import Path

from freeathome2mqtt.tools import gen_codes


def test_vendor_snapshots_expose_expected_members() -> None:
    pairing = gen_codes.load_vendor_enum(
        gen_codes.DEFAULT_VENDOR_DIR / "local_abbfreeathome_pairing.py", "Pairing"
    )
    function = gen_codes.load_vendor_enum(
        gen_codes.DEFAULT_VENDOR_DIR / "local_abbfreeathome_function.py", "Function"
    )
    parameter = gen_codes.load_vendor_enum(
        gen_codes.DEFAULT_VENDOR_DIR / "local_abbfreeathome_parameter.py", "Parameter"
    )
    assert pairing["AL_SWITCH_ON_OFF"].value == 1
    assert function["FID_SWITCH_ACTUATOR"].value == 7
    assert parameter["PID_LED_DAY_BRIGHTNESS"].value == 1


def test_generate_sources_covers_every_output_file() -> None:
    sources = gen_codes.generate_sources()
    assert set(sources) == {
        "pairings.py",
        "functions.py",
        "parameters.py",
        "interfaces.py",
        "NOTICE",
    }


def test_generate_sources_excludes_non_official_function_member() -> None:
    sources = gen_codes.generate_sources()
    assert "FID_SWITCH_ACTUATOR_PYCUSTOM0" not in sources["functions.py"]
    assert "FID_SWITCH_ACTUATOR = 7" in sources["functions.py"]


def test_generate_sources_is_deterministic() -> None:
    first = gen_codes.generate_sources()
    second = gen_codes.generate_sources()
    assert first == second


def test_generate_sources_produces_valid_python() -> None:
    sources = gen_codes.generate_sources()
    for filename, content in sources.items():
        if filename == "NOTICE":
            continue
        ast.parse(content, filename=filename)


def test_generate_sources_interfaces_match_docs_4_2() -> None:
    sources = gen_codes.generate_sources()
    for expected in ('"TP"', '"RF"', '"hue"', '"sonos"', '"smokealarm"', '"VD"'):
        assert expected in sources["interfaces.py"]


def test_notice_retains_upstream_licence_notices() -> None:
    sources = gen_codes.generate_sources()
    notice = sources["NOTICE"]
    assert "MIT License" in notice
    assert "local-abbfreeathome" in notice
    assert "ISC" in notice
    assert "node-free-at-home" in notice


def test_check_sources_matches_committed_output() -> None:
    sources = gen_codes.generate_sources()
    stale = gen_codes.check_sources(sources, codes_dir=gen_codes.DEFAULT_CODES_DIR)
    assert stale == []


def test_check_sources_detects_missing_and_mismatched_files(tmp_path: Path) -> None:
    sources = gen_codes.generate_sources()
    (tmp_path / "pairings.py").write_text("this is stale content", encoding="utf-8")
    stale = gen_codes.check_sources(sources, codes_dir=tmp_path)
    assert set(stale) == set(sources)


def test_write_sources_then_check_reports_clean(tmp_path: Path) -> None:
    sources = gen_codes.generate_sources()
    gen_codes.write_sources(sources, codes_dir=tmp_path)
    assert gen_codes.check_sources(sources, codes_dir=tmp_path) == []


def test_main_check_mode_succeeds_against_committed_output() -> None:
    exit_code = gen_codes.main(["--check"])
    assert exit_code == 0


def test_main_check_mode_fails_on_drift(tmp_path: Path) -> None:
    exit_code = gen_codes.main(["--check", "--codes-dir", str(tmp_path)])
    assert exit_code != 0


def test_main_write_mode_round_trips(tmp_path: Path) -> None:
    write_exit_code = gen_codes.main(["--codes-dir", str(tmp_path)])
    assert write_exit_code == 0
    check_exit_code = gen_codes.main(["--check", "--codes-dir", str(tmp_path)])
    assert check_exit_code == 0
