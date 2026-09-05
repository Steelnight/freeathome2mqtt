"""Owns the async benchmark report's lifecycle (docs/12 WP13).

Session-scoped and autouse so any `pytest -m bench` run -- the whole suite or one module --
produces `bench/results-async.json` for `tools/compare_bench.py` without each test module having
to remember to write it.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from . import _record


@pytest.fixture(scope="session", autouse=True)
def _async_bench_report() -> Iterator[None]:
    _record.reset()
    yield
    _record.write_report()
