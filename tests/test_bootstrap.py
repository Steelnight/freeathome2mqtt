"""Bootstrap smoke tests for the WP0 package skeleton (docs/11-implementation-plan.md#wp0).

Every real module lands empty-bodied — just a docstring stating its responsibility — until its
owning work package implements it. These tests only prove the skeleton is installed and importable;
they are not a substitute for the acceptance tests each later work package must add.
"""

import importlib
import pkgutil

import freeathome2mqtt


def _submodule_names() -> list[str]:
    return [
        module_info.name
        for module_info in pkgutil.walk_packages(
            freeathome2mqtt.__path__, prefix=f"{freeathome2mqtt.__name__}."
        )
    ]


SUBMODULE_NAMES = _submodule_names()


def test_version_is_set() -> None:
    assert freeathome2mqtt.__version__


def test_skeleton_is_non_empty() -> None:
    assert len(SUBMODULE_NAMES) >= 30, "expected the full docs/02 §2 module layout to be present"


def test_submodule_imports_cleanly() -> None:
    for name in SUBMODULE_NAMES:
        importlib.import_module(name)
