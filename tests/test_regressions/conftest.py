import imp
import pathlib

import pytest


@pytest.fixture
def regressions_resources_dir() -> pathlib.Path:
    return (pathlib.Path(__file__).parent / "resources").resolve()


@pytest.fixture
def tests_resources_dir(regressions_resources_dir):
    return regressions_resources_dir.parent.parent / "resources"


@pytest.fixture
def regressions_constants_module(regressions_resources_dir, tests_resources_dir):
    mod = imp.new_module("regressions_constants_module")
    mod.__dict__.update(
        {
            "REGRESSIONS_RESOURCES_DIR": str(regressions_resources_dir),
            "TEST_RESOURCES_DIR": str(tests_resources_dir),
        }
    )
    return mod
