# pylint: disable=missing-module-docstring, missing-function-docstring
import os

import pytest

from demo.polars.src import staging_output_config, transform_input_config, transform_output_config
from dynamicio.testharness import ExpectedOutputs


def pytest_ignore_collect(collection_path, config):  # pylint: disable=unused-argument
    """The polars pipeline runs only with ``--dynamicio-engine=polars``: the engine is a setting of the process, and the other demo tests use pandas."""
    return True if config.getoption("dynamicio_engine", default=None) != "polars" else None


class Outputs:
    """The files that the runners of the polars pipeline write."""

    STAGED = ("STAGED_FOO", "STAGED_BAR")
    FINAL = ("FINAL_FOO", "FINAL_BAR")

    @classmethod
    def clear(cls) -> None:
        ExpectedOutputs.clear(staging_output_config, *cls.STAGED)
        ExpectedOutputs.clear(transform_output_config, *cls.FINAL)
        for key in cls.STAGED:
            path = transform_input_config.get(source_key=key).local.file_path
            if os.path.exists(path):
                os.remove(path)


@pytest.fixture
def clean_outputs():
    """Remove the files of the runners before and after a test, so a stale file can not pass for a new output."""
    Outputs.clear()
    os.makedirs(os.path.dirname(staging_output_config.get(source_key="STAGED_FOO").local.file_path), exist_ok=True)
    os.makedirs(os.path.dirname(transform_output_config.get(source_key="FINAL_FOO").local.file_path), exist_ok=True)
    yield
    Outputs.clear()
