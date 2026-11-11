# pylint: disable=missing-module-docstring, missing-function-docstring
import pytest

from demo.src import staging_output_config, transform_input_config
from dynamicio.testharness import ExpectedOutputs


@pytest.fixture
def staged_inputs():
    """Place the expected outputs of staging where transform reads its inputs from, so the transform runner is tested without running staging first."""
    keys = ("STAGED_FOO", "STAGED_BAR")
    ExpectedOutputs.clear(transform_input_config, *keys)
    for key in keys:
        ExpectedOutputs.read_expected(staging_output_config, key).to_parquet(transform_input_config.get(key).local.file_path)
    yield
    ExpectedOutputs.clear(transform_input_config, *keys)
