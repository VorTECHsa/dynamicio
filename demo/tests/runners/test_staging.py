# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
# noqa
import pytest

from demo.src import staging_output_config
from demo.src.runners import staging
from dynamicio.testharness import ExpectedOutputs


class TestStaging:
    @pytest.mark.integration
    def test_staging_writes_the_staged_foo_and_bar_datasets(self):
        # Given the LOCAL sample inputs (staging_input.yaml) and the expected outputs declared in staging_output.yaml

        # When
        with ExpectedOutputs() as outputs:
            staging.main()

            # Then
            outputs.check(staging_output_config, "STAGED_FOO")
            outputs.check(staging_output_config, "STAGED_BAR")
