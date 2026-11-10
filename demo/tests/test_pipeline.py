"""An example pipeline to showcase how dynamicio can be used for setting up a local e2e test."""
# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, unused-argument, too-few-public-methods
# noqa
import asyncio

import pytest

from demo.src import staging_output_config, transform_output_config
from demo.src.runners import staging, transform
from dynamicio.testharness import ExpectedOutputs, OutputMismatchError


class TestPipeline:
    """Example e2e test."""

    @pytest.mark.end_to_end
    def test_dag_with_mock_sample_input_data(self):
        """Run the tasks in the LOCAL environment and compare every output with the `expected` path declared next to it in the resource definitions."""
        # Given
        # The LOCAL environments of src/resources/definitions/*.yaml: sample inputs, outputs on disk, `expected` paths for the outputs

        # When / Then: all mismatches are collected and raised together on exit; files of matching outputs are removed, the others are kept to inspect
        with ExpectedOutputs() as outputs:
            staging.main()
            asyncio.run(transform.main())

            outputs.check_all(staging_output_config)  # STAGED_FOO and STAGED_BAR: every output that declares `expected`
            outputs.check_all(transform_output_config)  # FINAL_FOO and FINAL_BAR

    @pytest.mark.end_to_end
    def test_every_mismatch_is_reported_at_once(self):
        """Showcase the report: two outputs are tampered with, and both are listed in the one error."""
        # Given the staged outputs, one with a changed value and one with a missing column
        staging.main()
        try:
            outputs = ExpectedOutputs()
            changed = outputs.read_generated(staging_output_config, "STAGED_FOO")
            changed.loc[0, "column_d"] = -1
            missing_column = outputs.read_generated(staging_output_config, "STAGED_BAR").drop(columns=["column_a"])

            # When
            outputs.compare(changed, outputs.read_expected(staging_output_config, "STAGED_FOO"), "STAGED_FOO")
            outputs.compare(missing_column, outputs.read_expected(staging_output_config, "STAGED_BAR"), "STAGED_BAR")

            # Then
            with pytest.raises(OutputMismatchError) as error:
                outputs.raise_if_failed()
            assert [name for name, _ in error.value.errors] == ["STAGED_FOO", "STAGED_BAR"]
        finally:
            ExpectedOutputs.clear(staging_output_config, "STAGED_FOO", "STAGED_BAR")
