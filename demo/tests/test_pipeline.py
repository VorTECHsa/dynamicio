"""An example pipeline to showcase how dynamicio can bt used for setting up a local e2e testing!"""
# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, unused-argument, too-few-public-methods
# noqa
import asyncio
import os

import pandas as pd
import pytest

from demo.src import processed_config, raw_config
from demo.src.runners import staging, transform


class TestPipeline:
    """Example e2e test."""

    @pytest.mark.end_to_end
    def test_dag_with_mock_sample_input_data(
        self,
        expected_staged_foo_df,
        expected_staged_bar_df,
        expected_final_foo_df,
        expected_final_bar_df,
    ):
        """Showcases how you can leverage dynamicio to read local data for fast feedback when you want to run your pipelines locally."""
        # Given
        # The src/resources/input.yaml

        # When
        staging.main()
        asyncio.run(transform.main())

        # Then
        try:
            pd.testing.assert_frame_equal(
                expected_staged_foo_df,
                pd.read_parquet(raw_config.get(source_key="STAGED_FOO").local.file_path)
            )
            pd.testing.assert_frame_equal(
                expected_staged_bar_df,
                pd.read_parquet(raw_config.get(source_key="STAGED_BAR").local.file_path)
            )
            pd.testing.assert_frame_equal(
                expected_final_foo_df,
                pd.read_parquet(processed_config.get(source_key="FINAL_FOO").local.file_path)
            )
            pd.testing.assert_frame_equal(
                expected_final_bar_df,
                pd.read_parquet(processed_config.get(source_key="FINAL_BAR").local.file_path)
            )
        finally:
            os.remove(raw_config.get(source_key="STAGED_FOO").local.file_path)
            os.remove(raw_config.get(source_key="STAGED_BAR").local.file_path)
            os.remove(processed_config.get(source_key="FINAL_FOO").local.file_path)
            os.remove(processed_config.get(source_key="FINAL_BAR").local.file_path)
