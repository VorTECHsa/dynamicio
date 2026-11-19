# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import os
import sys
from io import StringIO
from unittest.mock import patch

import pandas as pd
import pytest

import dynamicio.core
import dynamicio.schema.metrics
from dynamicio.config import IOConfig
from tests import constants
from tests.mocking.io import ReadS3JsonIO, WriteS3JsonIO
from tests.test_engines.ios import EngineCheckIO


class TestPatchTargetsOfConsumers:
    """The names that consumer test suites patch or import still reach the engine path of a read and a write."""

    @staticmethod
    def config(definitions: str, key: str):
        return IOConfig(path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, definitions), env_identifier="CLOUD", dynamic_vars=constants).get(source_key=key)

    @pytest.mark.unit
    def test_patched_wrangler_json_functions_take_the_write_and_the_read(self):
        # Given
        store = {}
        df = pd.DataFrame({"col_1": [1, 2], "col_2": ["a", "b"]})

        def to_json(df, path, index=True, s3_additional_kwargs=None, **pandas_kwargs):  # pylint: disable=unused-argument
            store[path] = df.to_json(index=index, **pandas_kwargs)

        # When
        with patch("dynamicio.mixins.with_s3.wr.s3.to_json", side_effect=to_json) as written:
            WriteS3JsonIO(source_config=self.config("definitions/processed.yaml", "WRITE_TO_S3_JSON")).write(df)

        # Then
        assert written.call_count == 1
        pd.testing.assert_frame_equal(pd.read_json(StringIO(next(iter(store.values())))), df)

    @pytest.mark.unit
    def test_a_patched_wrangler_read_json_is_the_frame_of_the_read(self):
        # Given
        expected = pd.DataFrame({"id": [1], "foo_name": ["x"], "bar": [2]})

        # When
        with patch("dynamicio.mixins.with_s3.wr.s3.read_json", return_value=expected.copy()) as read:
            result = ReadS3JsonIO(source_config=self.config("definitions/input.yaml", "READ_FROM_S3_JSON")).read()

        # Then
        assert read.call_count == 1
        pd.testing.assert_frame_equal(result, expected)

    @pytest.mark.unit
    def test_a_patched_log_metric_gets_the_metrics_of_a_write(self, tmp_path):
        # When
        with patch("dynamicio.metrics.log_metric") as log_metric:
            EngineCheckIO.make(tmp_path, log_schema_metrics=True).write(pd.DataFrame({"a": [4, 5]}))

        # Then
        log_metric.assert_called_once_with(dataset="POSITIVE", column="a", metric="Min", value=4)

    @pytest.mark.unit
    def test_the_old_import_paths_resolve(self):
        from dynamicio.core import CASTING_WARNING_MSG, SCHEMA_FROM_ARGUMENT, DynamicDataIO  # pylint: disable=import-outside-toplevel
        from dynamicio.schema.models import dataframe_validator, to_dataframe_schema  # pylint: disable=import-outside-toplevel

        assert dynamicio.core.SCHEMA_FROM_ARGUMENT is SCHEMA_FROM_ARGUMENT and DynamicDataIO is dynamicio.DynamicDataIO
        assert CASTING_WARNING_MSG and callable(dataframe_validator) and callable(to_dataframe_schema)
        assert sys.modules["dynamicio.metrics"] is dynamicio.schema.metrics
