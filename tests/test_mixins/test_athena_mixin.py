# pylint: disable=no-member, missing-docstring, too-few-public-methods, protected-access
import os
from unittest.mock import patch

import pytest

# Application Imports
from dynamicio.config import IOConfig
from tests import constants
from tests.mocking.io import ReadAthenaIO


@pytest.mark.unit
class TestAthenaIO:
    @pytest.mark.unit
    @patch("dynamicio.mixins.with_athena.wr.athena.read_sql_query")
    def test_read_from_athena_with_query(self, mock_wr_read, test_df):
        # Given
        mock_wr_read.return_value = test_df
        athena_config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_ATHENA")

        # When
        df = ReadAthenaIO(source_config=athena_config, query="SELECT * FROM dummy").read()

        # Then
        mock_wr_read.assert_called_once()
        assert df.equals(test_df)

    @pytest.mark.unit
    def test_read_from_athena_raises_when_query_missing_raises_error(self):
        # Given
        athena_config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_ATHENA")

        # When / Then
        with pytest.raises(ValueError, match="A 'query' must be provided for Athena reads"):
            ReadAthenaIO(source_config=athena_config).read()

    @pytest.mark.unit
    @patch("dynamicio.mixins.with_athena.wr.athena.read_sql_query")
    def test_read_from_athena_with_query_and_valid_options(self, mock_wr_read, test_df):
        # Given
        mock_wr_read.return_value = test_df
        athena_config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_ATHENA")

        # When
        df = ReadAthenaIO(source_config=athena_config, query="SELECT * FROM dummy", ctas_approach=True).read()

        # Then
        _, kwargs = mock_wr_read.call_args
        assert "ctas_approach" in kwargs and kwargs["ctas_approach"] is True
        assert df.equals(test_df)

    @pytest.mark.unit
    @patch("dynamicio.mixins.with_athena.wr.athena.read_sql_query")
    def test_read_from_athena_filters_invalid_options(self, mock_wr_read, test_df):
        # Given
        mock_wr_read.return_value = test_df
        athena_config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_ATHENA")

        # When
        df = ReadAthenaIO(source_config=athena_config, query="SELECT * FROM dummy", foo="bar").read()

        # Then
        call_kwargs = mock_wr_read.call_args.kwargs
        assert "foo" not in call_kwargs
        assert df.equals(test_df)
