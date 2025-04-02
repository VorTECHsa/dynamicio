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
    @patch("dynamicio.mixins.with_athena.connect")
    def test_read_from_athena_with_query(self, mock_connect, test_df):
        # Given: mock return values
        mock_cursor = mock_connect.return_value.cursor.return_value
        mock_cursor.execute.return_value = None  # execute returns None
        mock_cursor.fetchall.return_value = test_df
        mock_cursor.description = [("id", None), ("foo", None), ("bar", None), ("baz", None)]

        athena_config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_ATHENA")

        # When
        ReadAthenaIO(source_config=athena_config, query="SELECT * FROM dummy").read()

        # Then: assert calls
        mock_cursor.execute.assert_called_once_with("SELECT * FROM dummy")
        mock_cursor.fetchall.assert_called_once()

    @pytest.mark.unit
    def test_read_from_athena_raises_when_query_missing_raises_error(self):
        # Given
        athena_config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_ATHENA")

        # When / Then
        with pytest.raises(AssertionError, match="A 'query' must be provided for Athena read"):
            ReadAthenaIO(source_config=athena_config).read()

    @pytest.mark.unit
    @patch("dynamicio.mixins.with_athena.connect")
    def test_read_from_athena_with_query_and_options(self, mock_connect, test_df):
        # Given: mock return values
        mock_cursor = mock_connect.return_value.cursor.return_value
        mock_cursor.execute.return_value = None  # execute returns None
        mock_cursor.fetchall.return_value = test_df
        mock_cursor.description = [("id", None), ("foo", None), ("bar", None), ("baz", None)]

        athena_config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_ATHENA")

        # When
        ReadAthenaIO(source_config=athena_config, query="SELECT * FROM dummy", chunksize=1000).read()

        # Then
        mock_cursor.execute.assert_called_once_with("SELECT * FROM dummy")
        mock_cursor.fetchall.assert_called_with(chunksize=1000)

    @pytest.mark.unit
    @patch("dynamicio.mixins.with_athena.connect")
    def test_read_from_athena_filters_invalid_options(self, mock_connect, test_df):
        # Given: mock return values
        mock_cursor = mock_connect.return_value.cursor.return_value
        mock_cursor.execute.return_value = None  # execute returns None
        mock_cursor.fetchall.return_value = test_df
        mock_cursor.description = [("id", None), ("foo", None), ("bar", None), ("baz", None)]

        athena_config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_ATHENA")

        # When
        ReadAthenaIO(source_config=athena_config, query="SELECT * FROM dummy", foo="bar").read()

        # Then
        mock_cursor.execute.assert_called_once_with("SELECT * FROM dummy")
        mock_cursor.fetchall.assert_called_with()
