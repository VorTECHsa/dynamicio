# pylint: disable=no-member, missing-docstring, too-few-public-methods, protected-access
import os
from unittest.mock import ANY, patch

import pandas as pd
import pytest

from dynamicio import UnifiedIO
from dynamicio.config import IOConfig
from tests import constants


class ReadAthenaIO(UnifiedIO):
    schema = {"id": "object", "foo": "object", "bar": "int64", "baz": "object"}


@pytest.mark.unit
class TestAthenaIO:
    @pytest.mark.unit
    @patch("dynamicio.mixins.with_athena.connect")
    def test_read_from_athena_with_query(self, mock_connect, test_df):
        # Setup mocks
        mock_cursor = mock_connect.return_value.cursor.return_value
        mock_cursor.execute.return_value.fetch_df.return_value = test_df

        # Given
        athena_config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_ATHENA")

        # When
        df = ReadAthenaIO(source_config=athena_config, query="SELECT * FROM dummy").read()

        # Then
        assert df.equals(test_df)
        mock_connect.assert_called_once()
        mock_cursor.execute.assert_called_with("SELECT * FROM dummy")
        mock_cursor.execute.return_value.fetch_df.assert_called_once()

    @pytest.mark.unit
    @patch.object(pd, "read_sql_query")
    def test_read_from_athena_with_query_and_options(self, mock_read_sql_query):
        # Given
        athena_config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_ATHENA")

        # When
        ReadAthenaIO(
            source_config=athena_config,
            sql_query="SELECT * FROM dummy",
            chunksize=1000
        ).read()

        # Then
        mock_read_sql_query.assert_called_with(
            sql="SELECT * FROM dummy", con=ANY, chunksize=1000
        )
