# pylint: disable=no-member, missing-module-docstring, missing-class-docstring, missing-function-docstring, too-many-public-methods, too-few-public-methods, protected-access, C0103, C0302, R0801
import os
from unittest.mock import ANY, patch

import pandas as pd
import pytest
from sqlalchemy.sql.base import ImmutableColumnCollection

from dynamicio import WithPostgres
from dynamicio.config import IOConfig
from tests import constants
from tests.mocking.io import (
    ReadPostgresIO,
    WriteExtendedPostgresIO,
    WritePostgresIO,
)
from tests.mocking.models import ERModel, PgModel


class TestPostgresIO:
    @pytest.mark.unit
    def test_when_reading_from_postgres_with_env_as_cloud_get_table_columns_returns_valid_list_of_columns_for_a_model(self, expected_columns):
        # Given
        pg_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

        # When
        columns = ReadPostgresIO(source_config=pg_cloud_config)._get_table_columns(ERModel)  # pylint: disable=protected-access
        # Then
        assert columns == expected_columns

    @pytest.mark.unit
    @patch.object(WithPostgres, "_read_from_postgres")
    def test_read_from_postgres_is_called_for_loading_a_table_with_columns_with_env_as_cloud_and_type_as_postgres(self, mock__read_from_postgres, test_df):
        # Given
        mock__read_from_postgres.return_value = test_df
        postgres_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

        # When
        ReadPostgresIO(source_config=postgres_cloud_config).read()

        # Then
        mock__read_from_postgres.assert_called()

    @pytest.mark.unit
    @patch.object(WithPostgres, "_write_to_postgres")
    def test_write_to_postgres_is_called_for_uploading_a_table_with_columns_with_env_as_cloud_and_type_as_postgres(self, mock__write_to_postgres, test_df):
        # Given
        df = test_df
        postgres_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_PG_PARQUET")

        # When
        WritePostgresIO(source_config=postgres_cloud_config).write(df)

        # Then
        mock__write_to_postgres.assert_called()

    @pytest.mark.unit
    @patch.object(WithPostgres, "_write_to_postgres")
    def test_write_to_postgres_is_called_with_truncate_and_append_option(self, mock__write_to_postgres, test_df):
        # Given
        df = test_df
        postgres_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(
            source_key="WRITE_TO_PG_PARQUET",
        )

        # When
        write_config = WritePostgresIO(source_config=postgres_cloud_config, truncate_and_append=True)

        write_config.write(df)

        # Then
        mock__write_to_postgres.assert_called_once()
        (called_with_df,) = mock__write_to_postgres.call_args[0]
        pd.testing.assert_frame_equal(test_df, called_with_df)
        assert "truncate_and_append" in write_config.options

    @pytest.mark.unit
    @patch.object(WithPostgres, "_read_from_postgres")
    def test_read_from_postgres_by_implicitly_generating_datamodel_from_schema(self, mock__read_from_postgres, test_df):
        # Given
        mock__read_from_postgres.return_value = test_df
        postgres_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

        # When / Then
        ReadPostgresIO(source_config=postgres_cloud_config).read()
        mock__read_from_postgres.assert_called()

    @pytest.mark.unit
    @patch.object(WithPostgres, "_read_database")
    def test_read_from_postgres_with_query(self, mock__read_database):
        # Given
        postgres_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

        # When
        ReadPostgresIO(source_config=postgres_cloud_config, sql_query="SELECT * FROM example").read()

        # Then
        mock__read_database.assert_called_with(ANY, "SELECT * FROM example")

    @pytest.mark.unit
    @patch.object(WithPostgres, "_read_database")
    def test_read_from_postgres_with_query_in_options(self, mock__read_database):
        # Given
        postgres_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES_WITH_QUERY_IN_OPTIONS")

        # When
        ReadPostgresIO(source_config=postgres_cloud_config).read()

        # Then
        mock__read_database.assert_called_with(ANY, "SELECT * FROM table_name_from_yaml_options")

    @pytest.mark.unit
    @patch.object(pd, "read_sql")
    def test_read_from_postgres_with_query_and_options(self, mock__read_sql):
        # Given
        postgres_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

        # When
        ReadPostgresIO(source_config=postgres_cloud_config, sql_query="SELECT * FROM example", parse_dates=["date"], wrong_arg="whatever").read()

        # Then
        mock__read_sql.assert_called_with(sql="SELECT * FROM example", con=ANY, parse_dates=["date"])

    @pytest.mark.unit
    def test_generate_model_from_schema_returns_model(self):
        # Given
        postgres_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

        # When
        schema = postgres_cloud_config.dynamicio_schema
        schema_name = postgres_cloud_config.dynamicio_schema.name
        model = ReadPostgresIO(source_config=postgres_cloud_config)._generate_model_from_schema(schema)

        # Then
        assert len(model.__table__.columns) == len(schema.columns) and model.__tablename__ == schema_name

    @pytest.mark.unit
    def test_get_table_columns_from_generated_model_returns_valid_list_of_columns(self):
        # Given
        pg_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

        # When
        schema = pg_cloud_config.dynamicio_schema
        model = ReadPostgresIO(source_config=pg_cloud_config)._generate_model_from_schema(schema)  # pylint: disable=protected-access
        columns = ReadPostgresIO(source_config=pg_cloud_config)._get_table_columns(model)  # pylint: disable=protected-access

        # Then
        assert isinstance(model.__table__.columns, ImmutableColumnCollection)
        for x, y in zip(columns, [PgModel.id, PgModel.foo, PgModel.bar, PgModel.baz]):
            assert str(x) == str(y)

    @pytest.mark.unit
    def test_to_check_if_dataframe_has_valid_data_types(self):
        # Given
        postgres_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_PG_PARQUET")

        df = pd.DataFrame.from_records(
            [
                ["cm_1", "id_1", 1000, "12/12/2000", True, 12.76],
                ["cm_2", "id_2", 1000, "01/02/1990", False, 199.76],
                ["cm_3", "id_3", 1000, "01/05/1990", False, 12.76],
            ],
            columns=["id", "foo", "bar", "start_date", "active", "net"],
        )

        # When
        is_valid = WriteExtendedPostgresIO(source_config=postgres_cloud_config, show_casting_warnings=True)._has_valid_dtypes(df)

        # Then
        assert is_valid is True
