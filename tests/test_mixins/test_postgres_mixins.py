# pylint: disable=no-member, missing-module-docstring, missing-class-docstring, missing-function-docstring, too-many-public-methods, too-few-public-methods, protected-access, C0103, C0302, R0801
import os
import types
from unittest.mock import ANY, MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.orm import Session
from sqlalchemy.sql.base import ColumnCollection

# Application Imports
from dynamicio import WithPostgres
from dynamicio.config import IOConfig
from tests import constants
from tests.mocking.io import ReadPostgresIO, WriteExtendedPostgresIO, WritePostgresIO
from tests.mocking.models import ERModel, PgModel


class TestPostgresIO:

    @pytest.mark.unit
    def test_write_to_postgres_using_replace_strategy(self):
        # Given
        session = MagicMock()
        connection = session.connection.return_value
        cursor = connection.connection.cursor.return_value
        df = pd.DataFrame({"id": [1], "val": ["a"]})

        # When
        WithPostgres._write_to_database(session, "dummy_table", df, is_truncate_and_append=False)

        # Then
        assert session.execute.call_count == 1
        executed_sql = str(session.execute.call_args[0][0])
        assert "DELETE FROM dummy_table" in executed_sql
        cursor.copy_from.assert_called_once()
        session.commit.assert_called_once()

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
    @patch("dynamicio.mixins.with_postgres.logger")
    @patch.object(pd, "read_sql")
    def test_read_from_postgres_logs_started_downloading_once(self, mock__read_sql, mock__logger):
        # Given
        postgres_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

        # When
        with patch("dynamicio.mixins.with_postgres.session_for"):
            ReadPostgresIO(source_config=postgres_cloud_config).read()

        # Then
        started = [c for c in mock__logger.info.call_args_list if "Started downloading" in c.args[0]]
        assert len(started) == 1
        mock__read_sql.assert_called_once()

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
        assert isinstance(model.__table__.columns, ColumnCollection)
        for x, y in zip(columns, [PgModel.id, PgModel.foo, PgModel.bar, PgModel.baz]):
            assert str(x) == str(y)

    @pytest.mark.unit
    def test_generate_model_from_schema_raises_on_unknown_type(self):
        # Given
        pg_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

        schema = types.SimpleNamespace(
            name="bad_schema",
            columns={
                "bad_col": types.SimpleNamespace(name="bad_col", data_type="not_a_real_type"),
            },
        )

        # When / Then
        with pytest.raises(ValueError, match="Unsupported data_type 'not_a_real_type' for column 'bad_col' in schema 'bad_schema'"):
            ReadPostgresIO(source_config=pg_cloud_config)._generate_model_from_schema(schema)  # pylint: disable=protected-access

    @pytest.mark.unit
    def test_generate_model_sets_primary_key_on_first_column_only(self):
        # Given
        pg_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

        schema = types.SimpleNamespace(
            name="simple",
            columns={
                "id": types.SimpleNamespace(name="id", data_type="int64"),
                "value": types.SimpleNamespace(name="value", data_type="float64"),
            },
        )

        # When
        model = ReadPostgresIO(source_config=pg_cloud_config)._generate_model_from_schema(schema)  # pylint: disable=protected-access

        # Then
        columns = model.__table__.columns
        assert columns["id"].primary_key is True
        assert columns["value"].primary_key is False

    @pytest.mark.unit
    def test_generate_model_classname_generation(self):
        # Given
        pg_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

        schema = types.SimpleNamespace(
            name="ugly_table_name",
            columns={
                "id": types.SimpleNamespace(name="id", data_type="int64"),
            },
        )

        # When
        model = ReadPostgresIO(source_config=pg_cloud_config)._generate_model_from_schema(schema)  # pylint: disable=protected-access

        # Then
        assert model.__name__ == "UglyTableNameModel"

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


class TestCreateTable:
    @staticmethod
    def _model():
        schema = types.SimpleNamespace(
            name="create_if_missing_table",
            columns={
                "item_id": types.SimpleNamespace(name="item_id", data_type="object"),
                "quantity": types.SimpleNamespace(name="quantity", data_type="int64"),
                "is_active": types.SimpleNamespace(name="is_active", data_type="bool"),
            },
        )
        return WithPostgres._generate_model_from_schema(schema)

    @staticmethod
    def _config():
        return IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

    @staticmethod
    def _df():
        return pd.DataFrame({"id": ["a"], "foo": ["b"], "bar": [2000], "baz": ["c"]})

    @pytest.mark.unit
    def test_create_table_creates_the_schema_columns_without_a_primary_key(self):
        # Given
        engine = create_engine("sqlite://")
        session = Session(bind=engine)

        # When
        WithPostgres._create_table(session, self._model())
        session.commit()

        # Then
        inspector = inspect(engine)
        assert [column["name"] for column in inspector.get_columns("create_if_missing_table")] == ["item_id", "quantity", "is_active"]
        assert not inspector.get_pk_constraint("create_if_missing_table")["constrained_columns"]
        assert inspector.get_columns("create_if_missing_table")[0]["type"].length is None
        session.execute(text("INSERT INTO create_if_missing_table VALUES ('a', 1, 1), ('a', 2, 0)"))  # many rows per first column
        session.commit()
        session.close()

    @pytest.mark.unit
    def test_create_table_keeps_an_existing_table_and_its_rows(self):
        # Given
        engine = create_engine("sqlite://")
        session = Session(bind=engine)
        session.execute(text("CREATE TABLE create_if_missing_table (item_id VARCHAR, quantity BIGINT, is_active BOOLEAN, extra VARCHAR)"))
        session.execute(text("INSERT INTO create_if_missing_table VALUES ('a', 1, 1, 'x')"))
        session.commit()

        # When
        WithPostgres._create_table(session, self._model())
        session.commit()

        # Then
        assert [column["name"] for column in inspect(engine).get_columns("create_if_missing_table")] == ["item_id", "quantity", "is_active", "extra"]
        assert session.execute(text("SELECT count(*) FROM create_if_missing_table")).scalar() == 1
        session.close()

    @pytest.mark.unit
    def test_write_creates_the_missing_table_before_writing_when_the_option_is_set(self):
        # Given
        calls = MagicMock()

        # When
        with (
            patch("dynamicio.mixins.with_postgres.session_for") as mock_session_for,
            patch.object(WithPostgres, "_create_table", calls.create),
            patch.object(WithPostgres, "_write_to_database", calls.write),
        ):
            WritePostgresIO(source_config=self._config(), create_table=True).write(self._df())

        # Then
        session = mock_session_for.return_value.__enter__.return_value
        assert [name for name, _, _ in calls.mock_calls] == ["create", "write"]
        assert calls.create.call_args.args[0] is session
        assert calls.create.call_args.args[1].__tablename__ == calls.write.call_args.args[1]

    @pytest.mark.unit
    def test_write_drops_options_that_the_postgres_writer_does_not_list(self):
        # When
        with (
            patch("dynamicio.mixins.with_postgres.session_for"),
            patch.object(WithPostgres, "_create_table") as mock_create,
            patch.object(WithPostgres, "_write_to_database") as mock_write,
            patch("dynamicio.mixins.utils.logger") as mock_logger,
        ):
            WritePostgresIO(source_config=self._config(), truncate_and_append=True, unknown_option=1).write(self._df())

        # Then
        mock_create.assert_not_called()
        assert mock_write.call_args.args[3] is True
        assert "unknown_option" in mock_logger.warning.call_args.args[0]

    @pytest.mark.unit
    @pytest.mark.parametrize("options", [{}, {"create_table": False}])
    def test_write_does_not_create_tables_without_the_option(self, options):
        # When
        with (
            patch("dynamicio.mixins.with_postgres.session_for"),
            patch.object(WithPostgres, "_create_table") as mock_create,
            patch.object(WithPostgres, "_write_to_database") as mock_write,
        ):
            WritePostgresIO(source_config=self._config(), **options).write(self._df())

        # Then
        mock_create.assert_not_called()
        mock_write.assert_called_once()
