# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from sqlalchemy import text

# Application Imports
from dynamicio.engines import EngineSetting
from dynamicio.errors import EngineResourceError
from dynamicio.mixins.with_postgres import WithPostgres
from tests.integration.resources import Resources

pl = pytest.importorskip("polars")
PolarsPostgres = pytest.importorskip("dynamicio.engines.polars_postgres").PolarsPostgres
postgres = pytest.importorskip("tests.integration.test_polars_postgres")
FilesIO, Rows, Tables = postgres.FilesIO, postgres.Rows, postgres.Tables


@pytest.fixture(autouse=True)
def default_engine():
    EngineSetting.reset()
    yield
    EngineSetting.reset()


class Stack:
    db_host, db_port, db_name, db_user, db_password = "localhost", 5432, "db", "user", "password"


class TestCheck:
    @pytest.mark.unit
    @pytest.mark.parametrize("option", ["parse_dates", "coerce_float", "dtype", "index_col", "chunksize", "columns", "dtype_backend"])
    def test_a_pandas_reader_option_raises_engine_resource_error_when_the_resource_is_built(self, tmp_path, option):
        # When / Then
        with pytest.raises(EngineResourceError, match=f"no Postgres option '{option}'"):
            Tables.polars(Resources.postgres(tmp_path, Stack), **{option: None})

    @pytest.mark.unit
    def test_the_options_of_both_engines_are_accepted(self, tmp_path):
        # When
        io = Tables.polars(Resources.postgres(tmp_path, Stack), sql_query="SELECT 1", params={"a": 1}, truncate_and_append=True, create_table=True)

        # Then
        assert io.options["sql_query"] == "SELECT 1"

    @pytest.mark.unit
    def test_the_pandas_engine_keeps_its_reader_options(self, tmp_path):
        # When
        io = Tables.pandas(Resources.postgres(tmp_path, Stack), parse_dates=["created_at"])

        # Then
        assert io.options["parse_dates"] == ["created_at"]


class TestConsumerPatches:
    @pytest.mark.unit
    def test_a_patch_of_the_session_of_the_pandas_mixin_reaches_the_polars_read(self, tmp_path):
        # Given
        io = Tables.polars(Resources.postgres(tmp_path, Stack), sql_query="SELECT id FROM pg")

        # When
        with patch("dynamicio.mixins.with_postgres.session_for") as session_for, patch.object(PolarsPostgres, "execute") as execute:
            execute.return_value.keys.return_value = ["id"]
            execute.return_value.fetchall.return_value = [(1,), (2,)]
            frame = PolarsPostgres.read(io)

        # Then
        session_for.assert_called_once()
        assert frame["id"].to_list() == [1, 2]


class TestChunks:
    @pytest.mark.unit
    def test_the_chunks_cover_the_rows_in_order_with_the_pandas_dtypes_of_the_model(self, tmp_path):
        # Given
        io = Tables.polars(Resources.postgres(tmp_path, Stack))
        frame = Tables.frame(7)

        # When
        with patch.object(PolarsPostgres, "CHUNK_ROWS", 3):
            chunks = list(PolarsPostgres.chunks(io, frame))

        # Then
        assert [len(chunk) for chunk in chunks] == [3, 3, 1]
        assert all(isinstance(chunk.index, pd.RangeIndex) for chunk in chunks)
        assert {str(dtype) for dtype in chunks[0].dtypes} >= {"Int64", "datetime64[us]", "bool"}
        pd.testing.assert_frame_equal(pd.concat(chunks, ignore_index=True), Tables.pandas_frame(frame).astype({"created_at": "datetime64[us]", "name": "object"}))

    @pytest.mark.unit
    def test_a_frame_without_rows_gives_one_chunk_with_the_columns(self, tmp_path):
        # Given
        io = Tables.polars(Resources.postgres(tmp_path, Stack))

        # When
        chunks = list(PolarsPostgres.chunks(io, Tables.frame(0)))

        # Then
        assert len(chunks) == 1 and chunks[0].empty
        assert list(chunks[0].columns) == list(Tables.SCHEMA)

    @pytest.mark.unit
    def test_the_chunks_are_made_one_at_a_time(self, tmp_path):
        # Given
        io = Tables.polars(Resources.postgres(tmp_path, Stack))

        # When
        with patch.object(PolarsPostgres, "CHUNK_ROWS", 3):
            chunks = PolarsPostgres.chunks(io, Tables.frame(7))
            first = next(chunks)

        # Then
        assert not isinstance(chunks, (list, tuple))
        assert len(first) == 3

    @pytest.mark.unit
    def test_the_write_gives_the_chunks_to_the_pandas_writer(self, tmp_path):
        # Given
        io = Tables.polars(Resources.postgres(tmp_path, Stack))

        # When
        with patch.object(PolarsPostgres, "CHUNK_ROWS", 2), patch.object(WithPostgres, "_write_model") as write_model:
            io.write(Tables.frame(5))
            (connection_string, _, chunks), options = write_model.call_args
            sizes = [len(chunk) for chunk in chunks]

        # Then
        assert connection_string == "postgresql://user:password@localhost:5432/db"
        assert sizes == [2, 2, 1]
        assert not options


class TestExecute:
    @pytest.mark.unit
    @pytest.mark.parametrize("params, args", [(None, ()), ({"a": 1}, ({"a": 1},)), ([1, 2], ([1, 2],))])
    def test_a_string_goes_to_the_driver_as_in_pandas(self, params, args):
        # Given
        connection = MagicMock()

        # When
        PolarsPostgres.execute(connection, "SELECT %(a)s", params)

        # Then
        connection.exec_driver_sql.assert_called_once_with("SELECT %(a)s", *args)
        connection.execute.assert_not_called()

    @pytest.mark.unit
    def test_a_text_clause_goes_to_sqlalchemy(self):
        # Given
        connection, statement = MagicMock(), text("SELECT 1")

        # When
        PolarsPostgres.execute(connection, statement, None)

        # Then
        connection.execute.assert_called_once_with(statement)
        connection.exec_driver_sql.assert_not_called()


class TestTypeLookup:
    @pytest.mark.unit
    @pytest.mark.parametrize("unit", ["ns", "us", "ms"])
    def test_a_datetime_column_of_each_unit_is_a_timestamp_column(self, unit):
        # Given
        from dynamicio.mixins import with_postgres  # pylint: disable=import-outside-toplevel

        # When / Then
        assert with_postgres._type_lookup[f"datetime64[{unit}]"].__name__ == "DateTime"  # pylint: disable=protected-access
