# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
from contextlib import closing
from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pandera.pandas as pa
import pytest
from pandera.typing import Series
from sqlalchemy.exc import ProgrammingError

# Application Imports
from dynamicio.engines import EngineSetting
from dynamicio.errors import ColumnsDataTypeError
from dynamicio.mixins.with_postgres import WithPostgres
from dynamicio.schema import Field

pl = pytest.importorskip("polars")
DataModel = pytest.importorskip("dynamicio.schema.polars").DataModel
polars_files = pytest.importorskip("tests.test_engines.test_polars_files")
PolarsPostgres = pytest.importorskip("dynamicio.engines.polars_postgres").PolarsPostgres
FilesIO = polars_files.FilesIO

pytestmark = [pytest.mark.integration, pytest.mark.floci]


class Rows(DataModel, dataset="pg_rows", table="pg"):
    id: int
    price: float = Field(nullable=True)
    name: str = Field(nullable=True)
    flag: bool
    created_at: pl.Datetime = Field(nullable=True)
    n: int = Field(nullable=True, pandas_dtype="Int64")


class PandasRows(pa.DataFrameModel):
    __dataset_name__ = "pg_rows"
    __table_name__ = "pg"

    id: Series[int]
    price: Series[float] = Field(nullable=True)
    name: Series[str] = Field(nullable=True)
    flag: Series[bool]
    created_at: Series[pd.Timestamp] = Field(nullable=True)
    n: Series[pd.Int64Dtype] = Field(nullable=True)


@pytest.fixture(autouse=True)
def default_engine():
    EngineSetting.reset()
    yield
    EngineSetting.reset()


class Tables:
    SCHEMA = pl.Schema({"id": pl.Int64, "price": pl.Float64, "name": pl.String, "flag": pl.Boolean, "created_at": pl.Datetime("us"), "n": pl.Int64})

    @staticmethod
    def frame(rows: int = 3) -> "pl.DataFrame":
        """Rows of each type of the table, with nulls. The times are in microseconds, the unit of a Postgres ``timestamp``."""
        return pl.DataFrame(
            {
                "id": list(range(1, rows + 1)),
                "price": [None if i % 3 == 1 else i + 0.5 for i in range(rows)],
                "name": [None if i % 3 == 1 else f"n{i}" for i in range(rows)],
                "flag": [i % 2 == 0 for i in range(rows)],
                "created_at": [None if i % 3 == 2 else datetime(2024, 1, 1, 1, 2, 3, i) for i in range(rows)],
                "n": [None if i % 3 == 0 else i for i in range(rows)],
            },
            schema=Tables.SCHEMA,
        )

    @staticmethod
    def pandas_frame(frame) -> pd.DataFrame:
        """The pandas frame of a polars frame, with the dtypes of ``PandasRows``."""
        return frame.to_pandas().astype({"n": "Int64", "created_at": "datetime64[ns]"})

    @staticmethod
    def polars(config, **options):
        EngineSetting.reset("polars")
        return FilesIO(source_config=config, schema=Rows, **options)

    @staticmethod
    def pandas(config, **options):
        EngineSetting.reset("pandas")
        return FilesIO(source_config=config, schema=PandasRows, **options)

    @staticmethod
    def rows(floci, sql="SELECT * FROM pg ORDER BY id"):
        with closing(floci.pg_connection()) as connection, connection.cursor() as cursor:
            cursor.execute(sql)
            return cursor.fetchall()


class TestPolarsPostgresWrite:
    def test_a_written_table_has_the_rows_of_the_pandas_engine(self, floci, pg_config):
        # Given
        frame = Tables.frame(7)

        # When
        Tables.pandas(pg_config, create_table=True).write(Tables.pandas_frame(frame))
        pandas_rows = Tables.rows(floci)
        Tables.polars(pg_config).write(frame)

        # Then
        assert Tables.rows(floci) == pandas_rows
        assert len(pandas_rows) == 7

    def test_a_written_table_reads_back_the_same_frame_on_both_engines(self, pg_config):
        # Given
        frame = Tables.frame(7)

        # When
        Tables.polars(pg_config, create_table=True).write(frame)

        # Then
        assert Tables.polars(pg_config).read().sort("id").equals(frame)
        pd.testing.assert_frame_equal(Tables.pandas(pg_config).read().sort_values("id").reset_index(drop=True), Tables.pandas_frame(frame))

    def test_an_empty_string_and_a_nan_are_written_as_null(self, floci, pg_config):
        # Given the NULL rule of the COPY: an empty field is NULL
        frame = Tables.frame(1).with_columns(name=pl.lit(""), price=pl.lit(float("nan")))

        # When
        Tables.polars(pg_config, create_table=True).write(frame)

        # Then
        assert Tables.rows(floci, "SELECT name, price FROM pg") == [(None, None)]

    def test_line_breaks_tabs_and_backslashes_in_values_read_back_unchanged(self, pg_config):
        # Given the characters that the text format of COPY escapes, and a value that looks like its default NULL marker
        values = ["cr\rx", "lf\nx", "crlf\r\nx", "tab\tx", "back\\x", "\\N", "end\\", "é"]
        frame = Tables.frame(len(values)).with_columns(name=pl.Series(values))

        # When
        Tables.polars(pg_config, create_table=True).write(frame)

        # Then
        assert Tables.polars(pg_config).read().sort("id")["name"].to_list() == values

    @pytest.mark.parametrize("truncate_and_append", [False, True])
    def test_a_second_write_replaces_the_rows(self, pg_config, truncate_and_append):
        # Given
        Tables.polars(pg_config, create_table=True).write(Tables.frame(5))
        frame = Tables.frame(2)

        # When
        Tables.polars(pg_config, truncate_and_append=truncate_and_append).write(frame)

        # Then
        assert Tables.polars(pg_config).read().sort("id").equals(frame)

    def test_a_frame_without_rows_deletes_the_rows(self, floci, pg_config):
        # Given
        Tables.polars(pg_config, create_table=True).write(Tables.frame(5))

        # When
        Tables.polars(pg_config).write(Tables.frame(0))

        # Then
        assert not Tables.rows(floci)

    def test_each_chunk_is_one_copy_in_one_transaction(self, floci, pg_config):
        # Given
        frame = Tables.frame(10)
        copies = []
        copy = WithPostgres._copy  # pylint: disable=protected-access

        def counted(session, table_name, df):
            copies.append(len(df))
            copy(session, table_name, df)

        # When
        with patch.object(PolarsPostgres, "CHUNK_ROWS", 3), patch.object(WithPostgres, "_copy", side_effect=counted):
            Tables.polars(pg_config, create_table=True).write(frame)

        # Then
        assert copies == [3, 3, 3, 1]
        assert Tables.polars(pg_config).read().sort("id").equals(frame)
        assert len(Tables.rows(floci)) == 10

    def test_a_failed_chunk_rolls_back_the_write_and_keeps_the_old_rows(self, floci, pg_config):
        # Given
        Tables.polars(pg_config, create_table=True).write(Tables.frame(2))
        old = Tables.rows(floci)
        copy = WithPostgres._copy  # pylint: disable=protected-access
        calls = []

        def second_fails(session, table_name, df):
            calls.append(len(df))
            if len(calls) == 2:
                raise RuntimeError("the chunk failed")
            copy(session, table_name, df)

        # When / Then
        with patch.object(PolarsPostgres, "CHUNK_ROWS", 3), patch.object(WithPostgres, "_copy", side_effect=second_fails):
            with pytest.raises(RuntimeError, match="the chunk failed"):
                Tables.polars(pg_config).write(Tables.frame(10))
        assert Tables.rows(floci) == old

    def test_a_large_write_keeps_all_rows_and_nulls(self, floci, pg_config):
        # Given more rows than 2 chunks
        frame = Tables.frame(2 * PolarsPostgres.CHUNK_ROWS + 7)

        # When
        Tables.polars(pg_config, create_table=True).write(frame)

        # Then
        assert Tables.rows(floci, "SELECT count(*), count(name), count(n) FROM pg") == [(frame.height, frame["name"].count(), frame["n"].count())]


class TestPolarsPostgresRead:
    def test_the_read_of_a_pandas_table_is_the_frame_of_the_pandas_engine(self, pg_config):
        # Given
        frame = Tables.frame(7)
        Tables.pandas(pg_config, create_table=True).write(Tables.pandas_frame(frame))

        # When
        result = Tables.polars(pg_config).read().sort("id")

        # Then
        assert result.equals(frame)
        assert result.schema == Tables.SCHEMA
        pd.testing.assert_frame_equal(Tables.pandas_frame(result), Tables.pandas(pg_config).read().sort_values("id").reset_index(drop=True))

    def test_a_sql_query_with_params_in_the_style_of_the_driver_gives_the_rows_of_the_pandas_engine(self, pg_config):
        # Given
        Tables.polars(pg_config, create_table=True).write(Tables.frame(7))
        options = {"sql_query": "SELECT * FROM pg WHERE id > %(low)s ORDER BY id", "params": {"low": 4}}

        # When
        result = Tables.polars(pg_config, **options).read()
        expected = Tables.pandas(pg_config, **options).read()

        # Then
        assert result["id"].to_list() == expected["id"].tolist() == [5, 6, 7]

    def test_a_table_without_rows_gives_a_frame_without_rows_and_with_the_model_types(self, pg_config):
        # Given
        Tables.polars(pg_config, create_table=True).write(Tables.frame(0))

        # When
        result = Tables.polars(pg_config).read()

        # Then
        assert result.height == 0
        assert result.schema == Tables.SCHEMA

    def test_a_null_only_column_casts_to_the_model_type(self, pg_config):
        # Given
        Tables.polars(pg_config, create_table=True).write(Tables.frame(2).with_columns(price=pl.lit(None, pl.Float64)))

        # When
        result = Tables.polars(pg_config).read()

        # Then
        assert result.schema["price"] == pl.Float64 and result["price"].null_count() == 2

    def test_a_missing_table_raises_the_error_and_names_the_model(self, pg_config, caplog):
        # When / Then
        with pytest.raises(ProgrammingError):
            Tables.polars(pg_config).read()
        assert "'pg' does not exist: it is the `__table_name__` of the model of dataset 'pg_rows'" in caplog.text

    def test_a_column_of_another_type_raises_columns_data_type_error(self, floci, pg_config):
        # Given
        with closing(floci.pg_connection()) as connection:
            with connection, connection.cursor() as cursor:
                cursor.execute("CREATE TABLE pg (id text, price float, name text, flag boolean, created_at timestamp, n bigint)")
                cursor.execute("INSERT INTO pg VALUES ('x', 1, 'a', true, NULL, NULL)")

        # When / Then
        with pytest.raises(ColumnsDataTypeError):
            Tables.polars(pg_config).read()
