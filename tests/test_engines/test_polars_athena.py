# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import os
from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pandera.pandas as pa
import pytest
from pandera.typing import Series

# Application Imports
from dynamicio.config import IOConfig
from dynamicio.engines import EngineSetting
from dynamicio.errors import ColumnsDataTypeError
from dynamicio.schema import Field
from tests import constants

pl = pytest.importorskip("polars")
DataModel = pytest.importorskip("dynamicio.schema.polars").DataModel
FilesIO = pytest.importorskip("tests.test_engines.test_polars_files").FilesIO


class Rows(DataModel, dataset="athena_rows"):
    id: int
    price: float = Field(nullable=True)
    name: str = Field(nullable=True)
    flag: bool = Field(nullable=True)
    n: int = Field(nullable=True, pandas_dtype="Int64")
    at: pl.Datetime = Field(nullable=True)


class PandasRows(pa.DataFrameModel):
    __dataset_name__ = "athena_rows"

    id: Series[int]
    price: Series[float] = Field(nullable=True)
    name: Series[str] = Field(nullable=True)
    flag: Series[pd.BooleanDtype] = Field(nullable=True)
    n: Series[pd.Int64Dtype] = Field(nullable=True)
    at: Series[pd.Timestamp] = Field(nullable=True)


@pytest.fixture(autouse=True)
def default_engine():
    EngineSetting.reset()
    yield
    EngineSetting.reset()


class Queries:
    @staticmethod
    def config():
        return IOConfig(path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"), env_identifier="CLOUD", dynamic_vars=constants).get(
            source_key="READ_FROM_ATHENA"
        )

    @staticmethod
    def result(rows: int = 3) -> pd.DataFrame:
        """A result with the pandas nullable dtypes that awswrangler gives."""
        return pd.DataFrame(
            {
                "id": pd.array(range(rows), dtype="Int64"),
                "price": pd.array([None if i == 1 else i + 0.5 for i in range(rows)], dtype="Float64"),
                "name": pd.array([None if i == 2 else f"n{i}" for i in range(rows)], dtype="string"),
                "flag": pd.array([None if i == 0 else i % 2 == 0 for i in range(rows)], dtype="boolean"),
                "n": pd.array([None if i == 0 else i * 10**15 + 1 for i in range(rows)], dtype="Int64"),
                "at": pd.to_datetime([None if i == 1 else datetime(2024, 1, 1, 0, 0, i) for i in range(rows)]),
            }
        )

    @staticmethod
    def read(engine: str, model, result: pd.DataFrame, **options):
        """The frame of a read, and the arguments of the awswrangler query."""
        EngineSetting.reset(engine)
        with patch("dynamicio.mixins.with_athena.wr.athena.read_sql_query", return_value=result) as query:
            frame = FilesIO(source_config=Queries.config(), schema=model, query="SELECT * FROM t", **options).read()
        return frame, query.call_args


class TestPolarsAthenaRead:
    @pytest.mark.unit
    def test_a_read_gives_a_polars_frame_with_the_model_types(self):
        # When
        frame, _ = Queries.read("polars", Rows, Queries.result())

        # Then
        assert isinstance(frame, pl.DataFrame)
        assert frame.schema == pl.Schema({"id": pl.Int64, "price": pl.Float64, "name": pl.String, "flag": pl.Boolean, "n": pl.Int64, "at": pl.Datetime("us")})
        assert frame["n"].to_list() == [None, 10**15 + 1, 2 * 10**15 + 1]
        assert frame["price"].to_list() == [0.5, None, 2.5] and frame["name"].to_list() == ["n0", "n1", None]

    @pytest.mark.unit
    def test_a_read_has_the_values_of_the_pandas_engine(self):
        # When
        polars, _ = Queries.read("polars", Rows, Queries.result())
        pandas, _ = Queries.read("pandas", PandasRows, Queries.result())

        # Then
        assert polars.to_dicts() == pl.from_pandas(pandas).to_dicts()

    @pytest.mark.unit
    def test_the_query_and_the_options_go_to_awswrangler_as_on_the_pandas_engine(self):
        # When
        _, polars = Queries.read("polars", Rows, Queries.result(), ctas_approach=True, workgroup="wg")
        _, pandas = Queries.read("pandas", PandasRows, Queries.result(), ctas_approach=True, workgroup="wg")

        # Then
        assert polars == pandas
        assert polars.kwargs["ctas_approach"] is True and polars.args == ("SELECT * FROM t",)

    @pytest.mark.unit
    def test_a_result_without_rows_gives_a_frame_with_the_model_types(self):
        # When
        frame, _ = Queries.read("polars", Rows, Queries.result(0))

        # Then
        assert frame.height == 0 and frame.schema["n"] == pl.Int64 and frame.schema["name"] == pl.String

    @pytest.mark.unit
    def test_a_column_of_another_type_raises_columns_data_type_error(self):
        # Given
        result = Queries.result().assign(id=["a", "b", "c"])

        # When / Then
        with pytest.raises(ColumnsDataTypeError):
            Queries.read("polars", Rows, result)

    @pytest.mark.unit
    def test_a_chunksize_raises_the_value_error_of_the_pandas_engine(self):
        # When / Then
        with pytest.raises(ValueError, match="'chunksize' option is not supported"):
            Queries.read("polars", Rows, Queries.result(), chunksize=10)


class TestPolarsAthenaWrite:
    @pytest.mark.unit
    def test_a_write_calls_only_the_athena_writer_of_the_io(self):
        # Given
        EngineSetting.reset("polars")
        io = FilesIO(source_config=Queries.config(), schema=Rows)
        frame = pl.DataFrame({"id": [1], "price": [0.5], "name": ["a"], "flag": [True], "n": [1], "at": [datetime(2024, 1, 1)]})

        # When
        with patch.object(FilesIO, "_write_to_athena") as athena, patch("dynamicio.engines.polars_files.PolarsFiles.write") as files:
            io.write(frame)

        # Then
        athena.assert_called_once()
        files.assert_not_called()
