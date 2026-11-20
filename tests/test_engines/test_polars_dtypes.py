# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
from typing import Annotated

import pandas as pd
import pandera.pandas as pa
import pytest
from pandera.typing import Series

from dynamicio.schema import Field
from dynamicio.schema.metadata import PANDAS_DTYPE_KEY
from dynamicio.schema.models import to_dataframe_schema

pl = pytest.importorskip("polars")
pa_polars = pytest.importorskip("pandera.polars")
DataModel = pytest.importorskip("dynamicio.schema.polars").DataModel
DtypeMap = pytest.importorskip("dynamicio.schema.polars_dtypes").DtypeMap


class TestPolarsOf:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "pandas_dtype, polars_dtype",
        [
            ("int8", pl.Int8()),
            ("Int16", pl.Int16()),
            ("int32", pl.Int32()),
            ("int64", pl.Int64()),
            ("Int64", pl.Int64()),
            ("uint8", pl.UInt8()),
            ("UInt64", pl.UInt64()),
            ("float32", pl.Float32()),
            ("float64", pl.Float64()),
            ("bool", pl.Boolean()),
            ("boolean", pl.Boolean()),
            ("object", pl.String()),
            ("str", pl.String()),
            ("string", pl.String()),
            ("string[pyarrow]", pl.String()),
            ("category", pl.Categorical()),
            ("datetime64[ns]", pl.Datetime("ns")),
            ("datetime64[ms, UTC]", pl.Datetime("ms", "UTC")),
            ("datetime64[us,Europe/London]", pl.Datetime("us", "Europe/London")),
            ("timedelta64[us]", pl.Duration("us")),
            (" int64 ", pl.Int64()),
        ],
    )
    def test_a_pandas_dtype_of_the_map_gives_its_polars_dtype(self, pandas_dtype, polars_dtype):
        assert DtypeMap.polars_of(pandas_dtype) == polars_dtype

    @pytest.mark.unit
    def test_a_time_zone_is_kept(self):
        assert DtypeMap.polars_of("datetime64[ns, UTC]").time_zone == "UTC"

    @pytest.mark.unit
    @pytest.mark.parametrize("pandas_dtype", ["datetime64[s]", "timedelta64[s]", "timedelta64[ns, UTC]", "complex128", "Float64", "period[D]", "interval", "", "datetime64"])
    def test_a_pandas_dtype_without_a_pair_is_refused(self, pandas_dtype):
        with pytest.raises(ValueError, match="has no polars dtype in the dtype map of dynamicio"):
            DtypeMap.polars_of(pandas_dtype)


class TestPandasOf:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "polars_dtype, has_nulls, pandas_dtype",
        [
            (pl.Int8(), False, "int8"),
            (pl.Int8(), True, "Int8"),
            (pl.Int64(), False, "int64"),
            (pl.Int64(), True, "Int64"),
            (pl.UInt16(), False, "uint16"),
            (pl.UInt16(), True, "UInt16"),
            (pl.UInt64(), True, "UInt64"),
            (pl.Float32(), True, "float32"),
            (pl.Float64(), False, "float64"),
            (pl.Boolean(), False, "bool"),
            (pl.Boolean(), True, "boolean"),
            (pl.String(), True, "object"),
            (pl.Categorical(), False, "category"),
            (pl.Enum(["a", "b"]), False, "category"),
            (pl.Datetime("ns"), False, "datetime64[ns]"),
            (pl.Datetime("ms", "UTC"), False, "datetime64[ms, UTC]"),
            (pl.Duration("us"), False, "timedelta64[us]"),
            (pl.List(pl.String), True, "object"),
            (pl.List(pl.Int64), False, "object"),
            (pl.List(pl.Datetime("us", "UTC")), False, "object"),
        ],
    )
    def test_a_polars_dtype_of_the_map_gives_its_pandas_dtype(self, polars_dtype, has_nulls, pandas_dtype):
        assert DtypeMap.pandas_of(polars_dtype, has_nulls=has_nulls) == pandas_dtype

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "polars_dtype",
        [
            pl.Date(),
            pl.Time(),
            pl.List(pl.Date),
            pl.List(pl.Categorical),
            pl.List(pl.Enum(["a"])),
            pl.List(pl.List(pl.Int64)),
            pl.Decimal(10, 2),
            pl.Struct({"a": pl.Int64}),
            pl.Binary(),
            pl.Null(),
            pl.Array(pl.Int8, 2),
        ],
    )
    def test_a_polars_dtype_without_a_pair_is_refused(self, polars_dtype):
        with pytest.raises(ValueError, match="has no pandas dtype in the dtype map of dynamicio"):
            DtypeMap.pandas_of(polars_dtype)

    @pytest.mark.unit
    @pytest.mark.parametrize("polars_dtype", [pl.Int8(), pl.UInt32(), pl.Float64(), pl.Boolean(), pl.String(), pl.Datetime("us", "UTC"), pl.Duration("ms")])
    def test_the_reverse_map_goes_back_to_the_same_polars_dtype(self, polars_dtype):
        assert DtypeMap.polars_of(DtypeMap.pandas_of(polars_dtype)) == polars_dtype
        assert DtypeMap.polars_of(DtypeMap.pandas_of(polars_dtype, has_nulls=True)) == polars_dtype

    @pytest.mark.unit
    @pytest.mark.parametrize("pandas_dtype", ["int64", "Int64", "uint8", "float32", "bool", "boolean", "datetime64[ns]", "datetime64[ms, UTC]", "timedelta64[us]"])
    def test_the_map_agrees_with_pandas_on_the_dtype_of_a_polars_frame(self, pandas_dtype):
        # Given a pandas column of the dtype, through polars and back with the pyarrow types of pandas
        series = pd.Series([], dtype=pandas_dtype)

        # Then the polars dtype of the column is the dtype of the map
        assert pl.from_pandas(series).dtype == DtypeMap.polars_of(pandas_dtype)


class TestPairs:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "pandas_dtype, polars_dtype, expected",
        [
            ("Int64", pl.Int64(), True),
            ("int32", pl.Int64(), False),
            ("category", pl.Enum(["a"]), True),
            ("category", pl.Categorical(), True),
            ("object", pl.Categorical(), False),
            ("datetime64[ns, UTC]", pl.Datetime("ns", "UTC"), True),
            ("datetime64[ns]", pl.Datetime("ns", "UTC"), False),
            ("datetime64[ns, UTC]", pl.Datetime("ns"), False),
            ("datetime64[ns, UTC]", pl.Datetime("ns", "Europe/London"), False),
            ("datetime64[ms]", pl.Datetime("ns"), False),
            ("datetime64[s]", pl.Datetime("ms"), False),
            ("not a dtype", pl.String(), False),
            ("object", pl.List(pl.String), True),
            ("object", pl.List(pl.Int64), True),
            (" object ", pl.List(pl.Float64), True),
            ("string", pl.List(pl.String), False),
            ("object", pl.List(pl.Categorical), False),
            ("object", pl.List(pl.List(pl.String)), False),
            ("object", pl.Boolean(), True),
            ("boolean", pl.Boolean(), True),
            ("object", pl.Int64(), False),
            ("object", pl.Datetime("ns"), True),
            ("object", pl.Datetime("ms", "UTC"), True),
            ("object", pl.Date(), False),
        ],
    )
    def test_a_pandas_dtype_pairs_with_its_polars_dtype_only(self, pandas_dtype, polars_dtype, expected):
        assert DtypeMap.pairs(pandas_dtype, polars_dtype) is expected


class TestPolarsModelDtypes:
    @pytest.mark.unit
    def test_a_column_without_metadata_takes_the_reverse_map(self):
        class Plain(DataModel, dataset="plain"):
            a: int
            b: float
            c: str
            d: bool
            e: pl.UInt8
            f: Annotated[pl.Datetime, "ms", "UTC"]
            g: Annotated[pl.Enum, ["x", "y"]]
            h: pl.Duration

        columns = to_dataframe_schema(Plain).columns

        assert {name: column.data_type for name, column in columns.items()} == {
            "a": "int64",
            "b": "float64",
            "c": "object",
            "d": "bool",
            "e": "uint8",
            "f": "datetime64[ms, UTC]",
            "g": "category",
            "h": "timedelta64[us]",
        }

    @pytest.mark.unit
    def test_field_pandas_dtype_names_the_pandas_dtype_of_a_column(self):
        class Nullable(DataModel, dataset="nullable"):
            a: int = Field(nullable=True, pandas_dtype="Int64")
            b: str = Field(nullable=True, pandas_dtype="string[pyarrow]")
            c: bool = Field(nullable=True, pandas_dtype="boolean")
            d: Annotated[pl.Enum, ["x"]] = Field(nullable=False, pandas_dtype="category")

        columns = to_dataframe_schema(Nullable).columns

        assert [column.data_type for column in columns.values()] == ["Int64", "string[pyarrow]", "boolean", "category"]

    @pytest.mark.unit
    def test_a_raw_pandera_field_with_the_metadata_key_works_as_field(self):
        class Raw(DataModel, dataset="raw"):
            a: int = pa_polars.Field(nullable=True, metadata={PANDAS_DTYPE_KEY: "Int64"})

        assert to_dataframe_schema(Raw).columns["a"].data_type == "Int64"

    @pytest.mark.unit
    def test_field_keeps_the_dynamicio_metadata_with_the_pandas_dtype(self):
        options = Field(nullable=True, pandas_dtype="Int64", max_null_fraction=0.5)

        assert options.metadata[PANDAS_DTYPE_KEY] == "Int64"
        assert len(options.metadata) == 2

    @pytest.mark.unit
    def test_field_without_dynamicio_options_has_no_metadata(self):
        assert not Field(nullable=True).metadata

    @pytest.mark.unit
    @pytest.mark.parametrize("pandas_dtype", ["int32", "float64", "object", "datetime64[ns]", "not a dtype"])
    def test_a_pandas_dtype_that_is_not_a_pair_of_the_column_is_refused(self, pandas_dtype):
        class Mismatch(DataModel, dataset="mismatch"):
            a: int = Field(nullable=True, pandas_dtype=pandas_dtype)

        with pytest.raises(ValueError, match="Mismatch.a: pandas_dtype=.* is not a pair of the polars dtype Int64"):
            to_dataframe_schema(Mismatch)

    @pytest.mark.unit
    def test_a_pandas_dtype_that_is_not_a_string_is_refused(self):
        class Typed(DataModel, dataset="typed"):
            a: int = pa_polars.Field(nullable=True, metadata={PANDAS_DTYPE_KEY: int})

        with pytest.raises(ValueError, match="Typed.a: pandas_dtype=<class 'int'> is not a pair"):
            to_dataframe_schema(Typed)

    @pytest.mark.unit
    @pytest.mark.parametrize("annotation", [pl.Date, pl.Decimal, pl.Binary, pl.Time])
    def test_a_column_of_a_polars_dtype_without_a_pair_is_refused_at_compile(self, annotation):
        class Unpaired(DataModel, dataset="unpaired"):
            a: annotation  # type: ignore[valid-type]

        with pytest.raises(ValueError, match="Unpaired.a: the polars dtype .* has no pandas dtype .* Declare a dtype of the map"):
            to_dataframe_schema(Unpaired)

    @pytest.mark.unit
    def test_a_pandas_model_with_a_pandas_dtype_is_refused(self):
        class Pandas(pa.DataFrameModel):
            __dataset_name__ = "pandas"

            a: Series[int] = Field(nullable=True, pandas_dtype="Int64")

        with pytest.raises(ValueError, match="Pandas.a: pandas_dtype is for a polars model only"):
            to_dataframe_schema(Pandas)

    @pytest.mark.unit
    def test_of_model_gives_the_polars_dtype_of_each_column(self):
        class Typed(DataModel, dataset="typed"):
            a: int
            f: Annotated[pl.Datetime, "ms", "UTC"]
            g: Annotated[pl.Enum, ["x", "y"]]

        dtypes = DtypeMap.of_model(Typed)

        assert dtypes == {"a": pl.Int64(), "f": pl.Datetime("ms", "UTC"), "g": pl.Enum(["x", "y"])}
        assert dtypes["f"].time_zone == "UTC"
