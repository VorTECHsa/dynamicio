# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import csv
import json
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Annotated, Any, Dict, Optional

import pandas as pd
import pandera.pandas as pa
import pytest
import yaml
from pandera.typing import Series

import dynamicio
from dynamicio import UnifiedIO
from dynamicio.config import IOConfig
from dynamicio.core import SCHEMA_FROM_ARGUMENT
from dynamicio.engines import EngineSetting
from dynamicio.errors import ColumnsDataTypeError, EngineResourceError, NoObjectsFoundError
from dynamicio.schema import Field

pl = pytest.importorskip("polars")
pq = pytest.importorskip("pyarrow.parquet")
pa_types = pytest.importorskip("pyarrow")
DataModel = pytest.importorskip("dynamicio.schema.polars").DataModel


class Mixed(DataModel, dataset="mixed"):
    id: int
    price: float = Field(nullable=True)
    name: str = Field(nullable=True)
    flag: bool
    created_at: Annotated[pl.Datetime, "ns", "UTC"] = Field(nullable=True)
    n: int = Field(nullable=True, pandas_dtype="Int64")
    label: pl.Categorical = Field(nullable=True)
    wait: Annotated[pl.Duration, "ns"] = Field(nullable=True)


class PandasMixed(pa.DataFrameModel):
    __dataset_name__ = "mixed"

    id: Series[int]
    price: Series[float] = Field(nullable=True)
    name: Series[str] = Field(nullable=True)
    flag: Series[bool]
    created_at: Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = Field(nullable=True)
    n: Series[pd.Int64Dtype] = Field(nullable=True)
    label: Series[pd.CategoricalDtype] = Field(nullable=True)
    wait: Series[pd.Timedelta] = Field(nullable=True)


class Pair(DataModel, dataset="pair"):
    a: int
    b: str = Field(nullable=True)


class PandasPair(pa.DataFrameModel):
    __dataset_name__ = "pair"

    a: Series[int]
    b: Series[str] = Field(nullable=True)


class FilesIO(UnifiedIO):
    schema = SCHEMA_FROM_ARGUMENT


class Files:
    """The resources, frames and engine switches of the file tests. Each resource has its own YAML file: `IOConfig` keeps the definitions of a path."""

    CASES = [
        ("parquet", {}),
        ("csv", {}),
        ("csv", {"index": False}),
        ("json", {"orient": "records"}),
        ("json", {"orient": "records", "lines": True}),
    ]

    @staticmethod
    def config(tmp_path: Path, file_type: str, options: Optional[Dict[str, Any]] = None, source: str = "local", **section: Any) -> Any:
        section = section or {"file_path": str(tmp_path / f"data.{file_type}")}
        environment: Dict[str, Any] = {"type": source, "s3" if source.startswith("s3") else "local": {**section, "file_type": file_type}}
        if options:
            environment["options"] = options
        path = tmp_path / f"definitions_{uuid.uuid4().hex}.yaml"
        path.write_text(yaml.safe_dump({"X": {"LOCAL": environment}}))
        return IOConfig(str(path), env_identifier="LOCAL", dynamic_vars=FilesIO).get(source_key="X")

    @staticmethod
    def frame() -> Any:
        """A frame of each dtype, with nulls, a string that looks like a number, and times in milliseconds: the unit of `DataFrame.to_json`."""
        return pl.DataFrame(
            {
                "id": [1, 2, 3],
                "price": [1.5, None, 3.0],
                "name": ["a", None, "007"],
                "flag": [True, False, True],
                "created_at": pl.Series([datetime(2024, 1, 1, tzinfo=timezone.utc), None, datetime(2024, 6, 1, 1, 2, 3, 456000, tzinfo=timezone.utc)], dtype=pl.Datetime("ns", "UTC")),
                "n": [1, None, 3],
                "label": pl.Series(["x", None, "y"], dtype=pl.Categorical),
                "wait": pl.Series([timedelta(milliseconds=1), None, timedelta(days=1)], dtype=pl.Duration("ns")),
            }
        )

    @staticmethod
    def pandas_frame(config: Any) -> pd.DataFrame:
        """The frame of `frame()` as the pandas engine casts it to `PandasMixed`."""
        io = FilesIO(source_config=config, schema=PandasMixed)
        dtypes = {name: column.data_type for name, column in io.schema.columns.items()}
        return io._apply_schema(Files.frame().to_pandas().astype(dtypes))  # pylint: disable=protected-access

    @staticmethod
    def write_with_pandas(config: Any) -> None:
        EngineSetting.reset("pandas")
        FilesIO(source_config=config, schema=PandasMixed).write(Files.pandas_frame(config))

    @staticmethod
    def read_with_pandas(config: Any) -> pd.DataFrame:
        EngineSetting.reset("pandas")
        return FilesIO(source_config=config, schema=PandasMixed).read()

    @staticmethod
    def polars(config: Any, model: Any = Mixed, **options: Any) -> FilesIO:
        EngineSetting.reset("polars")
        return FilesIO(source_config=config, schema=model, **options)


class TestRoundTrip:
    @pytest.mark.unit
    @pytest.mark.parametrize("file_type, options", Files.CASES)
    def test_a_frame_that_the_polars_engine_writes_reads_back_equal(self, tmp_path, file_type, options):
        # Given
        config = Files.config(tmp_path, file_type, options)

        # When
        Files.polars(config).write(Files.frame())
        result = Files.polars(config).read()

        # Then
        assert result.equals(Files.frame())
        assert result.schema == Files.frame().schema

    @pytest.mark.unit
    @pytest.mark.parametrize("file_type, options", Files.CASES)
    def test_the_bytes_equal_the_bytes_of_the_pandas_engine(self, tmp_path, file_type, options):
        # Given
        config = Files.config(tmp_path, file_type, options)
        path = tmp_path / f"data.{file_type}"

        # When
        Files.polars(config).write(Files.frame())
        polars_bytes = path.read_bytes()
        Files.write_with_pandas(config)

        # Then
        assert polars_bytes == path.read_bytes()

    @pytest.mark.unit
    @pytest.mark.parametrize("file_type, options", Files.CASES[:3])
    def test_the_pandas_engine_reads_the_file_as_its_own_frame(self, tmp_path, file_type, options):
        # Given
        config = Files.config(tmp_path, file_type, options)

        # When
        Files.polars(config).write(Files.frame())
        result = Files.read_with_pandas(config)

        # Then
        pd.testing.assert_frame_equal(result, Files.pandas_frame(config))

    @pytest.mark.unit
    @pytest.mark.parametrize("file_type, options", Files.CASES)
    def test_the_polars_engine_reads_a_file_of_the_pandas_engine(self, tmp_path, file_type, options):
        # Given
        config = Files.config(tmp_path, file_type, options)
        Files.write_with_pandas(config)

        # When
        result = Files.polars(config).read()

        # Then
        assert result.equals(Files.frame())

    @pytest.mark.unit
    @pytest.mark.parametrize("file_type, options", Files.CASES)
    def test_a_frame_without_rows_reads_back_without_rows(self, tmp_path, file_type, options):
        # Given
        config = Files.config(tmp_path, file_type, options)

        # When
        Files.polars(config, Pair).write(pl.DataFrame({"a": pl.Series([], dtype=pl.Int64), "b": pl.Series([], dtype=pl.String)}))
        result = Files.polars(config, Pair).read()

        # Then
        assert result.height == 0
        assert result.schema == pl.Schema({"a": pl.Int64, "b": pl.String})

    @pytest.mark.unit
    @pytest.mark.parametrize("file_type, options", Files.CASES)
    def test_a_lazy_frame_is_written(self, tmp_path, file_type, options):
        # Given
        config = Files.config(tmp_path, file_type, options)

        # When
        Files.polars(config).write(Files.frame().lazy())

        # Then
        assert Files.polars(config).read().equals(Files.frame())


class TestParquetFooter:
    @staticmethod
    def footers(tmp_path: Path, frame: Any, model: Any, pandas_frame: pd.DataFrame, pandas_model: Any, **options: Any):
        config = Files.config(tmp_path, "parquet", options)
        path = tmp_path / "data.parquet"
        Files.polars(config, model).write(frame)
        polars_file = pq.ParquetFile(str(path))
        polars_schema, polars_metadata = polars_file.schema_arrow, polars_file.metadata
        EngineSetting.reset("pandas")
        FilesIO(source_config=config, schema=pandas_model).write(pandas_frame)
        pandas_file = pq.ParquetFile(str(path))
        return (polars_schema, polars_metadata), (pandas_file.schema_arrow, pandas_file.metadata)

    @pytest.mark.unit
    def test_the_footer_has_the_pandas_metadata_types_and_compression_of_the_pandas_engine(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")

        # When
        (polars_schema, polars_metadata), (pandas_schema, pandas_metadata) = self.footers(tmp_path, Files.frame(), Mixed, Files.pandas_frame(config), PandasMixed)

        # Then
        assert polars_schema.equals(pandas_schema, check_metadata=True)
        assert json.loads(polars_schema.metadata[b"pandas"]) == json.loads(pandas_schema.metadata[b"pandas"])
        columns = range(polars_metadata.row_group(0).num_columns)
        assert {polars_metadata.row_group(0).column(i).compression for i in columns} == {"SNAPPY"}
        assert [polars_metadata.row_group(0).column(i).physical_type for i in columns] == [pandas_metadata.row_group(0).column(i).physical_type for i in columns]

    @pytest.mark.unit
    def test_the_metadata_option_is_in_the_footer_of_the_file(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet", {"metadata": {"as_of": "2026-11-24T10:00:00.123456"}})

        # When
        Files.polars(config, Pair).write(pl.DataFrame({"a": [1], "b": ["x"]}))

        # Then
        footer = FilesIO(source_config=config, schema=Pair).footer()
        assert footer.metadata[b"as_of"] == b"2026-11-24T10:00:00.123456"
        assert b"pandas" in footer.metadata
        assert footer.rows == 1

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "metadata, message",
        [
            ({"pandas": "x"}, "cannot set pandas"),
            ({"ARROW:schema": "x"}, "cannot set ARROW:schema"),
            ({"as_of": 1}, "dict of str keys and str values"),
            ("as_of", "dict of str keys and str values"),
        ],
    )
    def test_the_metadata_option_refuses_a_value_that_is_not_valid(self, tmp_path, metadata, message):
        # Given
        config = Files.config(tmp_path, "parquet", {"metadata": metadata})

        # When / Then
        with pytest.raises(EngineResourceError, match=message):
            Files.polars(config, Pair).write(pl.DataFrame({"a": [1], "b": ["x"]}))

    @pytest.mark.unit
    def test_the_compression_option_reaches_the_writer(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet", {"compression": "zstd"})

        # When
        Files.polars(config, Pair).write(pl.DataFrame({"a": [1], "b": ["x"]}))

        # Then
        assert pq.ParquetFile(str(tmp_path / "data.parquet")).metadata.row_group(0).column(0).compression == "ZSTD"

    @pytest.mark.unit
    @pytest.mark.parametrize("count", [1, 127, 128, 40000])
    def test_the_category_codes_have_the_width_that_pandas_gives_them(self, tmp_path, count):
        class Labels(DataModel, dataset="labels"):
            label: pl.Categorical

        class PandasLabels(pa.DataFrameModel):
            __dataset_name__ = "labels"

            label: Series[pd.CategoricalDtype]

        # Given
        values = [f"v{i:05d}" for i in range(count)]

        # When
        (polars_schema, _), (pandas_schema, _) = self.footers(
            tmp_path, pl.DataFrame({"label": pl.Series(values, dtype=pl.Categorical)}), Labels, pd.DataFrame({"label": pd.Categorical(values)}), PandasLabels
        )

        # Then
        assert polars_schema.field("label").type == pandas_schema.field("label").type
        assert json.loads(polars_schema.metadata[b"pandas"])["columns"] == json.loads(pandas_schema.metadata[b"pandas"])["columns"]

    @pytest.mark.unit
    def test_an_enum_column_has_the_number_of_its_categories(self, tmp_path):
        class Sizes(DataModel, dataset="sizes"):
            size: Annotated[pl.Enum, ["s", "m", "l"]]

        # Given
        config = Files.config(tmp_path, "parquet")

        # When
        Files.polars(config, Sizes).write(pl.DataFrame({"size": pl.Series(["s", "s"], dtype=pl.Enum(["s", "m", "l"]))}))

        # Then
        metadata = json.loads(pq.read_schema(str(tmp_path / "data.parquet")).metadata[b"pandas"])
        assert metadata["columns"][0]["metadata"]["num_categories"] == 3
        assert Files.polars(config, Sizes).read()["size"].to_list() == ["s", "s"]

    @pytest.mark.unit
    def test_an_all_null_string_column_has_the_null_type_of_pandas(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")

        # When
        Files.polars(config, Pair).write(pl.DataFrame({"a": [1, 2], "b": pl.Series([None, None], dtype=pl.String)}))

        # Then
        footer = pq.read_schema(str(tmp_path / "data.parquet"))
        expected = pa_types.Schema.from_pandas(pd.DataFrame({"a": [1, 2], "b": pd.Series([None, None], dtype="object")}))
        assert footer.field("b").type == pa_types.null()
        assert footer.equals(expected, check_metadata=True)
        result = Files.polars(config, Pair).read()
        assert result.schema["b"] == pl.String and result["b"].to_list() == [None, None]
        EngineSetting.reset("pandas")
        assert FilesIO(source_config=config, schema=PandasPair).read()["b"].isna().tolist() == [True, True]

    @pytest.mark.unit
    def test_an_int96_timestamp_reads_as_the_datetime_of_the_model(self, tmp_path):
        class Events(DataModel, dataset="events"):
            created_at: Annotated[pl.Datetime, "ns", "UTC"]

        # Given
        config = Files.config(tmp_path, "parquet")
        values = pd.Series([pd.Timestamp("2024-01-01 00:00:00.000000001", tz="UTC"), pd.Timestamp("2024-06-01 12:00:00", tz="UTC")])
        pd.DataFrame({"created_at": values}).to_parquet(tmp_path / "data.parquet", use_deprecated_int96_timestamps=True)

        # When
        result = Files.polars(config, Events).read()

        # Then
        assert result.schema["created_at"] == pl.Datetime("ns", "UTC")
        assert result["created_at"].to_pandas().tolist() == values.tolist()

    @pytest.mark.unit
    def test_the_columns_of_the_model_are_read_in_the_order_of_the_model(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")
        pd.DataFrame({"b": ["x"], "extra": [0], "a": [1]}).to_parquet(tmp_path / "data.parquet")

        # When
        result = Files.polars(config, Pair).read()

        # Then
        assert result.columns == ["a", "b"]
        assert result.row(0) == (1, "x")


class TestCsvRead:
    @pytest.mark.unit
    @pytest.mark.parametrize("null", ["", "NA", "N/A", "NaN", "null", "NULL", "None", "<NA>", "#N/A", "n/a", "nan", "-NaN"])
    def test_the_null_strings_of_pandas_read_as_nulls(self, tmp_path, null):
        # Given
        (tmp_path / "data.csv").write_text(f"a,b\n1,{null}\n2,x\n")
        config = Files.config(tmp_path, "csv")

        # When
        result = Files.polars(config, Pair).read()

        # Then
        assert result["b"].to_list() == [None, "x"]
        EngineSetting.reset("pandas")
        assert FilesIO(source_config=config, schema=PandasPair).read()["b"].isna().tolist() == [True, False]

    @pytest.mark.unit
    def test_a_string_that_looks_like_a_number_keeps_its_text(self, tmp_path):
        # Given
        (tmp_path / "data.csv").write_text("a,b\n1,007\n2,1e3\n")

        # When
        result = Files.polars(Files.config(tmp_path, "csv"), Pair).read()

        # Then
        assert result["b"].to_list() == ["007", "1e3"]

    @pytest.mark.unit
    def test_a_float_on_the_last_row_of_a_float_column_reads_as_a_float(self, tmp_path):
        class Prices(DataModel, dataset="prices"):
            price: float

        # Given
        (tmp_path / "data.csv").write_text("price\n" + "1\n" * 2000 + "2.5\n")

        # When
        result = Files.polars(Files.config(tmp_path, "csv"), Prices).read()

        # Then
        assert result["price"].tail(1).to_list() == [2.5]
        assert result.schema["price"] == pl.Float64

    @pytest.mark.unit
    def test_the_read_options_reach_the_reader(self, tmp_path):
        # Given
        (tmp_path / "data.csv").write_text("# a comment\nskipped\na;b\n1;-\n2;'x;y'\n3;z\n")
        config = Files.config(tmp_path, "csv", {"sep": ";", "na_values": "-", "quotechar": "'", "skiprows": 2, "nrows": 2})

        # When
        result = Files.polars(config, Pair).read()

        # Then
        assert result.to_dicts() == [{"a": 1, "b": None}, {"a": 2, "b": "x;y"}]

    @pytest.mark.unit
    def test_quote_none_reads_the_quotes_as_text_as_pandas_does(self, tmp_path):
        # Given
        (tmp_path / "data.csv").write_text('a,b\n1,"literal"\n')
        config = Files.config(tmp_path, "csv", {"quoting": csv.QUOTE_NONE, "index": False})

        # When
        result = Files.polars(config, Pair).read()

        # Then
        EngineSetting.reset("pandas")
        assert result["b"].to_list() == ['"literal"'] == FilesIO(source_config=config, schema=PandasPair).read()["b"].tolist()

    @pytest.mark.unit
    def test_quote_none_writes_and_reads_back_the_quotes_of_a_value(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "csv", {"quoting": csv.QUOTE_NONE, "index": False})
        Files.polars(config, Pair).write(pl.DataFrame({"a": [1], "b": ['say "hi"']}))

        # When
        result = Files.polars(config, Pair).read()

        # Then
        assert result["b"].to_list() == ['say "hi"']

    @pytest.mark.unit
    def test_the_columns_that_the_model_does_not_declare_are_left_out(self, tmp_path):
        # Given
        (tmp_path / "data.csv").write_text(",b,extra,a\n0,x,9,1\n")

        config = Files.config(tmp_path, "csv")

        # When
        result = Files.polars(config, Pair).read()

        # Then
        EngineSetting.reset("pandas")
        assert result.columns == list(FilesIO(source_config=config, schema=PandasPair).read().columns)
        assert result.select("a", "b").row(0) == (1, "x")


class TestJsonRead:
    @pytest.mark.unit
    @pytest.mark.parametrize("unit, epoch", [("ms", 1704067200123), ("s", 1704067200), ("us", 1704067200123456), ("ns", 1704067200123456789)])
    def test_an_epoch_reads_as_the_utc_instant_in_its_date_unit(self, tmp_path, unit, epoch):
        class Events(DataModel, dataset="events"):
            created_at: Annotated[pl.Datetime, "ns", "Europe/London"]

        # Given
        (tmp_path / "data.json").write_text(json.dumps([{"created_at": epoch}]))
        config = Files.config(tmp_path, "json", {"orient": "records", "date_unit": unit})

        # When
        result = Files.polars(config, Events).read()

        # Then
        expected = pd.Timestamp(epoch, unit=unit, tz="UTC").tz_convert("Europe/London")
        assert result["created_at"].to_pandas().tolist() == [expected]

    @pytest.mark.unit
    def test_an_epoch_of_a_naive_datetime_is_the_utc_wall_time(self, tmp_path):
        class Events(DataModel, dataset="events"):
            created_at: Annotated[pl.Datetime, "ns", None]

        # Given
        (tmp_path / "data.json").write_text(json.dumps([{"created_at": 1704067200000}]))

        # When
        result = Files.polars(Files.config(tmp_path, "json", {"orient": "records"}), Events).read()

        # Then
        assert result["created_at"].to_list() == [datetime(2024, 1, 1)]

    @pytest.mark.unit
    @pytest.mark.parametrize("unit, value, expected", [("ms", 1500, timedelta(seconds=1.5)), ("s", 2, timedelta(seconds=2)), ("ns", 7, pd.Timedelta(7))])
    def test_an_integer_under_a_duration_column_is_a_duration_in_its_date_unit(self, tmp_path, unit, value, expected):
        class Waits(DataModel, dataset="waits"):
            wait: Annotated[pl.Duration, "ns"]

        # Given
        (tmp_path / "data.json").write_text(json.dumps([{"wait": value}]))
        config = Files.config(tmp_path, "json", {"orient": "records", "date_unit": unit})

        # When
        result = Files.polars(config, Waits).read()

        # Then
        assert result["wait"].to_pandas().tolist() == [pd.Timedelta(expected)]

    @pytest.mark.unit
    def test_an_iso_string_under_a_datetime_column_is_parsed(self, tmp_path):
        class Events(DataModel, dataset="events"):
            created_at: Annotated[pl.Datetime, "ns", "UTC"]

        # Given
        (tmp_path / "data.json").write_text("\n".join(json.dumps({"created_at": value}) for value in ["2024-01-01T00:00:00Z", "2024-01-02T00:00:00Z"]))
        config = Files.config(tmp_path, "json", {"orient": "records", "lines": True})

        # When
        result = Files.polars(config, Events).read()

        # Then
        assert result["created_at"].dt.day().to_list() == [1, 2]


class TestLocalBatch:
    @pytest.mark.unit
    @pytest.mark.parametrize("file_type", ["parquet", "csv"])
    def test_the_files_under_a_prefix_read_as_one_frame_in_sorted_order(self, tmp_path, file_type):
        # Given
        (tmp_path / "batch").mkdir()
        for name, rows in (("2", {"a": [3], "b": ["z"]}), ("1", {"a": [1, 2], "b": ["x", None]})):
            getattr(pd.DataFrame(rows), f"to_{file_type}")(tmp_path / "batch" / f"{name}.{file_type}", index=False)
        config = Files.config(tmp_path, file_type, source="local_batch", path_prefix=f"{tmp_path}/batch/")

        # When
        result = Files.polars(config, Pair).read()

        # Then
        assert result.to_dicts() == [{"a": 1, "b": "x"}, {"a": 2, "b": None}, {"a": 3, "b": "z"}]

    @pytest.mark.unit
    def test_a_dynamic_file_path_takes_the_fields_of_its_template(self, tmp_path):
        # Given
        for run in ("a", "b"):
            pd.DataFrame({"a": [1], "b": [run]}).to_csv(tmp_path / f"run_{run}.csv", index=False)
        config = Files.config(tmp_path, "csv", source="local_batch", dynamic_file_path=f"{tmp_path}/run_{{run}}.csv")

        # When
        result = Files.polars(config, Pair, run="b").read()

        # Then
        assert result["b"].to_list() == ["b"]

    @pytest.mark.unit
    def test_a_file_that_misses_a_column_has_nulls_in_it(self, tmp_path):
        # Given
        (tmp_path / "batch").mkdir()
        pd.DataFrame({"a": [1], "b": ["x"]}).to_csv(tmp_path / "batch" / "1.csv", index=False)
        pd.DataFrame({"a": [2]}).to_csv(tmp_path / "batch" / "2.csv", index=False)
        config = Files.config(tmp_path, "csv", source="local_batch", path_prefix=f"{tmp_path}/batch/")

        # When
        result = Files.polars(config, Pair).read()

        # Then
        assert result.to_dicts() == [{"a": 1, "b": "x"}, {"a": 2, "b": None}]

    @pytest.mark.unit
    def test_a_prefix_without_files_raises_no_objects_found(self, tmp_path):
        # Given
        (tmp_path / "batch").mkdir()
        config = Files.config(tmp_path, "csv", source="local_batch", path_prefix=f"{tmp_path}/batch/")

        # When / Then
        with pytest.raises(NoObjectsFoundError):
            Files.polars(config, Pair).read()

    @pytest.mark.unit
    def test_a_prefix_without_files_returns_none_with_missing_ok(self, tmp_path):
        # Given
        (tmp_path / "batch").mkdir()
        config = Files.config(tmp_path, "csv", source="local_batch", path_prefix=f"{tmp_path}/batch/")

        # When / Then
        assert Files.polars(config, Pair, missing_ok=True).read() is None


class TestMissingFile:
    @pytest.mark.unit
    @pytest.mark.parametrize("file_type, options", Files.CASES)
    def test_a_missing_file_raises_file_not_found(self, tmp_path, file_type, options):
        with pytest.raises(FileNotFoundError):
            Files.polars(Files.config(tmp_path, file_type, options)).read()

    @pytest.mark.unit
    @pytest.mark.parametrize("file_type, options", Files.CASES)
    def test_a_missing_file_returns_none_with_missing_ok(self, tmp_path, file_type, options):
        assert Files.polars(Files.config(tmp_path, file_type, options), missing_ok=True).read() is None


class TestResourceCheck:
    @pytest.mark.unit
    def test_a_file_type_without_a_polars_reader_is_refused_when_the_resource_is_built(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "hdf", file_path=str(tmp_path / "data.h5"))

        # When / Then
        with pytest.raises(EngineResourceError, match="not hdf files"):
            Files.polars(config)

    @pytest.mark.unit
    def test_an_option_from_the_code_is_checked(self, tmp_path):
        with pytest.raises(EngineResourceError, match="no csv option dtype"):
            Files.polars(Files.config(tmp_path, "csv"), dtype={"a": str})

    @pytest.mark.unit
    def test_the_fields_of_the_path_template_are_not_options(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "csv", file_path=str(tmp_path / "data_{run}.csv"))

        # When
        Files.polars(config, Pair, run="x").write(pl.DataFrame({"a": [1], "b": ["y"]}))

        # Then
        assert (tmp_path / "data_x.csv").exists()
        assert Files.polars(config, Pair, run="x").read().row(0) == (1, "y")

    @pytest.mark.unit
    def test_an_option_that_changes_after_the_build_is_refused_at_a_read_and_the_engine_stays_unfrozen(self, tmp_path):
        # Given
        io = Files.polars(Files.config(tmp_path, "csv"), Pair)
        io.options["dtype"] = {"a": str}

        # When
        with pytest.raises(EngineResourceError, match="no csv option dtype"):
            io.read()

        # Then
        dynamicio.configure(engine="pandas")
        assert EngineSetting.current().name == "pandas"

    @pytest.mark.unit
    def test_an_option_that_changes_after_the_build_is_refused_before_the_file_is_written(self, tmp_path):
        # Given
        io = Files.polars(Files.config(tmp_path, "json", {"orient": "records"}), Pair)
        io.options["orient"] = "split"

        # When
        with pytest.raises(EngineResourceError, match="not split"):
            io.write(pl.DataFrame({"a": [1], "b": ["x"]}))

        # Then
        assert not (tmp_path / "data.json").exists()

    @staticmethod
    def io_of(tmp_path, source, section, **options):
        (tmp_path / "x.yaml").write_text(yaml.safe_dump({"X": {"LOCAL": {"type": source, source: section}}}))
        return Files.polars(IOConfig(str(tmp_path / "x.yaml"), env_identifier="LOCAL", dynamic_vars=FilesIO).get(source_key="X"), Pair, **options)

    @pytest.mark.unit
    def test_a_read_of_kafka_raises_the_error_of_the_pandas_engine(self, tmp_path):
        # Given
        io = self.io_of(tmp_path, "kafka", {"kafka_server": "localhost:9092", "kafka_topic": "t"}, key_column="a")

        # When / Then
        with pytest.raises(NotImplementedError, match="does not support reading from Kafka"):
            io.read()

    @pytest.mark.unit
    def test_a_write_of_athena_raises_the_error_of_the_pandas_engine(self, tmp_path):
        # Given
        io = self.io_of(tmp_path, "athena", {"s3_output": "s3://bucket/out/"})

        # When / Then
        with pytest.raises(NotImplementedError, match="Athena does not support direct writes"):
            io.write(pl.DataFrame({"a": [1], "b": ["x"]}))


class Nullable(DataModel, dataset="nullable"):
    a: pl.Int64 = Field(nullable=True)
    b: pl.Boolean = Field(nullable=True)
    c: pl.UInt8 = Field(nullable=True)


class Flagged(DataModel, dataset="flagged"):
    flag: pl.Boolean = Field(nullable=True, pandas_dtype="object")


class TestBooleanObjectColumn:
    @pytest.mark.unit
    def test_a_boolean_column_of_the_pandas_dtype_object_has_the_file_of_a_pandas_column_of_bools_and_none(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")
        pd.DataFrame({"flag": pd.Series([True, None, False], dtype="object")}).to_parquet(tmp_path / "pandas.parquet")

        # When
        Files.polars(config, Flagged).write(pl.DataFrame({"flag": [True, None, False]}))

        # Then
        assert pq.read_schema(str(tmp_path / "data.parquet")).equals(pq.read_schema(str(tmp_path / "pandas.parquet")), check_metadata=True)
        assert pd.read_parquet(tmp_path / "data.parquet")["flag"].tolist() == [True, None, False]
        assert Files.polars(config, Flagged).read()["flag"].to_list() == [True, None, False]


class Stamped(DataModel, dataset="stamped"):
    stamp: Annotated[pl.Datetime, "ns", None] = Field(nullable=True, pandas_dtype="object")


class TestDatetimeObjectColumn:
    OPTIONS = {"coerce_timestamps": "ms", "allow_truncated_timestamps": True}

    @pytest.mark.unit
    @pytest.mark.parametrize("values", [[datetime(2026, 1, 1, 12), None], [None, None]])
    @pytest.mark.parametrize("operation", ["write", "sink"])
    def test_a_datetime_column_of_the_pandas_dtype_object_has_the_file_of_a_pandas_column_of_timestamps_and_none(self, tmp_path, values, operation):
        # Given
        config = Files.config(tmp_path, "parquet", self.OPTIONS)
        pd.DataFrame({"stamp": pd.Series([None if value is None else pd.Timestamp(value) for value in values], dtype="object")}).to_parquet(tmp_path / "pandas.parquet", **self.OPTIONS)
        frame = pl.DataFrame({"stamp": values}, schema={"stamp": pl.Datetime("ns")})

        # When
        TestNullsOfANumpyDtype.store(Files.polars(config, Stamped), frame, operation)

        # Then
        assert pq.read_schema(str(tmp_path / "data.parquet")).equals(pq.read_schema(str(tmp_path / "pandas.parquet")), check_metadata=True)
        assert Files.polars(config, Stamped).read()["stamp"].to_list() == values


class TestNullsOfANumpyDtype:
    """A column of a numpy integer or ``bool`` dtype with a null takes the nullable pandas dtype, as `DataFrame.to_parquet` gives it."""

    FRAME = {"a": [1, None, 2**60 + 1], "b": [True, None, False], "c": [1, None, 3]}
    SCHEMA = {"a": pl.Int64, "b": pl.Boolean, "c": pl.UInt8}

    @classmethod
    def frame(cls) -> Any:
        return pl.DataFrame(cls.FRAME, schema=cls.SCHEMA)

    @staticmethod
    def store(io: FilesIO, frame: Any, operation: str) -> None:
        if operation == "write":
            io.write(frame)
        else:
            io.sink(frame.lazy())

    @pytest.mark.unit
    @pytest.mark.parametrize("operation", ["write", "sink"])
    def test_a_parquet_file_reads_in_pandas_with_the_nullable_dtypes_and_the_exact_values(self, tmp_path, operation):
        # Given
        io = Files.polars(Files.config(tmp_path, "parquet"), Nullable)

        # When
        self.store(io, self.frame(), operation)

        # Then
        result = pd.read_parquet(tmp_path / "data.parquet")
        assert result.dtypes.astype(str).to_dict() == {"a": "Int64", "b": "boolean", "c": "UInt8"}
        assert result["a"].tolist() == [1, pd.NA, 2**60 + 1]
        assert Files.polars(Files.config(tmp_path, "parquet"), Nullable).read().equals(self.frame())

    @pytest.mark.unit
    @pytest.mark.parametrize("operation", ["write", "sink"])
    def test_a_frame_without_nulls_keeps_the_numpy_dtypes(self, tmp_path, operation):
        # Given
        frame = pl.DataFrame({"a": [1, 2], "b": [True, False], "c": [1, 3]}, schema=self.SCHEMA)
        io = Files.polars(Files.config(tmp_path, "parquet"), Nullable)

        # When
        self.store(io, frame, operation)

        # Then
        assert pd.read_parquet(tmp_path / "data.parquet").dtypes.astype(str).to_dict() == {"a": "int64", "b": "bool", "c": "uint8"}

    @pytest.mark.unit
    def test_a_pandas_model_of_a_nullable_dtype_reads_the_exact_values_on_the_pandas_engine(self, tmp_path):
        class PandasNullable(pa.DataFrameModel):
            __dataset_name__ = "nullable"

            a: Series[pd.Int64Dtype] = pa.Field(nullable=True)
            b: Series[pd.BooleanDtype] = pa.Field(nullable=True)
            c: Series[pd.UInt8Dtype] = pa.Field(nullable=True)

        # Given
        Files.polars(Files.config(tmp_path, "parquet"), Nullable).write(self.frame())

        # When
        EngineSetting.reset("pandas")
        result = FilesIO(source_config=Files.config(tmp_path, "parquet"), schema=PandasNullable).read()

        # Then
        assert result["a"].tolist() == [1, pd.NA, 2**60 + 1]
        assert result["b"].tolist() == [True, pd.NA, False]

    @pytest.mark.unit
    def test_a_pandas_model_of_a_numpy_dtype_cannot_hold_the_null_on_the_pandas_engine(self, tmp_path):
        class PandasNumpy(pa.DataFrameModel):
            __dataset_name__ = "nullable"

            a: Series[int] = pa.Field(nullable=True)

        # Given a file of the polars engine, and the same column as a pandas engine writes it with a null: a float64 NaN
        Files.polars(Files.config(tmp_path, "parquet"), Nullable).write(self.frame())
        pd.DataFrame({"a": [1.0, None, 3.0]}).to_parquet(tmp_path / "pandas.parquet")
        EngineSetting.reset("pandas")

        # When / Then the pandas engine raises for the two files
        for name in ("data.parquet", "pandas.parquet"):
            config = Files.config(tmp_path, "parquet", file_path=str(tmp_path / name))
            with pytest.raises(ColumnsDataTypeError):
                FilesIO(source_config=config, schema=PandasNumpy).read()

    @pytest.mark.unit
    @pytest.mark.parametrize("file_type, options", [("csv", {"index": False}), ("json", {"orient": "records"})])
    def test_a_text_file_keeps_the_exact_values_and_reads_back_equal(self, tmp_path, file_type, options):
        # Given
        config = Files.config(tmp_path, file_type, options)

        # When
        Files.polars(config, Nullable).write(self.frame())

        # Then
        assert str(2**60 + 1) in (tmp_path / f"data.{file_type}").read_text()
        assert Files.polars(config, Nullable).read().equals(self.frame())


class TestWriteConversion:
    @pytest.mark.unit
    @pytest.mark.parametrize("file_type, options", [("csv", {"index": False}), ("json", {"orient": "records"})])
    def test_a_large_integer_next_to_a_null_keeps_its_value(self, tmp_path, file_type, options):
        class Counts(DataModel, dataset="counts"):
            id: int
            count: int = Field(nullable=True, pandas_dtype="Int64")

        class PandasCounts(pa.DataFrameModel):
            __dataset_name__ = "counts"

            id: Series[int]
            count: Series[pd.Int64Dtype] = pa.Field(nullable=True)

        # Given
        config = Files.config(tmp_path, file_type, options)
        frame = pl.DataFrame({"id": [1, 2], "count": [2**53 + 1, None]})

        # When
        Files.polars(config, Counts).write(frame)

        # Then
        assert Files.polars(config, Counts).read().equals(frame)
        written = (tmp_path / f"data.{file_type}").read_bytes()
        EngineSetting.reset("pandas")
        FilesIO(source_config=config, schema=PandasCounts).write(pd.DataFrame({"id": [1, 2], "count": pd.array([2**53 + 1, None], dtype="Int64")}))
        assert written == (tmp_path / f"data.{file_type}").read_bytes()

    @pytest.mark.unit
    def test_a_nan_in_a_required_float_column_fails_the_write_validation(self, tmp_path):
        class Prices(DataModel, dataset="prices"):
            price: float

        # Given
        frame = pl.DataFrame({"price": [1.5, float("nan")]})

        # When / Then
        with pytest.raises(dynamicio.errors.SchemaValidationError, match="has_no_null_values"):
            Files.polars(Files.config(tmp_path, "parquet"), Prices, apply_schema_validations=True).write(frame)
        assert not (tmp_path / "data.parquet").exists()

    @pytest.mark.unit
    def test_a_parquet_write_without_an_index_has_the_footer_of_the_pandas_engine(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet", {"index": False})

        # When
        (polars_schema, _), (pandas_schema, _) = TestParquetFooter.footers(tmp_path, Files.frame(), Mixed, Files.pandas_frame(config), PandasMixed, index=False)

        # Then
        metadata = json.loads(polars_schema.metadata[b"pandas"])
        assert metadata == json.loads(pandas_schema.metadata[b"pandas"])
        assert metadata["index_columns"] == [] and metadata["column_indexes"] == []


class TestCsvOptions:
    @pytest.mark.unit
    def test_a_decimal_comma_file_reads_back_as_floats(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "csv", {"sep": ";", "decimal": ",", "index": False})
        Files.polars(config).write(Files.frame())

        # When
        result = Files.polars(config).read()

        # Then
        assert "1,5" in (tmp_path / "data.csv").read_text()
        assert result.equals(Files.frame())

    @pytest.mark.unit
    def test_a_headerless_csv_without_names_is_refused_at_the_read(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "csv", {"header": False, "index": False})
        Files.polars(config, Pair).write(pl.DataFrame({"a": [1], "b": ["x"]}))

        # When / Then
        assert (tmp_path / "data.csv").read_text() == "1,x\n"
        with pytest.raises(EngineResourceError, match="without a header line only with the names option"):
            Files.polars(config, Pair).read()
        with pytest.raises(EngineResourceError, match="without a header line only with the names option"):
            Files.polars(config, Pair).scan()

    @pytest.mark.unit
    def test_a_headerless_csv_with_names_reads_as_on_the_pandas_engine(self, tmp_path):
        # Given
        (tmp_path / "data.csv").write_text("1,x\n2,NA\n")
        config = Files.config(tmp_path, "csv", {"header": None, "names": ["a", "b"]})

        # When
        result = Files.polars(config, Pair).read()
        scanned = Files.polars(config, Pair).scan().collect()
        EngineSetting.reset("pandas")
        expected = FilesIO(source_config=config, schema=PandasPair).read()

        # Then
        assert result.to_dicts() == [{"a": 1, "b": "x"}, {"a": 2, "b": None}]
        assert scanned.equals(result)
        assert expected["a"].tolist() == [1, 2] and expected["b"].tolist()[0] == "x" and pd.isna(expected["b"].tolist()[1])

    @pytest.mark.unit
    def test_a_file_that_the_polars_engine_writes_with_header_false_reads_back_with_names(self, tmp_path):
        # Given: the pandas reader refuses `header: false`, and the polars reader reads the file that the writer gives
        config = Files.config(tmp_path, "csv", {"header": False, "index": False, "names": ["a", "b"]})
        Files.polars(config, Pair).write(pl.DataFrame({"a": [1], "b": ["007"]}))

        # When / Then
        assert (tmp_path / "data.csv").read_text() == "1,007\n"
        assert Files.polars(config, Pair).read().to_dicts() == [{"a": 1, "b": "007"}]

    @pytest.mark.unit
    @pytest.mark.parametrize("options, text", [({"header": 0}, "x,y\n1,z\n"), ({}, "1,z\n")])
    def test_names_replace_a_header_line_or_name_a_file_without_one_as_on_the_pandas_engine(self, tmp_path, options, text):
        # Given: with names and without header, pandas reads the first line as data
        (tmp_path / "data.csv").write_text(text)
        config = Files.config(tmp_path, "csv", {**options, "names": ["a", "b"]})

        # When
        result = Files.polars(config, Pair).read()
        EngineSetting.reset("pandas")
        expected = FilesIO(source_config=config, schema=PandasPair).read()

        # Then
        assert result.to_dicts() == [{"a": 1, "b": "z"}]
        assert expected.to_dict("records") == [{"a": 1, "b": "z"}]

    @pytest.mark.unit
    @pytest.mark.parametrize("names", [["a"], ["a", "b", "c"]])
    def test_names_for_another_number_of_columns_raise_at_the_read(self, tmp_path, names):
        # Given
        (tmp_path / "data.csv").write_text("1,x\n")
        config = Files.config(tmp_path, "csv", {"header": None, "names": names})

        # When / Then
        with pytest.raises(pl.exceptions.SchemaError, match="does not match number of columns"):
            Files.polars(config, Pair).read()

    @pytest.mark.unit
    @pytest.mark.parametrize("names", ["a,b", ["a", 1]])
    def test_names_that_are_not_a_list_of_strings_are_refused_when_the_resource_is_built(self, tmp_path, names):
        with pytest.raises(EngineResourceError, match="list of column names"):
            Files.polars(Files.config(tmp_path, "csv", {"names": names}), Pair)

    @pytest.mark.unit
    def test_null_strings_for_each_column_are_refused_when_the_resource_is_built(self, tmp_path):
        with pytest.raises(EngineResourceError, match="not for each column"):
            Files.polars(Files.config(tmp_path, "csv", {"na_values": {"b": ["-"]}}), Pair)

    @pytest.mark.unit
    def test_a_decimal_mark_that_is_not_a_point_or_a_comma_is_refused(self, tmp_path):
        with pytest.raises(EngineResourceError, match="decimal point or a decimal comma"):
            Files.polars(Files.config(tmp_path, "csv", {"decimal": "'"}), Pair)


class Tagged(DataModel, dataset="tagged"):
    id: int
    tags: pl.List(pl.String) = Field(nullable=True)  # type: ignore[valid-type]
    counts: pl.List(pl.Int64) = Field(nullable=True)  # type: ignore[valid-type]


class PandasTagged(pa.DataFrameModel):
    __dataset_name__ = "tagged"

    id: Series[int]
    tags: Series[object] = Field(nullable=True)
    counts: Series[object] = Field(nullable=True)


class TestListColumns:
    @staticmethod
    def frame() -> Any:
        """A frame of list columns with a null, an empty list and a null item."""
        return pl.DataFrame(
            {"id": [1, 2, 3], "tags": [["a", "b"], None, []], "counts": [[1, None], [], None]},
            schema={"id": pl.Int64, "tags": pl.List(pl.String), "counts": pl.List(pl.Int64)},
        )

    @staticmethod
    def pandas_frame() -> pd.DataFrame:
        return pd.DataFrame({"id": [1, 2, 3], "tags": [["a", "b"], None, []], "counts": [[1, None], [], None]})

    @staticmethod
    def as_lists(values: pd.Series) -> list:
        """The cells of a pandas column of lists as Python lists: pyarrow reads a list cell as a numpy array."""
        return [None if value is None else [None if pd.isna(item) else item for item in value] for value in values]

    @pytest.mark.unit
    @pytest.mark.parametrize("file_type, options", [("parquet", {}), ("json", {"orient": "records"}), ("json", {"orient": "records", "lines": True})])
    def test_a_frame_of_list_columns_reads_back_equal(self, tmp_path, file_type, options):
        # Given
        config = Files.config(tmp_path, file_type, options)

        # When
        Files.polars(config, Tagged).write(self.frame())
        result = Files.polars(config, Tagged).read()

        # Then
        assert result.equals(self.frame())
        assert result.schema == self.frame().schema

    @pytest.mark.unit
    def test_the_parquet_footer_of_list_columns_is_the_footer_of_the_pandas_engine(self, tmp_path):
        # When
        (polars_schema, _), (pandas_schema, _) = TestParquetFooter.footers(tmp_path, self.frame(), Tagged, self.pandas_frame(), PandasTagged)

        # Then
        assert polars_schema.equals(pandas_schema, check_metadata=True)
        assert json.loads(polars_schema.metadata[b"pandas"]) == json.loads(pandas_schema.metadata[b"pandas"])

    @pytest.mark.unit
    def test_a_list_column_with_a_null_row_that_has_items_reads_back_equal(self, tmp_path):
        # Given a null row that keeps the items of its list, as `when().then()` gives it
        config = Files.config(tmp_path, "parquet")
        frame = pl.DataFrame(
            {"id": [1, 2, 3], "tags": [["a"], ["b"], ["c"]], "counts": [[1], [2], [3]]}, schema={"id": pl.Int64, "tags": pl.List(pl.String), "counts": pl.List(pl.Int64)}
        )
        frame = frame.with_columns(pl.when(pl.col("id") != 2).then(pl.col(name)).alias(name) for name in ("tags", "counts"))
        assert frame["tags"].to_arrow().offsets.to_pylist() == [0, 1, 2, 3]

        # When
        Files.polars(config, Tagged).write(frame)

        # Then
        assert Files.polars(config, Tagged).read().equals(frame)

    @pytest.mark.unit
    def test_an_all_null_list_column_has_the_null_type_of_pandas(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")
        frame = pl.DataFrame({"id": [1], "tags": [None], "counts": [None]}, schema={"id": pl.Int64, "tags": pl.List(pl.String), "counts": pl.List(pl.Int64)})

        # When
        Files.polars(config, Tagged).write(frame)

        # Then
        schema = pq.ParquetFile(str(tmp_path / "data.parquet")).schema_arrow
        assert [schema.field(name).type for name in ("tags", "counts")] == [pa_types.null(), pa_types.null()]
        result = Files.polars(config, Tagged).read()
        assert result.schema["tags"] == pl.List(pl.String) and result.schema["counts"] == pl.List(pl.Int64)
        assert result["tags"].to_list() == [None] and result["counts"].to_list() == [None]

    @pytest.mark.unit
    def test_the_pandas_engine_reads_the_lists_of_a_polars_parquet_file(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")
        Files.polars(config, Tagged).write(self.frame())

        # When
        EngineSetting.reset("pandas")
        result = FilesIO(source_config=config, schema=PandasTagged).read()

        # Then
        assert self.as_lists(result["tags"]) == [["a", "b"], None, []]
        assert self.as_lists(result["counts"]) == [[1, None], [], None]

    @pytest.mark.unit
    def test_the_polars_engine_reads_the_lists_of_a_pandas_parquet_file(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")
        EngineSetting.reset("pandas")
        FilesIO(source_config=config, schema=PandasTagged).write(self.pandas_frame())

        # When
        result = Files.polars(config, Tagged).read()

        # Then
        assert result.equals(self.frame())

    @pytest.mark.unit
    @pytest.mark.parametrize("options", [{}, {"index": False}])
    def test_a_csv_resource_of_a_model_with_list_columns_is_refused_when_the_resource_is_built(self, tmp_path, options):
        with pytest.raises(EngineResourceError, match=r"a CSV file cannot hold the List columns \['tags', 'counts'\] of"):
            Files.polars(Files.config(tmp_path, "csv", options), Tagged)

    @pytest.mark.unit
    @pytest.mark.parametrize("operation", ["read", "scan"])
    def test_a_batch_with_a_file_of_empty_lists_reads_as_the_list_type_of_the_model(self, tmp_path, operation):
        # Given a file of empty lists, which pandas writes as list<null>, and a file of strings
        (tmp_path / "batch").mkdir()
        pd.DataFrame({"tags": [[], []]}).to_parquet(tmp_path / "batch" / "0.parquet")
        pd.DataFrame({"tags": [["a"], None]}).to_parquet(tmp_path / "batch" / "1.parquet")
        config = Files.config(tmp_path, "parquet", source="local_batch", path_prefix=f"{tmp_path}/batch/")

        class Tags(DataModel, dataset="tags"):
            tags: pl.List(pl.String) = Field(nullable=True)  # type: ignore[valid-type]

        # When
        io = Files.polars(config, Tags)
        result = io.read() if operation == "read" else io.scan().collect()

        # Then
        assert result.schema == pl.Schema({"tags": pl.List(pl.String)})
        assert result["tags"].to_list() == [[], [], ["a"], None]

    @pytest.mark.unit
    def test_a_sink_of_list_columns_has_the_footer_of_a_write(self, tmp_path):
        # Given
        write, sink = Files.config(tmp_path, "parquet", file_path=str(tmp_path / "write.parquet")), Files.config(tmp_path, "parquet", file_path=str(tmp_path / "sink.parquet"))

        # When
        Files.polars(write, Tagged).write(self.frame())
        Files.polars(sink, Tagged).sink(self.frame().lazy())

        # Then
        assert pq.read_schema(tmp_path / "sink.parquet").equals(pq.read_schema(tmp_path / "write.parquet"), check_metadata=True)
        assert Files.polars(sink, Tagged).read().equals(self.frame())


class Keyed(DataModel, dataset="keyed"):
    key: str
    n: int = Field(nullable=True, pandas_dtype="Int64")
    created_at: Annotated[pl.Datetime, "ns", None] = Field(nullable=True)


class PandasKeyed(pa.DataFrameModel):
    __dataset_name__ = "keyed"

    key: Series[str]
    n: Series[pd.Int64Dtype] = Field(nullable=True)
    created_at: Series[pd.Timestamp] = Field(nullable=True)


class TestJsonIndexColumn:
    """A JSON object with orient index or columns: its keys are the column `index_column` of the frame."""

    @staticmethod
    def frame() -> Any:
        return pl.DataFrame(
            {"key": ["data", "other"], "n": [1, None], "created_at": pl.Series([datetime(2026, 11, 3, 6, 0, 15), None], dtype=pl.Datetime("ns"))},
        )

    @staticmethod
    def pandas_frame() -> pd.DataFrame:
        return pd.DataFrame({"key": ["data", "other"], "n": pd.array([1, None], dtype="Int64"), "created_at": pd.to_datetime(["2026-11-03 06:00:15", None])})

    @pytest.mark.unit
    @pytest.mark.parametrize("orient", ["index", "columns"])
    def test_the_bytes_equal_the_bytes_of_the_pandas_engine_and_read_back_equal(self, tmp_path, orient):
        # Given
        config = Files.config(tmp_path, "json", {"orient": orient, "index_column": "key"})
        path = tmp_path / "data.json"

        # When
        Files.polars(config, Keyed).write(self.frame())
        polars_bytes = path.read_bytes()
        result = Files.polars(config, Keyed).read()
        EngineSetting.reset("pandas")
        FilesIO(source_config=config, schema=PandasKeyed).write(self.pandas_frame())

        # Then
        assert polars_bytes == path.read_bytes()
        assert json.loads(polars_bytes) == json.loads(self.pandas_frame().set_index("key").to_json(orient=orient))
        assert result.equals(self.frame())

    @pytest.mark.unit
    @pytest.mark.parametrize("orient", ["index", "columns"])
    @pytest.mark.parametrize("name, options", [("data.json.gz", {}), ("data.json", {"compression": "gzip"}), ("data.json.bz2", {})], ids=["suffix", "option", "bz2"])
    def test_a_compressed_file_reads_back_equal(self, tmp_path, orient, name, options):
        # Given
        path = tmp_path / name
        config = Files.config(tmp_path, "json", {"orient": orient, "index_column": "key", **options}, file_path=str(path))

        # When
        Files.polars(config, Keyed).write(self.frame())
        result = Files.polars(config, Keyed).read()

        # Then
        assert path.read_bytes()[:2] != b'{"'
        assert result.equals(self.frame())

    @pytest.mark.unit
    @pytest.mark.parametrize("orient", ["index", "columns"])
    def test_a_frame_without_rows_reads_back_with_the_dtypes_of_the_model(self, tmp_path, orient):
        # Given
        config = Files.config(tmp_path, "json", {"orient": orient, "index_column": "key"})
        empty = self.frame().clear()

        # When
        Files.polars(config, Keyed).write(empty)
        result = Files.polars(config, Keyed).read()

        # Then
        assert result.equals(empty)
        assert result.schema == empty.schema

    @pytest.mark.unit
    @pytest.mark.parametrize("orient", ["index", "columns"])
    def test_the_pandas_engine_reads_the_keys_as_the_index_column(self, tmp_path, orient):
        # Given
        config = Files.config(tmp_path, "json", {"orient": orient, "index_column": "key"})
        Files.polars(config, Keyed).write(self.frame())

        # When
        EngineSetting.reset("pandas")
        result = FilesIO(source_config=config, schema=PandasKeyed).read()

        # Then
        pd.testing.assert_frame_equal(result, self.pandas_frame())

    @pytest.mark.unit
    @pytest.mark.parametrize("orient, document", [("index", '{"outer":{"key":"inner","n":1}}'), ("columns", '{"key":{"outer":"inner"},"n":{"outer":1}}')])
    def test_a_column_of_the_name_of_the_index_column_is_refused_on_both_engines(self, tmp_path, orient, document):
        # Given
        (tmp_path / "data.json").write_text(document)
        config = Files.config(tmp_path, "json", {"orient": orient, "index_column": "key"})

        # When / Then
        with pytest.raises(ValueError, match="cannot insert key, already exists"):
            Files.polars(config, Keyed).read()
        EngineSetting.reset("pandas")
        with pytest.raises(ValueError, match="cannot insert key, already exists"):
            FilesIO(source_config=config, schema=PandasKeyed).read()

    @pytest.mark.unit
    def test_a_key_that_a_column_does_not_have_is_a_null(self, tmp_path):
        # Given
        (tmp_path / "data.json").write_text('{"n":{"a":1,"b":2},"created_at":{"a":1793685615000}}')
        config = Files.config(tmp_path, "json", {"orient": "columns", "index_column": "key"})

        # When
        result = Files.polars(config, Keyed).read()

        # Then
        assert result.get_column("key").to_list() == ["a", "b"]
        assert result.get_column("created_at").to_list() == [datetime(2026, 11, 3, 6, 0, 15), None]

    @pytest.mark.unit
    def test_the_keys_take_a_numeric_dtype_of_the_model(self, tmp_path):
        class Numbered(DataModel, dataset="numbered"):
            key: int
            n: int

        # Given
        (tmp_path / "data.json").write_text('{"7":{"n":1},"8":{"n":2}}')
        config = Files.config(tmp_path, "json", {"orient": "index", "index_column": "key"})

        # When
        result = Files.polars(config, Numbered).read()

        # Then
        assert result.equals(pl.DataFrame({"key": [7, 8], "n": [1, 2]}))

    @pytest.mark.unit
    def test_an_empty_object_is_a_frame_without_rows(self, tmp_path):
        # Given
        (tmp_path / "data.json").write_text("{}")
        config = Files.config(tmp_path, "json", {"orient": "index", "index_column": "key"})

        # When
        result = Files.polars(config, Keyed).read()

        # Then
        assert result.height == 0
        assert result.schema == pl.Schema({"key": pl.String, "n": pl.Int64, "created_at": pl.Datetime("ns")})

    @pytest.mark.unit
    def test_an_index_column_that_the_model_does_not_have_is_refused(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "json", {"orient": "index", "index_column": "label"})

        # When / Then
        with pytest.raises(EngineResourceError, match="the index_column 'label' of .* is not a column of its model"):
            Files.polars(config, Keyed)


class Indexed(DataModel, dataset="indexed"):
    vessel_id: str
    event_id: str
    note: str = Field(nullable=True)
    index_level_1: str = Field(nullable=False, alias="__index_level_1__")


class TestParquetIndexColumns:
    OPTIONS = {"index_columns": ["vessel_id", {"__index_level_1__": "event_id"}]}

    @staticmethod
    def frame() -> pl.DataFrame:
        return pl.DataFrame({"vessel_id": ["v1", "v2"], "event_id": ["e1", "e2"], "note": pl.Series([None, "x"], dtype=pl.String), "__index_level_1__": ["e1", "e2"]})

    @pytest.mark.unit
    def test_index_columns_give_the_file_of_a_pandas_frame_with_that_index(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet", self.OPTIONS)
        indexed = self.frame().to_pandas().drop(columns="__index_level_1__").set_index(["vessel_id", "event_id"], drop=False).drop(columns="vessel_id")
        indexed.to_parquet(tmp_path / "pandas.parquet")

        # When
        Files.polars(config, Indexed).write(self.frame())

        # Then
        footer = pq.read_schema(str(tmp_path / "data.parquet"))
        assert footer.equals(pq.read_schema(str(tmp_path / "pandas.parquet")), check_metadata=True)
        assert Files.polars(Files.config(tmp_path, "parquet"), Indexed).read().equals(self.frame())

    @pytest.mark.unit
    def test_the_pandas_engine_reads_the_index_of_index_columns(self, tmp_path):
        # Given
        Files.polars(Files.config(tmp_path, "parquet", self.OPTIONS), Indexed).write(self.frame())

        # When
        result = pd.read_parquet(tmp_path / "data.parquet")

        # Then
        assert result.index.names == ["vessel_id", "event_id"]
        assert result.index.tolist() == [("v1", "e1"), ("v2", "e2")]

    @pytest.mark.unit
    @pytest.mark.parametrize("value", ["vessel_id", [], [{"vessel_id": "a", "event_id": "b"}], ["vessel_id", "vessel_id"], [1]])
    def test_an_index_columns_value_that_is_not_valid_is_refused_when_the_resource_is_built(self, tmp_path, value):
        with pytest.raises(EngineResourceError, match="index_columns"):
            Files.polars(Files.config(tmp_path, "parquet", {"index_columns": value}), Indexed)

    @pytest.mark.unit
    def test_an_index_column_that_is_not_in_the_frame_is_refused(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet", {"index_columns": ["missing"]})

        # When / Then
        with pytest.raises(EngineResourceError, match="not columns of the frame"):
            Files.polars(config, Indexed).write(self.frame())

    @pytest.mark.unit
    def test_an_index_column_with_another_name_than_its_field_is_refused(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet", {"index_columns": [{"vessel_id": None}]})

        # When / Then
        with pytest.raises(EngineResourceError, match="Name the column '__index_level_0__'"):
            Files.polars(config, Indexed).write(self.frame())
