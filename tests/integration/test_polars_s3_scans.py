# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import io
import logging
import uuid
from datetime import datetime, timedelta
from typing import Annotated

import pandas as pd
import pytest
from botocore.exceptions import ClientError

# Application Imports
from dynamicio.engines import EngineSetting
from dynamicio.errors import ColumnsDataTypeError, NoObjectsFoundError, ObjectNotFoundError, S3KeyNotFoundError
from dynamicio.schema import Field
from tests.integration.resources import Resources

pl = pytest.importorskip("polars")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
polars_files = pytest.importorskip("tests.test_engines.test_polars_files")
DataModel = pytest.importorskip("dynamicio.schema.polars").DataModel
Files, FilesIO, Pair, PandasPair = polars_files.Files, polars_files.FilesIO, polars_files.Pair, polars_files.PandasPair

pytestmark = [pytest.mark.integration, pytest.mark.floci]

CASES = [("parquet", None), ("csv", None), ("csv", {"index": False}), ("json", None), ("json", {"lines": True, "orient": "records"})]


class Wide(DataModel, dataset="wide"):
    i: int
    f: float
    s: str = Field(nullable=True)
    c: str = Field(nullable=True)
    t: Annotated[pl.Datetime, "ns", None]


@pytest.fixture(autouse=True)
def default_engine():
    EngineSetting.reset()
    yield
    EngineSetting.reset()


class Objects:
    @staticmethod
    def put(floci, key, body: bytes):
        floci.s3_client().put_object(Bucket=floci.bucket, Key=key, Body=body)

    @staticmethod
    def put_table(floci, key, table):
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        Objects.put(floci, key, buffer.getvalue())

    @staticmethod
    def put_frame(floci, key, frame: pd.DataFrame, file_type: str):
        if file_type == "parquet":
            body = frame.to_parquet()
        elif file_type == "csv":
            body = frame.to_csv(index=False).encode()
        else:
            body = frame.to_json(orient="records").encode()
        Objects.put(floci, key, body)

    @staticmethod
    def read_with_pandas(config, model=PandasPair):
        EngineSetting.reset("pandas")
        try:
            return FilesIO(source_config=config, schema=model).read()
        finally:
            EngineSetting.reset()


class TestScanOfOneObject:
    @pytest.mark.parametrize("file_type, options", CASES)
    def test_a_scan_collects_to_the_frame_that_was_written(self, floci, prefix, tmp_path, file_type, options):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/data.{file_type}", file_type, options)
        Files.polars(config).write(Files.frame())

        # When
        scanned = Files.polars(config).scan()

        # Then
        assert isinstance(scanned, pl.LazyFrame)
        assert scanned.collect().equals(Files.frame())

    @pytest.mark.parametrize("file_type, options", CASES)
    def test_a_scan_of_an_object_of_the_pandas_engine_collects_to_the_frame_of_the_model(self, floci, prefix, tmp_path, file_type, options):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/data.{file_type}", file_type, options)
        Files.write_with_pandas(config)

        # When / Then
        assert Files.polars(config).scan().collect().equals(Files.frame())

    def test_a_filter_on_a_parquet_scan_gives_the_rows_that_match(self, floci, prefix, tmp_path):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/data.parquet", "parquet")
        Files.polars(config, Pair).write(pl.DataFrame({"a": list(range(100)), "b": [str(n) for n in range(100)]}))

        # When
        result = Files.polars(config, Pair).scan().filter(pl.col("a") >= 98).select("b").collect()

        # Then
        assert result.to_dicts() == [{"b": "98"}, {"b": "99"}]

    def test_an_empty_parquet_object_scans_without_rows(self, floci, prefix, tmp_path):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/data.parquet", "parquet")
        Files.polars(config, Pair).write(pl.DataFrame(schema={"a": pl.Int64, "b": pl.String}))

        # When / Then
        assert Files.polars(config, Pair).scan().collect().height == 0


class TestScanOfMissingObjects:
    @pytest.mark.parametrize("file_type, error", [("parquet", S3KeyNotFoundError), ("csv", ObjectNotFoundError), ("json", ObjectNotFoundError)])
    def test_a_missing_key_raises_the_error_of_the_pandas_engine_at_the_scan(self, floci, prefix, tmp_path, file_type, error):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/missing.{file_type}", file_type)

        # When / Then
        with pytest.raises(error):
            Files.polars(config, Pair).scan()

    @pytest.mark.parametrize("file_type", ["parquet", "csv", "json"])
    def test_a_missing_key_returns_none_with_missing_ok(self, floci, prefix, tmp_path, file_type):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/missing.{file_type}", file_type)

        # When / Then
        assert Files.polars(config, Pair, missing_ok=True).scan() is None

    @pytest.mark.usefixtures("floci")
    def test_a_missing_bucket_is_not_a_missing_key_even_with_missing_ok(self, tmp_path):
        # Given
        config = Resources.s3_file(tmp_path, f"dynamicio-missing-{uuid.uuid4().hex[:8]}", "data.parquet", "parquet")

        # When / Then
        with pytest.raises(ClientError) as raised:
            Files.polars(config, Pair, missing_ok=True).scan()
        assert not isinstance(raised.value, S3KeyNotFoundError)

    def test_a_prefix_key_is_not_scanned(self, floci, prefix, tmp_path):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/data/", "parquet")

        # When / Then
        with pytest.raises(NotImplementedError, match="objects under an S3 prefix"):
            Files.polars(config, Pair).scan()


class TestPrefixRead:
    @pytest.mark.parametrize("file_type", ["parquet", "csv", "json"])
    def test_a_prefix_reads_as_the_frame_of_the_pandas_engine(self, floci, prefix, tmp_path, file_type):
        # Given
        for name, rows in (("1", {"a": [1, 2], "b": ["x", None]}), ("2", {"a": [3], "b": ["z"]}), ("sub/3", {"a": [4], "b": ["w"]})):
            Objects.put_frame(floci, f"{prefix}/data/{name}.{file_type}", pd.DataFrame(rows), file_type)
        options = {"orient": "records"} if file_type == "json" else None
        config = Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/data/", file_type, options=options)

        # When
        result = Files.polars(config, Pair).read()
        scanned = Files.polars(config, Pair).scan()

        # Then
        expected = Objects.read_with_pandas(config)
        assert result.to_dicts() == [{"a": a, "b": None if pd.isna(b) else b} for a, b in zip(expected["a"], expected["b"])]
        assert result.to_dicts() == [{"a": 1, "b": "x"}, {"a": 2, "b": None}, {"a": 3, "b": "z"}, {"a": 4, "b": "w"}]
        assert isinstance(scanned, pl.LazyFrame)
        assert scanned.collect().equals(result)

    def test_a_filter_on_a_prefix_scan_gives_the_rows_that_match(self, floci, prefix, tmp_path):
        # Given
        for n in range(3):
            Objects.put_frame(floci, f"{prefix}/data/{n}.parquet", pd.DataFrame({"a": [n * 10, n * 10 + 1], "b": [f"r{n}", None]}), "parquet")
        config = Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/data/")

        # When
        result = Files.polars(config, Pair).scan().filter(pl.col("a") > 10).select("a").collect()

        # Then
        assert result["a"].to_list() == [11, 20, 21]

    def test_a_dynamic_file_path_reads_the_keys_of_its_pattern(self, floci, prefix, tmp_path):
        # Given
        Objects.put_frame(floci, f"{prefix}/data/x/run_7.parquet", pd.DataFrame({"a": [1], "b": ["x"]}), "parquet")
        Objects.put_frame(floci, f"{prefix}/data/y/z/run_7.parquet", pd.DataFrame({"a": [2], "b": ["y"]}), "parquet")
        Objects.put_frame(floci, f"{prefix}/data/x/run_8.parquet", pd.DataFrame({"a": [3], "b": ["no"]}), "parquet")
        config = Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/data/**/run_{{run_id}}.parquet", options={"run_id": 7}, path_key="dynamic_file_path")

        # When
        result = Files.polars(config, Pair, run_id=7).read()

        # Then
        assert result.to_dicts() == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
        assert result.to_dicts() == Objects.read_with_pandas(config).to_dict("records")

    def test_the_columns_option_selects_the_columns(self, floci, prefix, tmp_path):
        # Given
        Objects.put_frame(floci, f"{prefix}/data/1.parquet", pd.DataFrame({"a": [1], "b": ["x"]}), "parquet")
        config = Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/data/", options={"columns": ["b", "a"]})

        # When / Then
        assert Files.polars(config, Pair).read().columns == ["b", "a"]

    def test_the_directory_keys_under_a_prefix_are_left_out(self, floci, prefix, tmp_path):
        # Given
        Objects.put(floci, f"{prefix}/data/sub/", b"")
        Objects.put_frame(floci, f"{prefix}/data/1.parquet", pd.DataFrame({"a": [1], "b": ["x"]}), "parquet")
        config = Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/data/")

        # When / Then
        assert Files.polars(config, Pair).read().to_dicts() == [{"a": 1, "b": "x"}]


class TestPrefixWithoutObjects:
    @pytest.mark.parametrize("file_type", ["parquet", "csv"])
    def test_a_prefix_without_objects_raises_no_objects_found(self, floci, prefix, tmp_path, file_type):
        # Given
        config = Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/empty/", file_type)

        # When / Then
        with pytest.raises(NoObjectsFoundError):
            Files.polars(config, Pair).read()
        with pytest.raises(NoObjectsFoundError):
            Files.polars(config, Pair).scan()

    def test_a_prefix_without_objects_returns_none_with_missing_ok(self, floci, prefix, tmp_path):
        # Given
        config = Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/empty/")

        # When / Then
        assert Files.polars(config, Pair, missing_ok=True).read() is None
        assert Files.polars(config, Pair, missing_ok=True).scan() is None


class TestPrefixMissingColumns:
    def test_an_object_that_lacks_a_column_gets_nulls_and_the_warning_of_the_pandas_engine(self, floci, prefix, tmp_path, caplog):
        # Given
        Objects.put_frame(floci, f"{prefix}/data/1.parquet", pd.DataFrame({"a": [1]}), "parquet")
        Objects.put_frame(floci, f"{prefix}/data/2.parquet", pd.DataFrame({"a": [2]}), "parquet")
        Objects.put_frame(floci, f"{prefix}/data/3.parquet", pd.DataFrame({"b": ["z"], "a": [3]}), "parquet")
        config = Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/data/")

        # When
        with caplog.at_level(logging.WARNING):
            result = Files.polars(config, Pair).read()
        polars_warnings = [record.getMessage() for record in caplog.records if "lack columns" in record.getMessage()]
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            Objects.read_with_pandas(config)
        pandas_warnings = [record.getMessage() for record in caplog.records if "lack columns" in record.getMessage()]

        # Then
        assert result.to_dicts() == [{"a": 1, "b": None}, {"a": 2, "b": None}, {"a": 3, "b": "z"}]
        assert polars_warnings == pandas_warnings
        assert "{'b': 2}" in polars_warnings[0]

    def test_a_column_that_no_object_has_raises_the_value_error_of_the_pandas_engine(self, floci, prefix, tmp_path):
        # Given
        Objects.put_frame(floci, f"{prefix}/data/1.parquet", pd.DataFrame({"a": [1]}), "parquet")
        config = Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/data/")

        # When
        with pytest.raises(ValueError) as polars_error:
            Files.polars(config, Pair).scan()
        with pytest.raises(ValueError) as pandas_error:
            Objects.read_with_pandas(config)

        # Then
        assert str(polars_error.value) == str(pandas_error.value)
        assert "missing from every parquet object" in str(polars_error.value)


class TestPrefixTypes:
    def test_the_narrower_types_of_some_objects_upcast_to_the_widest_type(self, floci, prefix, tmp_path):
        # Given
        old = pa.table(
            {
                "i": pa.array([1], pa.int32()),
                "f": pa.array([1.5], pa.float32()),
                "s": pa.array([None], pa.null()),
                "c": pa.array(["k"]).dictionary_encode(),
                "t": pa.array([datetime(2024, 1, 1)], pa.timestamp("us")),
            }
        )
        new = pa.table(
            {
                "i": pa.array([2**40], pa.int64()),
                "f": pa.array([2.25], pa.float64()),
                "s": pa.array(["v"]),
                "c": pa.array(["m"]),
                "t": pa.array([datetime(2024, 1, 2)], pa.timestamp("ns")),
            }
        )
        Objects.put_table(floci, f"{prefix}/data/1.parquet", old)
        Objects.put_table(floci, f"{prefix}/data/2.parquet", new)
        config = Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/data/")

        # When
        result = Files.polars(config, Wide).read()

        # Then
        assert result.schema == pl.Schema({"i": pl.Int64, "f": pl.Float64, "s": pl.String, "c": pl.String, "t": pl.Datetime("ns")})
        assert result.to_dicts() == [
            {"i": 1, "f": 1.5, "s": None, "c": "k", "t": datetime(2024, 1, 1)},
            {"i": 2**40, "f": 2.25, "s": "v", "c": "m", "t": datetime(2024, 1, 2)},
        ]

    def test_an_integer_column_of_one_object_and_a_float_column_of_another_read_as_floats(self, floci, prefix, tmp_path):
        class Prices(DataModel, dataset="prices"):
            price: float

        # Given
        Objects.put_table(floci, f"{prefix}/data/1.parquet", pa.table({"price": pa.array([1], pa.int64())}))
        Objects.put_table(floci, f"{prefix}/data/2.parquet", pa.table({"price": pa.array([2.5], pa.float64())}))
        config = Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/data/")

        # When / Then
        assert Files.polars(config, Prices).read()["price"].to_list() == [1.0, 2.5]

    @pytest.mark.parametrize(
        "arrays",
        [(pa.array(["x"]), pa.array([1], pa.int64())), (pa.array([1], pa.int64()), pa.array([2], pa.uint64()))],
    )
    def test_types_that_do_not_upcast_raise_a_columns_data_type_error(self, floci, prefix, tmp_path, arrays, caplog):
        # Given
        first, second = arrays
        Objects.put_table(floci, f"{prefix}/data/1.parquet", pa.table({"a": first, "b": ["x"]}))
        Objects.put_table(floci, f"{prefix}/data/2.parquet", pa.table({"a": second, "b": ["y"]}))
        config = Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/data/")

        # When / Then
        with pytest.raises(ColumnsDataTypeError):
            Files.polars(config, Pair).scan()
        assert "which do not upcast to one type" in caplog.text

    def test_an_empty_object_with_another_type_is_left_out_as_on_the_pandas_engine(self, floci, prefix, tmp_path):
        # Given
        Objects.put_table(floci, f"{prefix}/data/1.parquet", pa.table({"a": pa.array([], pa.string()), "b": pa.array([], pa.string())}))
        Objects.put_table(floci, f"{prefix}/data/2.parquet", pa.table({"a": pa.array([1], pa.int64()), "b": ["x"]}))
        config = Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/data/")

        # When
        result = Files.polars(config, Pair).read()

        # Then
        assert result.to_dicts() == [{"a": 1, "b": "x"}]
        assert result.to_pandas().equals(Objects.read_with_pandas(config))

    def test_durations_of_several_units_read_in_the_unit_of_the_model(self, floci, prefix, tmp_path):
        class Waits(DataModel, dataset="waits"):
            n: int
            wait: Annotated[pl.Duration, "ns"]

        # Given
        for number, unit in enumerate(["ms", "ns", "ms"]):
            table = pa.table({"n": pa.array([number], pa.int64()), "wait": pa.array([timedelta(seconds=number + 1)], pa.duration(unit))})
            Objects.put_table(floci, f"{prefix}/data/{number}.parquet", table)
        config = Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/data/")

        # When
        result = Files.polars(config, Waits).read()

        # Then
        assert result.schema == pl.Schema({"n": pl.Int64, "wait": pl.Duration("ns")})
        assert result.to_dicts() == [{"n": number, "wait": timedelta(seconds=number + 1)} for number in range(3)]
