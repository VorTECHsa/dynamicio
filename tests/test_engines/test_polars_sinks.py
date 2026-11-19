# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import io
import os
from pathlib import Path
from typing import Annotated, Any, Dict, List, Optional
from unittest.mock import patch

import pandas as pd
import pytest

from dynamicio.engines import EngineSetting
from dynamicio.errors import ColumnsDataTypeError, SchemaValidationError
from dynamicio.schema import Field, Metric
from tests import constants

pl = pytest.importorskip("polars")
pq = pytest.importorskip("pyarrow.parquet")
DataModel = pytest.importorskip("dynamicio.schema.polars").DataModel
polars_files = pytest.importorskip("tests.test_engines.test_polars_files")
Files, FilesIO, Mixed, Pair = polars_files.Files, polars_files.FilesIO, polars_files.Mixed, polars_files.Pair


class Checked(DataModel, dataset="checked"):
    a: int = Field(nullable=False, gt=0, metrics=[Metric.MIN, Metric.MAX])
    b: str = Field(nullable=True, metrics=[Metric.COUNTS])


class Kinds(DataModel, dataset="kinds"):
    label: pl.Categorical = Field(nullable=True)
    level: Annotated[pl.Enum, ["low", "mid", "high"]] = Field(nullable=True)
    empty: pl.Categorical = Field(nullable=True)
    nothing: str = Field(nullable=True)


@pytest.fixture(autouse=True)
def default_engine():
    EngineSetting.reset()
    yield
    EngineSetting.reset()


class Sinks:
    """The resources and the file checks of the sink tests."""

    @staticmethod
    def s3_config(tmp_path: Path, source: str, file_type: str, options: Optional[Dict[str, Any]] = None, **section: Any) -> Any:
        return Files.config(tmp_path, file_type, options, source, bucket=constants.MOCK_BUCKET, **section)

    @staticmethod
    def footer(path: Path) -> Any:
        parquet = pq.ParquetFile(str(path))
        groups = [parquet.metadata.row_group(i) for i in range(parquet.metadata.num_row_groups)]
        columns = [[(group.column(i).physical_type, group.column(i).compression) for i in range(group.num_columns)] for group in groups]
        return parquet.schema_arrow, [group.num_rows for group in groups], columns

    @staticmethod
    def write_and_sink(tmp_path: Path, file_type: str, options: Dict[str, Any], frame: Any, model: Any = Mixed):
        """The paths of the file of ``write()`` and of the file of ``sink()``, for the same frame and the same options."""
        written, sunk = tmp_path / f"written.{file_type}", tmp_path / f"sunk.{file_type}"
        Files.polars(Files.config(tmp_path, file_type, options, file_path=str(written)), model).write(frame)
        Files.polars(Files.config(tmp_path, file_type, options, file_path=str(sunk)), model).sink(frame.lazy())
        return written, sunk

    @staticmethod
    def body(client, key: str) -> bytes:
        return client.get_object(Bucket=constants.MOCK_BUCKET, Key=key)["Body"].read()

    @staticmethod
    def write_or_sink(resource: Any, frame: Any, lazy: bool) -> None:
        if lazy:
            resource.sink(frame.lazy())
        else:
            resource.write(frame)

    @staticmethod
    def stages(directory: Path) -> List[str]:
        return [name for name in os.listdir(directory) if name.startswith(".dynamicio-sink-")]

    @staticmethod
    def logged_metrics(action) -> List[Any]:
        with patch("dynamicio.schema.polars_metrics.PolarsMetrics.log") as log:
            action()
        return [call.args for call in log.call_args_list]


class TestSinkGivesTheFileOfWrite:
    @pytest.mark.unit
    @pytest.mark.parametrize("file_type, options", [case for case in Files.CASES if case[0] != "parquet"])
    def test_a_text_file_has_the_bytes_of_write(self, tmp_path, file_type, options):
        # Given / When
        written, sunk = Sinks.write_and_sink(tmp_path, file_type, options, Files.frame())

        # Then
        assert sunk.read_bytes() == written.read_bytes()

    @pytest.mark.unit
    @pytest.mark.parametrize("options", [{}, {"row_group_size": 2}, {"row_group_size": 1}, {"compression": "zstd"}, {"index": False}])
    def test_a_parquet_file_has_the_footer_and_the_row_groups_of_write(self, tmp_path, options):
        # Given / When
        written, sunk = Sinks.write_and_sink(tmp_path, "parquet", options, Files.frame())

        # Then
        (sunk_schema, sunk_rows, sunk_columns), (written_schema, written_rows, written_columns) = Sinks.footer(sunk), Sinks.footer(written)
        assert sunk_schema.equals(written_schema, check_metadata=True)
        assert (sunk_rows, sunk_columns) == (written_rows, written_columns)
        pd.testing.assert_frame_equal(pd.read_parquet(sunk), pd.read_parquet(written))

    @pytest.mark.unit
    def test_the_rows_go_in_row_groups_of_the_row_group_size(self, tmp_path):
        # Given
        frame = pl.DataFrame({"a": list(range(10)), "b": [str(i) for i in range(10)]})

        # When
        written, sunk = Sinks.write_and_sink(tmp_path, "parquet", {"row_group_size": 3}, frame, Pair)

        # Then
        assert Sinks.footer(sunk)[1] == Sinks.footer(written)[1] == [3, 3, 3, 1]
        assert pl.read_parquet(sunk).equals(frame)

    @pytest.mark.unit
    @pytest.mark.parametrize("file_type, options", Files.CASES)
    def test_a_frame_without_rows_gives_the_file_of_write(self, tmp_path, file_type, options):
        # Given
        frame = pl.DataFrame({"a": pl.Series([], dtype=pl.Int64), "b": pl.Series([], dtype=pl.String)})

        # When
        written, sunk = Sinks.write_and_sink(tmp_path, file_type, options, frame, Pair)

        # Then
        if file_type == "parquet":
            assert Sinks.footer(sunk)[0].equals(Sinks.footer(written)[0], check_metadata=True)
            assert Sinks.footer(sunk)[1] == Sinks.footer(written)[1] == [0]
        else:
            assert sunk.read_bytes() == written.read_bytes()

    @pytest.mark.unit
    def test_categorical_enum_and_all_null_columns_have_the_footer_of_write(self, tmp_path):
        # Given
        frame = pl.DataFrame(
            {
                "label": pl.Series(["x", None, "y", "x"], dtype=pl.Categorical),
                "level": pl.Series(["low", "high", None, "low"], dtype=pl.Enum(["low", "mid", "high"])),
                "empty": pl.Series([None, None, None, None], dtype=pl.Categorical),
                "nothing": pl.Series([None, None, None, None], dtype=pl.String),
            }
        )

        # When
        written, sunk = Sinks.write_and_sink(tmp_path, "parquet", {}, frame, Kinds)

        # Then
        assert Sinks.footer(sunk)[0].equals(Sinks.footer(written)[0], check_metadata=True)
        pd.testing.assert_frame_equal(pd.read_parquet(sunk), pd.read_parquet(written))

    @pytest.mark.unit
    def test_the_sink_reads_back_equal(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")

        # When
        Files.polars(config).sink(Files.frame().lazy())

        # Then
        assert Files.polars(config).read().equals(Files.frame())

    @pytest.mark.unit
    def test_a_data_frame_is_sunk_as_its_lazy_frame(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")

        # When
        Files.polars(config).sink(Files.frame())

        # Then
        assert Files.polars(config).read().equals(Files.frame())

    @pytest.mark.unit
    def test_the_columns_that_are_not_in_the_model_are_not_written(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")

        # When
        Files.polars(config, Pair).sink(pl.LazyFrame({"extra": [0.5], "b": ["x"], "a": [1]}))

        # Then
        assert pl.read_parquet(tmp_path / "data.parquet").columns == ["b", "a"]

    @pytest.mark.unit
    def test_the_stage_directory_is_removed(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")

        # When
        Files.polars(config).sink(Files.frame().lazy())

        # Then
        assert not Sinks.stages(tmp_path)


class TestSinkValidationsAndMetrics:
    @pytest.mark.unit
    def test_a_failed_validation_leaves_the_old_file_and_logs_no_metrics(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")
        Files.polars(config, Checked).write(pl.DataFrame({"a": [1], "b": ["old"]}))
        before = (tmp_path / "data.parquet").read_bytes()
        resource = Files.polars(config, Checked, apply_schema_validations=True, log_schema_metrics=True)

        # When
        with patch("dynamicio.schema.polars_metrics.PolarsMetrics.log") as log, pytest.raises(SchemaValidationError) as error:
            resource.sink(pl.LazyFrame({"a": [1, -1], "b": ["x", "y"]}))

        # Then
        assert "is_greater_than" in error.value.message
        assert (tmp_path / "data.parquet").read_bytes() == before
        assert not Sinks.stages(tmp_path)
        log.assert_not_called()

    @pytest.mark.unit
    @pytest.mark.parametrize("file_type, options", [("csv", {}), ("json", {"orient": "records"})])
    def test_a_failed_validation_does_not_write_a_text_file(self, tmp_path, file_type, options):
        # Given
        config = Files.config(tmp_path, file_type, options)

        # When / Then
        with pytest.raises(SchemaValidationError):
            Files.polars(config, Checked, apply_schema_validations=True).sink(pl.LazyFrame({"a": [0], "b": ["x"]}))
        assert not (tmp_path / f"data.{file_type}").exists()

    @pytest.mark.unit
    def test_the_metrics_of_a_sink_are_the_metrics_of_write(self, tmp_path):
        # Given
        frame = pl.DataFrame({"a": [3, 1, 2], "b": ["x", None, "y"]})
        config = Files.config(tmp_path, "parquet")

        # When
        written = Sinks.logged_metrics(lambda: Files.polars(config, Checked, log_schema_metrics=True).write(frame))
        sunk = Sinks.logged_metrics(lambda: Files.polars(config, Checked, log_schema_metrics=True).sink(frame.lazy()))

        # Then
        assert sunk == written
        assert len(sunk) == 3

    @pytest.mark.unit
    def test_no_validation_runs_when_validations_are_off(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")

        # When
        Files.polars(config, Checked, apply_schema_validations=False).sink(pl.LazyFrame({"a": [-1], "b": ["x"]}))

        # Then
        assert pl.read_parquet(tmp_path / "data.parquet")["a"].to_list() == [-1]

    @pytest.mark.unit
    def test_the_validations_see_the_cast_frame(self, tmp_path):
        # Given: a string column that casts to the integers of the model
        config = Files.config(tmp_path, "parquet")

        # When
        Files.polars(config, Checked, apply_schema_validations=True).sink(pl.LazyFrame({"a": ["2", "3"], "b": ["x", "y"]}))

        # Then
        assert pl.read_parquet(tmp_path / "data.parquet")["a"].to_list() == [2, 3]


class TestSinkErrors:
    @pytest.mark.unit
    def test_a_failed_cast_raises_columns_data_type_error_and_writes_nothing(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")

        # When / Then
        with pytest.raises(ColumnsDataTypeError):
            Files.polars(config, Pair).sink(pl.LazyFrame({"a": ["1", "one"], "b": ["x", "y"]}))
        assert not (tmp_path / "data.parquet").exists()
        assert not Sinks.stages(tmp_path)

    @pytest.mark.unit
    def test_a_missing_column_raises_columns_data_type_error(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")

        # When / Then
        with pytest.raises(ColumnsDataTypeError):
            Files.polars(config, Pair).sink(pl.LazyFrame({"a": [1]}))

    @pytest.mark.unit
    def test_the_pandas_engine_refuses_a_sink(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")
        EngineSetting.reset("pandas")
        resource = FilesIO(source_config=config, schema=polars_files.PandasPair)

        # When / Then
        with pytest.raises(NotImplementedError, match="sink\\(\\) needs the polars engine"):
            resource.sink(pd.DataFrame({"a": [1], "b": ["x"]}))

    @pytest.mark.unit
    def test_a_resource_that_is_not_sunk_yet_is_refused(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet", source="local_batch", path_prefix=f"{tmp_path}/batch/")

        # When / Then
        with pytest.raises(NotImplementedError, match="does not sink local_batch resources yet"):
            Files.polars(config, Pair).sink(pl.LazyFrame({"a": [1], "b": ["x"]}))

    @pytest.mark.unit
    def test_an_s3_parquet_key_that_ends_in_a_slash_is_refused(self, tmp_path):
        # Given
        config = Sinks.s3_config(tmp_path, "s3_file", "parquet", file_path="data/")

        # When / Then
        with pytest.raises(ValueError, match="must be a file, not a directory"):
            Files.polars(config, Pair).sink(pl.LazyFrame({"a": [1], "b": ["x"]}))


class TestSinkOnS3:
    @pytest.mark.unit
    @pytest.mark.parametrize("file_type, options", Files.CASES)
    def test_the_object_of_a_sink_is_the_object_of_write(self, moto_s3_bucket, tmp_path, file_type, options):
        # Given
        written = Sinks.s3_config(tmp_path, "s3_file", file_type, options, file_path=f"written.{file_type}")
        sunk = Sinks.s3_config(tmp_path, "s3_file", file_type, options, file_path=f"sunk.{file_type}")

        # When
        Files.polars(written).write(Files.frame())
        Files.polars(sunk).sink(Files.frame().lazy())

        # Then
        (tmp_path / "w").write_bytes(Sinks.body(moto_s3_bucket, f"written.{file_type}"))
        (tmp_path / "s").write_bytes(Sinks.body(moto_s3_bucket, f"sunk.{file_type}"))
        if file_type == "parquet":
            assert Sinks.footer(tmp_path / "s") == Sinks.footer(tmp_path / "w")
        else:
            assert (tmp_path / "s").read_bytes() == (tmp_path / "w").read_bytes()
        assert Files.polars(sunk).read().equals(Files.frame())

    @pytest.mark.unit
    def test_the_object_has_the_bucket_owner_full_control_acl(self, moto_s3_bucket, tmp_path):
        # Given
        config = Sinks.s3_config(tmp_path, "s3_file", "parquet", file_path="data.parquet")

        # When
        with patch.object(moto_s3_bucket, "upload_file", wraps=moto_s3_bucket.upload_file) as upload:
            Files.polars(config, Pair).sink(pl.LazyFrame({"a": [1], "b": ["x"]}))

        # Then
        assert upload.call_args.kwargs["ExtraArgs"] == {"ACL": "bucket-owner-full-control"}

    @pytest.mark.unit
    def test_a_failed_validation_leaves_the_old_object(self, moto_s3_bucket, tmp_path):
        # Given
        config = Sinks.s3_config(tmp_path, "s3_file", "parquet", file_path="data.parquet")
        moto_s3_bucket.put_object(Bucket=constants.MOCK_BUCKET, Key="data.parquet", Body=b"old")

        # When / Then
        with pytest.raises(SchemaValidationError):
            Files.polars(config, Checked, apply_schema_validations=True).sink(pl.LazyFrame({"a": [-1], "b": ["x"]}))
        assert moto_s3_bucket.get_object(Bucket=constants.MOCK_BUCKET, Key="data.parquet")["Body"].read() == b"old"


class TestPrefixWrites:
    @staticmethod
    def objects(client, prefix: str) -> Dict[str, pd.DataFrame]:
        keys = [item["Key"] for item in client.list_objects_v2(Bucket=constants.MOCK_BUCKET, Prefix=prefix).get("Contents", [])]
        return {key: pd.read_parquet(io.BytesIO(client.get_object(Bucket=constants.MOCK_BUCKET, Key=key)["Body"].read())) for key in sorted(keys)}

    @staticmethod
    def rows(objects: Dict[str, pd.DataFrame]) -> List[int]:
        return sorted(row for part in objects.values() for row in part["a"].tolist())

    @staticmethod
    def partitions(objects: Dict[str, pd.DataFrame]) -> List[str]:
        return sorted({key.rsplit("/", 1)[0] for key in objects})

    @pytest.mark.unit
    @pytest.mark.parametrize("lazy", [False, True])
    def test_a_prefix_has_the_partitions_and_rows_of_the_pandas_engine(self, moto_s3_bucket, tmp_path, lazy):
        # Given
        frame = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "x"]})
        options = {"partition_cols": ["b"]}
        polars_config = Sinks.s3_config(tmp_path, "s3_path_prefix", "parquet", options, path_prefix="polars/")
        pandas_config = Sinks.s3_config(tmp_path, "s3_path_prefix", "parquet", options, path_prefix="pandas/")

        # When
        Sinks.write_or_sink(Files.polars(polars_config, Pair), frame, lazy)
        EngineSetting.reset("pandas")
        FilesIO(source_config=pandas_config, schema=polars_files.PandasPair).write(frame.to_pandas())

        # Then
        polars_objects, pandas_objects = self.objects(moto_s3_bucket, "polars/"), self.objects(moto_s3_bucket, "pandas/")
        assert [key.split("/", 1)[1] for key in self.partitions(polars_objects)] == [key.split("/", 1)[1] for key in self.partitions(pandas_objects)] == ["b=x", "b=y"]
        assert self.rows(polars_objects) == self.rows(pandas_objects) == [1, 2, 3]

    @pytest.mark.unit
    def test_an_overwrite_with_allow_delete_deletes_the_old_objects(self, moto_s3_bucket, tmp_path):
        # Given
        config = Sinks.s3_config(tmp_path, "s3_path_prefix", "parquet", {"partition_cols": ["b"], "mode": "overwrite", "allow_delete": True}, path_prefix="data/")
        moto_s3_bucket.put_object(Bucket=constants.MOCK_BUCKET, Key="data/b=old/part.parquet", Body=b"old")

        # When
        Files.polars(config, Pair).write(pl.DataFrame({"a": [1], "b": ["x"]}))

        # Then
        assert self.partitions(self.objects(moto_s3_bucket, "data/")) == ["data/b=x"]

    @pytest.mark.unit
    def test_an_overwrite_without_allow_delete_appends(self, moto_s3_bucket, tmp_path):
        # Given
        config = Sinks.s3_config(tmp_path, "s3_path_prefix", "parquet", {"partition_cols": ["b"], "mode": "overwrite"}, path_prefix="data/")
        Files.polars(config, Pair).write(pl.DataFrame({"a": [1], "b": ["old"]}))

        # When
        Files.polars(config, Pair).write(pl.DataFrame({"a": [2], "b": ["x"]}))

        # Then
        assert self.partitions(self.objects(moto_s3_bucket, "data/")) == ["data/b=old", "data/b=x"]

    @pytest.mark.unit
    @pytest.mark.usefixtures("moto_s3_bucket")
    def test_a_prefix_without_partition_columns_raises_the_error_of_the_pandas_engine(self, tmp_path):
        # Given
        config = Sinks.s3_config(tmp_path, "s3_path_prefix", "parquet", path_prefix="data/")

        # When / Then
        with pytest.raises(ValueError, match="`partition_cols` is required"):
            Files.polars(config, Pair).write(pl.DataFrame({"a": [1], "b": ["x"]}))

    @pytest.mark.unit
    def test_a_failed_validation_writes_no_object(self, moto_s3_bucket, tmp_path):
        # Given
        config = Sinks.s3_config(tmp_path, "s3_path_prefix", "parquet", {"partition_cols": ["b"]}, path_prefix="data/")

        # When / Then
        with pytest.raises(SchemaValidationError):
            Files.polars(config, Checked, apply_schema_validations=True).sink(pl.LazyFrame({"a": [-1], "b": ["x"]}))
        assert not self.objects(moto_s3_bucket, "data/")


class TestBothEnginesRaiseTheSameErrors:
    CASES = [
        ({"a": [1, -1], "b": ["x", "y"]}, SchemaValidationError),
        ({"a": ["1", "one"], "b": ["x", "y"]}, ColumnsDataTypeError),
        ({"a": [1]}, ColumnsDataTypeError),
    ]

    @staticmethod
    def error_of(action) -> type:
        try:
            action()
        except Exception as error:  # pylint: disable=broad-exception-caught
            return type(error)
        raise AssertionError("no error")

    @pytest.mark.unit
    @pytest.mark.parametrize("columns, expected", CASES)
    def test_a_write_raises_the_error_of_the_pandas_engine(self, tmp_path, columns, expected):
        # Given
        config = Files.config(tmp_path, "parquet")

        class PandasChecked(polars_files.pa.DataFrameModel):
            __dataset_name__ = "checked"

            a: polars_files.Series[int] = Field(nullable=False, gt=0)
            b: polars_files.Series[str] = Field(nullable=True)

        # When
        polars_error = self.error_of(lambda: Files.polars(config, Checked, apply_schema_validations=True).write(pl.DataFrame(columns)))
        polars_sink_error = self.error_of(lambda: Files.polars(config, Checked, apply_schema_validations=True).sink(pl.LazyFrame(columns)))
        EngineSetting.reset("pandas")
        pandas_error = self.error_of(lambda: FilesIO(source_config=config, schema=PandasChecked, apply_schema_validations=True).write(pd.DataFrame(columns)))

        # Then
        assert polars_error is polars_sink_error is pandas_error is expected
        assert not (tmp_path / "data.parquet").exists()


class TestSinkOfParts:
    @staticmethod
    def parts() -> List[Any]:
        return [
            pl.LazyFrame({"a": pl.Series([1, 2], dtype=pl.Int32), "b": ["x", None]}),
            pl.LazyFrame({"a": [3], "b": [None]}),
            pl.DataFrame({"a": pl.Series([4, 5], dtype=pl.Int64), "b": ["y", "z"], "extra": [0.5, 1.5]}),
        ]

    @staticmethod
    def whole() -> Any:
        return pl.DataFrame({"a": [1, 2, 3, 4, 5], "b": ["x", None, None, "y", "z"]})

    @pytest.mark.unit
    @pytest.mark.parametrize("options", [{}, {"row_group_size": 2}, {"index": False}])
    def test_the_parts_give_the_file_of_the_whole_frame(self, tmp_path, options):
        # Given
        whole, parts = tmp_path / "whole.parquet", tmp_path / "parts.parquet"

        # When
        Files.polars(Files.config(tmp_path, "parquet", options, file_path=str(whole)), Pair).write(self.whole())
        Files.polars(Files.config(tmp_path, "parquet", options, file_path=str(parts)), Pair).sink(self.parts())

        # Then
        assert Sinks.footer(parts) == Sinks.footer(whole)
        pd.testing.assert_frame_equal(pd.read_parquet(parts), pd.read_parquet(whole))

    @pytest.mark.unit
    def test_the_pandas_metadata_has_a_range_index_of_all_the_rows_and_the_types_of_the_model(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")

        # When
        Files.polars(config, Pair).sink(self.parts())

        # Then
        metadata = pq.read_schema(str(tmp_path / "data.parquet")).pandas_metadata
        assert metadata["index_columns"] == [{"kind": "range", "name": None, "start": 0, "stop": 5, "step": 1}]
        assert [(column["name"], column["pandas_type"]) for column in metadata["columns"]] == [("a", "int64"), ("b", "unicode")]

    @pytest.mark.unit
    @pytest.mark.parametrize("file_type, options", [("csv", {}), ("json", {"orient": "records"})])
    def test_text_parts_give_the_bytes_of_the_whole_frame(self, tmp_path, file_type, options):
        # Given
        whole, parts = tmp_path / f"whole.{file_type}", tmp_path / f"parts.{file_type}"

        # When
        Files.polars(Files.config(tmp_path, file_type, options, file_path=str(whole)), Pair).write(self.whole())
        Files.polars(Files.config(tmp_path, file_type, options, file_path=str(parts)), Pair).sink(tuple(self.parts()))

        # Then
        assert parts.read_bytes() == whole.read_bytes()

    @pytest.mark.unit
    def test_one_part_is_the_sink_of_its_frame(self, tmp_path):
        # Given
        one, frame = tmp_path / "one.parquet", tmp_path / "frame.parquet"

        # When
        Files.polars(Files.config(tmp_path, "parquet", file_path=str(one)), Pair).sink([self.whole().lazy()])
        Files.polars(Files.config(tmp_path, "parquet", file_path=str(frame)), Pair).sink(self.whole().lazy())

        # Then
        assert Sinks.footer(one) == Sinks.footer(frame)

    @pytest.mark.unit
    def test_a_failed_validation_in_a_later_part_leaves_the_old_file(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")
        (tmp_path / "data.parquet").write_bytes(b"old")
        parts = [pl.LazyFrame({"a": [1], "b": ["x"]}), pl.LazyFrame({"a": [-1], "b": ["y"]})]

        # When / Then
        with pytest.raises(SchemaValidationError):
            Files.polars(config, Checked, apply_schema_validations=True, log_schema_metrics=True).sink(parts)
        assert (tmp_path / "data.parquet").read_bytes() == b"old"

    @pytest.mark.unit
    def test_the_metrics_cover_all_the_parts(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")
        parts = [pl.LazyFrame({"a": [1], "b": ["x"]}), pl.LazyFrame({"a": [7], "b": ["y"]})]

        # When
        sunk = Sinks.logged_metrics(lambda: Files.polars(config, Checked, log_schema_metrics=True).sink(parts))
        written = Sinks.logged_metrics(lambda: Files.polars(config, Checked, log_schema_metrics=True).write(pl.DataFrame({"a": [1, 7], "b": ["x", "y"]})))

        # Then
        assert sunk == written

    @pytest.mark.unit
    def test_a_part_without_a_schema_column_raises_columns_data_type_error(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")

        # When / Then
        with pytest.raises(ColumnsDataTypeError):
            Files.polars(config, Pair).sink([pl.LazyFrame({"a": [1], "b": ["x"]}), pl.LazyFrame({"a": [2]})])
        assert not (tmp_path / "data.parquet").exists()

    @pytest.mark.unit
    def test_no_parts_raise_value_error(self, tmp_path):
        # When / Then
        with pytest.raises(ValueError, match="got no parts"):
            Files.polars(Files.config(tmp_path, "parquet"), Pair).sink([])

    @pytest.mark.unit
    def test_a_pandas_part_raises_wrong_frame_error(self, tmp_path):
        # Given
        WrongFrameError = pytest.importorskip("dynamicio.errors").WrongFrameError  # pylint: disable=invalid-name

        # When / Then
        with pytest.raises(WrongFrameError):
            Files.polars(Files.config(tmp_path, "parquet"), Pair).sink([pl.LazyFrame({"a": [1], "b": ["x"]}), pd.DataFrame({"a": [2], "b": ["y"]})])
        assert not (tmp_path / "data.parquet").exists()

    @pytest.mark.unit
    def test_parts_of_a_prefix_give_the_objects_of_the_whole_frame(self, moto_s3_bucket, tmp_path):
        # Given
        (tmp_path / "w").mkdir()
        (tmp_path / "p").mkdir()
        whole = Sinks.s3_config(tmp_path / "w", "s3_path_prefix", "parquet", {"partition_cols": ["b"]}, path_prefix="whole/")
        parts = Sinks.s3_config(tmp_path / "p", "s3_path_prefix", "parquet", {"partition_cols": ["b"]}, path_prefix="parts/")
        frame = self.whole().fill_null("n")

        # When
        Files.polars(whole, Pair).write(frame)
        Files.polars(parts, Pair).sink([frame.head(2).lazy(), frame.tail(3).lazy()])

        # Then
        keys = sorted(item["Key"] for item in moto_s3_bucket.list_objects_v2(Bucket=constants.MOCK_BUCKET)["Contents"])
        assert sorted(key.split("/", 1)[1].rsplit("/", 1)[0] for key in keys if key.startswith("parts/")) == ["b=n", "b=x", "b=y", "b=z"]
        assert len([key for key in keys if key.startswith("parts/")]) == len([key for key in keys if key.startswith("whole/")])
