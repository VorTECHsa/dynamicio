# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import io
from unittest.mock import patch

import pandas as pd
import pytest

# Application Imports
from dynamicio.engines import EngineSetting
from dynamicio.errors import SchemaValidationError
from dynamicio.mixins import with_s3
from tests.integration.resources import Resources
from tests.integration.test_s3 import S3Objects

pl = pytest.importorskip("polars")
pq = pytest.importorskip("pyarrow.parquet")
polars_files = pytest.importorskip("tests.test_engines.test_polars_files")
polars_sinks = pytest.importorskip("tests.test_engines.test_polars_sinks")
s3_upload = pytest.importorskip("tests.test_transport.test_s3_upload")
Files, FilesIO, Pair, PandasPair = polars_files.Files, polars_files.FilesIO, polars_files.Pair, polars_files.PandasPair
Checked, Sinks = polars_sinks.Checked, polars_sinks.Sinks
S3Upload = pytest.importorskip("dynamicio.transport.s3.upload").S3Upload

pytestmark = [pytest.mark.integration, pytest.mark.floci]

CASES = [("parquet", None), ("parquet", {"row_group_size": 2}), ("csv", None), ("csv", {"index": False}), ("json", None), ("json", {"lines": True, "orient": "records"})]


@pytest.fixture(autouse=True)
def default_engine():
    EngineSetting.reset()
    yield
    EngineSetting.reset()


class FlociObjects:
    @staticmethod
    def body(floci, key):
        return floci.s3_client().get_object(Bucket=floci.bucket, Key=key)["Body"].read()

    @staticmethod
    def partitions(floci, prefix):
        return sorted({key[len(prefix) + 1 :].rsplit("/", 1)[0] for key in S3Objects.keys(floci, prefix)})

    @staticmethod
    def rows(floci, prefix, column):
        frames = [pd.read_parquet(io.BytesIO(FlociObjects.body(floci, key))) for key in S3Objects.keys(floci, prefix)]
        return sorted(value for frame in frames for value in frame[column].tolist())


class TestPolarsSinkOnS3:
    @pytest.mark.parametrize("file_type, options", CASES)
    def test_the_object_of_a_sink_is_the_object_of_write(self, floci, prefix, tmp_path, file_type, options):
        # Given
        (tmp_path / "w").mkdir()
        (tmp_path / "s").mkdir()
        written = Resources.s3_file(tmp_path / "w", floci.bucket, f"{prefix}/written.{file_type}", file_type, options)
        sunk = Resources.s3_file(tmp_path / "s", floci.bucket, f"{prefix}/sunk.{file_type}", file_type, options)

        # When
        Files.polars(written).write(Files.frame())
        Files.polars(sunk).sink(Files.frame().lazy())

        # Then
        (tmp_path / "written").write_bytes(FlociObjects.body(floci, f"{prefix}/written.{file_type}"))
        (tmp_path / "sunk").write_bytes(FlociObjects.body(floci, f"{prefix}/sunk.{file_type}"))
        if file_type == "parquet":
            assert Sinks.footer(tmp_path / "sunk") == Sinks.footer(tmp_path / "written")
        else:
            assert (tmp_path / "sunk").read_bytes() == (tmp_path / "written").read_bytes()
        assert Files.polars(sunk).read().equals(Files.frame())

    def test_the_pandas_engine_reads_a_sunk_object_as_its_own_frame(self, floci, prefix, tmp_path):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/data.parquet", "parquet")

        # When
        Files.polars(config).sink(Files.frame().lazy())

        # Then
        pd.testing.assert_frame_equal(Files.read_with_pandas(config), Files.pandas_frame(config))

    def test_a_failed_validation_leaves_the_old_object(self, floci, prefix, tmp_path):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/data.parquet", "parquet")
        S3Objects.put(floci, f"{prefix}/data.parquet")

        # When / Then
        with pytest.raises(SchemaValidationError):
            Files.polars(config, Checked, apply_schema_validations=True).sink(pl.LazyFrame({"a": [-1], "b": ["x"]}))
        assert FlociObjects.body(floci, f"{prefix}/data.parquet") == b"old"


class TestPolarsPrefixWritesOnS3:
    @pytest.mark.parametrize("lazy", [False, True])
    def test_a_prefix_has_the_partitions_and_rows_of_the_pandas_engine(self, floci, prefix, tmp_path, lazy):
        # Given
        frame = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "x"]})
        (tmp_path / "polars").mkdir()
        (tmp_path / "pandas").mkdir()
        options = {"partition_cols": ["b"]}
        polars_config = Resources.s3_path_prefix(tmp_path / "polars", floci.bucket, f"{prefix}/polars/", options=options)
        pandas_config = Resources.s3_path_prefix(tmp_path / "pandas", floci.bucket, f"{prefix}/pandas/", options=options)

        # When
        Sinks.write_or_sink(Files.polars(polars_config, Pair), frame, lazy)
        EngineSetting.reset("pandas")
        FilesIO(source_config=pandas_config, schema=PandasPair).write(frame.to_pandas())

        # Then
        assert FlociObjects.partitions(floci, f"{prefix}/polars") == FlociObjects.partitions(floci, f"{prefix}/pandas") == ["b=x", "b=y"]
        assert FlociObjects.rows(floci, f"{prefix}/polars", "a") == FlociObjects.rows(floci, f"{prefix}/pandas", "a") == [1, 2, 3]

    # A prefix read of 11.x does not read the `col=value` directories, so the partition column is in no object, as on the pandas engine
    def test_a_partition_column_is_not_read_back_on_either_engine(self, floci, prefix, tmp_path):
        # Given
        config = Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/data/", options={"partition_cols": ["b"]})
        Files.polars(config, Pair).write(pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "x"]}))

        # When
        with pytest.raises(ValueError, match="missing from every parquet object") as polars_error:
            Files.polars(config, Pair).read()
        EngineSetting.reset("pandas")
        with pytest.raises(ValueError, match="missing from every parquet object") as pandas_error:
            FilesIO(source_config=config, schema=PandasPair).read()

        # Then
        assert str(polars_error.value) == str(pandas_error.value)

    def test_an_overwrite_with_allow_delete_deletes_the_old_objects(self, floci, prefix, tmp_path):
        # Given
        config = Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/data/", options={"partition_cols": ["b"], "mode": "overwrite", "allow_delete": True})
        S3Objects.put(floci, f"{prefix}/data/b=old/part.parquet")

        # When
        Files.polars(config, Pair).sink(pl.LazyFrame({"a": [1], "b": ["x"]}))

        # Then
        assert FlociObjects.partitions(floci, f"{prefix}/data") == ["b=x"]

    def test_a_failed_validation_writes_no_object_and_deletes_none(self, floci, prefix, tmp_path):
        # Given
        config = Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/data/", options={"partition_cols": ["b"], "mode": "overwrite", "allow_delete": True})
        S3Objects.put(floci, f"{prefix}/data/b=old/part.parquet")

        # When / Then
        with pytest.raises(SchemaValidationError):
            Files.polars(config, Checked, apply_schema_validations=True).sink(pl.LazyFrame({"a": [-1], "b": ["x"]}))
        assert FlociObjects.partitions(floci, f"{prefix}/data") == ["b=old"]


class TestS3UploadOnS3:
    def test_a_large_object_goes_up_in_parts_with_the_acl(self, floci, prefix, tmp_path):
        # Given
        content = bytes(range(256)) * (2 * s3_upload.PART // 256 + 1)
        (tmp_path / "data").write_bytes(content)
        calls = s3_upload.Calls(with_s3._shared_s3_client())  # pylint: disable=protected-access

        # When
        with patch.object(S3Upload, "CONFIG", s3_upload.SMALL_PARTS):
            S3Upload.upload(str(tmp_path / "data"), floci.bucket, f"{prefix}/data.parquet")

        # Then
        assert calls.names.count("UploadPart") == 3
        assert FlociObjects.body(floci, f"{prefix}/data.parquet") == content

    def test_a_failed_part_aborts_the_upload_and_leaves_the_old_object(self, floci, prefix, tmp_path):
        # Given
        S3Objects.put(floci, f"{prefix}/data.parquet")
        (tmp_path / "data").write_bytes(b"\0" * (2 * s3_upload.PART + 1))
        client = with_s3._shared_s3_client()  # pylint: disable=protected-access
        s3_upload.FailedPart(client)
        calls = s3_upload.Calls(client)

        # When
        with patch.object(S3Upload, "CONFIG", s3_upload.SMALL_PARTS), pytest.raises(Exception, match="the part failed"):
            S3Upload.upload(str(tmp_path / "data"), floci.bucket, f"{prefix}/data.parquet")

        # Then
        assert "AbortMultipartUpload" in calls.names
        uploads = floci.s3_client().list_multipart_uploads(Bucket=floci.bucket, Prefix=prefix).get("Uploads", [])
        assert not uploads
        assert FlociObjects.body(floci, f"{prefix}/data.parquet") == b"old"
        with_s3._drop_shared_s3_client()  # pylint: disable=protected-access
