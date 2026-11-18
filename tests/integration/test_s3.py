# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import uuid

import pandas as pd
import pytest
from botocore.exceptions import ClientError

# Application Imports
import dynamicio.errors
from tests.integration.resources import Resources
from tests.mocking.io import ReadS3HdfIO, ReadS3ParquetIO, WriteS3IO

pytestmark = [pytest.mark.integration, pytest.mark.floci]


class S3Objects:
    @staticmethod
    def keys(floci, prefix):
        pages = floci.s3_client().get_paginator("list_objects_v2").paginate(Bucket=floci.bucket, Prefix=f"{prefix}/")
        return {item["Key"] for page in pages for item in page.get("Contents", [])}

    @staticmethod
    def put(floci, *keys):
        for key in keys:
            floci.s3_client().put_object(Bucket=floci.bucket, Key=key, Body=b"old")


class TestS3FileRoundTrip:
    @pytest.mark.parametrize("file_type", ["parquet", "csv", "json", "hdf"])
    def test_a_file_written_to_s3_reads_back_the_same_frame(self, floci, prefix, tmp_path, file_type):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/data.{file_type}", file_type)
        df = pd.DataFrame({"col_1": [1, 2, 3], "col_2": ["a", None, "c"]})

        # When
        WriteS3IO(source_config=config).write(df)
        result = WriteS3IO(source_config=config).read()

        # Then
        pd.testing.assert_frame_equal(result, df)


class TestS3PathPrefixWrite:
    """The 3 `mode` values of a path prefix write on S3 that deletes objects with `DeleteObjects`."""

    @staticmethod
    def write(config, **options):
        WriteS3IO(source_config=config, partition_cols=["col_2"], **options).write(pd.DataFrame({"col_1": [1, 2], "col_2": ["a", "b"]}))

    def test_append_keeps_the_old_objects(self, floci, prefix, tmp_path):
        # Given
        old = {f"{prefix}/data/col_2=a/old.parquet", f"{prefix}/data/col_2=z/old.parquet"}
        S3Objects.put(floci, *old)

        # When
        self.write(Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/data/"))

        # Then
        keys = S3Objects.keys(floci, f"{prefix}/data")
        assert old <= keys
        assert {key.split("/")[-2] for key in keys - old} == {"col_2=a", "col_2=b"}

    @pytest.mark.parametrize("mode", ["overwrite", "overwrite_partitions"])
    def test_a_delete_mode_without_allow_delete_appends(self, floci, prefix, tmp_path, mode):
        # Given
        old = {f"{prefix}/data/col_2=a/old.parquet", f"{prefix}/data/col_2=z/old.parquet"}
        S3Objects.put(floci, *old)

        # When
        self.write(Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/data/"), mode=mode)

        # Then
        keys = S3Objects.keys(floci, f"{prefix}/data")
        assert old <= keys
        assert len(keys) == len(old) + 2

    def test_overwrite_deletes_the_old_objects_under_the_prefix_only(self, floci, prefix, tmp_path):
        # Given a sibling prefix that starts with the same characters
        old = {f"{prefix}/data/col_2=a/old.parquet", f"{prefix}/data/col_2=z/old.parquet", f"{prefix}/data/root.parquet"}
        sibling = f"{prefix}/data_2/col_2=a/old.parquet"
        S3Objects.put(floci, *old, sibling)

        # When
        self.write(Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/data/"), mode="overwrite", allow_delete=True)

        # Then
        keys = S3Objects.keys(floci, f"{prefix}/data")
        assert not old & keys
        assert {key.split("/")[-2] for key in keys} == {"col_2=a", "col_2=b"}
        assert S3Objects.keys(floci, f"{prefix}/data_2") == {sibling}

    @pytest.mark.parametrize("engine", ["pyarrow", "fastparquet"])
    def test_overwrite_partitions_deletes_the_old_objects_of_the_written_partitions_only(self, floci, prefix, tmp_path, engine):
        # Given an old object in a written partition, one in another partition and one directly under the prefix
        written, kept = f"{prefix}/data/col_2=a/old.parquet", {f"{prefix}/data/col_2=z/old.parquet", f"{prefix}/data/root.parquet"}
        S3Objects.put(floci, written, *kept)

        # When
        self.write(Resources.s3_path_prefix(tmp_path, floci.bucket, f"{prefix}/data/"), mode="overwrite_partitions", allow_delete=True, engine=engine)

        # Then
        keys = S3Objects.keys(floci, f"{prefix}/data")
        assert written not in keys
        assert kept <= keys
        new_data_files = {key for key in keys - kept if not key.split("/")[-1].startswith("_")}  # fastparquet also writes `_metadata` files under the prefix
        assert {key.split("/")[-2] for key in new_data_files} == {"col_2=a", "col_2=b"}

    def test_a_delete_mode_on_the_whole_bucket_raises_before_the_write(self, floci, tmp_path):
        # Given
        config = Resources.s3_path_prefix(tmp_path, floci.bucket, "data/")
        config.s3.path_prefix = "/"
        before = floci.s3_client().list_objects_v2(Bucket=floci.bucket).get("KeyCount")

        # When / Then
        with pytest.raises(ValueError, match="whole bucket"):
            self.write(config, mode="overwrite", allow_delete=True)
        assert floci.s3_client().list_objects_v2(Bucket=floci.bucket).get("KeyCount") == before


class TestS3MissingObjects:
    @pytest.mark.parametrize("case", [("parquet", ReadS3ParquetIO, "NoSuchKey"), ("hdf", ReadS3HdfIO, "404")], ids=["parquet", "hdf"])
    def test_a_missing_key_raises_s3_key_not_found_error(self, floci, prefix, tmp_path, case):
        # Given
        file_type, io_class, code = case
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/missing.{file_type}", file_type)

        # When / Then
        with pytest.raises(dynamicio.errors.S3KeyNotFoundError) as raised:
            io_class(source_config=config).read()
        assert raised.value.response["Error"]["Code"] == code

    def test_a_missing_key_read_by_awswrangler_raises_s3_key_not_found_error(self, floci, prefix, tmp_path):
        # Given an option that only awswrangler takes, so that awswrangler reads the file
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/missing.parquet", "parquet")

        # When / Then
        with pytest.raises(dynamicio.errors.S3KeyNotFoundError):
            ReadS3ParquetIO(source_config=config, ignore_empty=False).read()

    @pytest.mark.usefixtures("floci")
    def test_a_missing_bucket_read_by_awswrangler_raises_the_head_bucket_error(self, tmp_path):
        # Given
        config = Resources.s3_file(tmp_path, f"dynamicio-missing-{uuid.uuid4().hex[:8]}", "data.parquet", "parquet")

        # When / Then
        with pytest.raises(ClientError) as raised:
            ReadS3ParquetIO(source_config=config, ignore_empty=False, missing_ok=True).read()
        assert raised.value.operation_name == "HeadBucket"
        assert not isinstance(raised.value, dynamicio.errors.S3KeyNotFoundError)

    @pytest.mark.parametrize("file_type, io_class", [("parquet", ReadS3ParquetIO), ("hdf", ReadS3HdfIO)], ids=["parquet", "hdf"])
    def test_a_missing_key_returns_none_when_missing_ok_is_true(self, floci, prefix, tmp_path, file_type, io_class):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/missing.{file_type}", file_type)

        # When
        df = io_class(source_config=config, missing_ok=True).read()

        # Then
        assert df is None

    @pytest.mark.usefixtures("floci")
    def test_an_hdf_read_from_a_missing_bucket_raises_the_head_bucket_error_even_when_missing_ok_is_true(self, tmp_path):
        # Given S3 returns the same 404 for a missing bucket as for a missing key
        config = Resources.s3_file(tmp_path, f"dynamicio-missing-{uuid.uuid4().hex[:8]}", "data.h5", "hdf")

        # When / Then
        with pytest.raises(ClientError) as raised:
            ReadS3HdfIO(source_config=config, missing_ok=True).read()
        assert raised.value.operation_name == "HeadBucket"
        assert not isinstance(raised.value, dynamicio.errors.S3KeyNotFoundError)
