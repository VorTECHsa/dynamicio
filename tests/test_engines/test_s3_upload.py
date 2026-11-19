# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import io
from typing import List
from unittest.mock import patch

import pytest
from boto3.s3.transfer import TransferConfig
from botocore.exceptions import ClientError, EndpointConnectionError

from dynamicio.engines.s3_upload import S3Upload
from tests import constants

# The smallest part that S3 takes: an object of 2 parts and a bit stays small in a test
PART = 5 * 1024**2
SMALL_PARTS = TransferConfig(multipart_threshold=PART, multipart_chunksize=PART, max_concurrency=2, use_threads=True)


class Calls:
    """The names of the S3 operations that a client calls, from the ``before-call`` events of botocore."""

    def __init__(self, client):
        self.names: List[str] = []
        client.meta.events.register("before-call.s3", self.record)

    def record(self, model, **_):
        self.names.append(model.name)


class FailedPart:
    """Fail the second ``UploadPart`` of a client, as a part that S3 refuses."""

    def __init__(self, client):
        self.parts = 0
        client.meta.events.register("before-call.s3.UploadPart", self.fail)

    def fail(self, **_):
        self.parts += 1
        if self.parts == 2:
            raise ClientError({"Error": {"Code": "InternalError", "Message": "the part failed"}}, "UploadPart")


class TestS3Upload:
    @staticmethod
    def body(client, key: str) -> bytes:
        return client.get_object(Bucket=constants.MOCK_BUCKET, Key=key)["Body"].read()

    @pytest.mark.unit
    @pytest.mark.parametrize("from_file", [True, False])
    def test_a_small_object_goes_up_in_one_put_with_the_acl(self, moto_s3_bucket, tmp_path, from_file):
        # Given
        (tmp_path / "data").write_bytes(b"rows")
        source = str(tmp_path / "data") if from_file else io.BytesIO(b"rows")
        calls = Calls(moto_s3_bucket)

        # When
        S3Upload.upload(source, constants.MOCK_BUCKET, "data.parquet")

        # Then
        assert calls.names == ["PutObject"]
        assert self.body(moto_s3_bucket, "data.parquet") == b"rows"
        grants = moto_s3_bucket.get_object_acl(Bucket=constants.MOCK_BUCKET, Key="data.parquet")["Grants"]
        assert [grant["Permission"] for grant in grants] == ["FULL_CONTROL"]

    @pytest.mark.unit
    @pytest.mark.parametrize("from_file", [True, False])
    def test_a_large_object_goes_up_in_parts(self, moto_s3_bucket, tmp_path, from_file):
        # Given
        content = bytes(range(256)) * (2 * PART // 256 + 1)
        (tmp_path / "data").write_bytes(content)
        source = str(tmp_path / "data") if from_file else io.BytesIO(content)
        calls = Calls(moto_s3_bucket)

        # When
        with patch.object(S3Upload, "CONFIG", SMALL_PARTS):
            S3Upload.upload(source, constants.MOCK_BUCKET, "data.parquet")

        # Then
        assert calls.names.count("UploadPart") == 3
        assert calls.names[0] == "CreateMultipartUpload" and calls.names[-1] == "CompleteMultipartUpload"
        assert self.body(moto_s3_bucket, "data.parquet") == content

    @pytest.mark.unit
    def test_the_default_parts_are_64_mib_with_4_in_flight(self):
        # Then
        assert (S3Upload.CONFIG.multipart_threshold, S3Upload.CONFIG.multipart_chunksize, S3Upload.CONFIG.max_request_concurrency) == (64 * 1024**2, 64 * 1024**2, 4)

    @pytest.mark.unit
    def test_a_failed_part_aborts_the_upload_and_leaves_the_old_object(self, moto_s3_bucket, tmp_path):
        # Given
        moto_s3_bucket.put_object(Bucket=constants.MOCK_BUCKET, Key="data.parquet", Body=b"old")
        (tmp_path / "data").write_bytes(b"\0" * (2 * PART + 1))
        FailedPart(moto_s3_bucket)
        calls = Calls(moto_s3_bucket)

        # When
        with patch.object(S3Upload, "CONFIG", SMALL_PARTS), pytest.raises(Exception, match="the part failed"):
            S3Upload.upload(str(tmp_path / "data"), constants.MOCK_BUCKET, "data.parquet")

        # Then
        assert "AbortMultipartUpload" in calls.names and "CompleteMultipartUpload" not in calls.names
        assert not moto_s3_bucket.list_multipart_uploads(Bucket=constants.MOCK_BUCKET).get("Uploads")
        assert self.body(moto_s3_bucket, "data.parquet") == b"old"

    @pytest.mark.unit
    def test_a_broken_connection_uploads_the_buffer_again_from_the_start(self, moto_s3_bucket):
        # Given
        buffer = io.BytesIO(b"rows")
        upload = moto_s3_bucket.upload_fileobj
        attempts = []

        def fail_once(source, *args, **kwargs):
            attempts.append(source.tell())
            if len(attempts) == 1:
                source.read()
                raise EndpointConnectionError(endpoint_url="https://s3")
            return upload(source, *args, **kwargs)

        # When
        with patch.object(moto_s3_bucket, "upload_fileobj", side_effect=fail_once), patch("dynamicio.mixins.with_s3.time.sleep"):
            S3Upload.upload(buffer, constants.MOCK_BUCKET, "data.parquet")

        # Then
        assert attempts == [0, 0]
        assert self.body(moto_s3_bucket, "data.parquet") == b"rows"

    @pytest.mark.unit
    @pytest.mark.parametrize("operation, size, config", [("PutObject", 4, S3Upload.CONFIG), ("UploadPart", 2 * PART + 1, SMALL_PARTS)], ids=["put", "parts"])
    def test_a_connection_that_breaks_in_the_transfer_uploads_the_buffer_again(self, moto_s3_bucket, operation, size, config):
        # Given
        content = bytes(range(256)) * (size // 256) + b"r" * (size % 256)
        buffer, failures = io.BytesIO(content), []

        def fail_once(**_):
            if not failures:
                failures.append(operation)
                raise EndpointConnectionError(endpoint_url="https://s3")

        moto_s3_bucket.meta.events.register(f"before-call.s3.{operation}", fail_once)

        # When
        with patch.object(S3Upload, "CONFIG", config), patch("dynamicio.mixins.with_s3.time.sleep"):
            S3Upload.upload(buffer, constants.MOCK_BUCKET, "data.parquet")

        # Then
        assert failures == [operation]
        assert self.body(moto_s3_bucket, "data.parquet") == content
        assert not buffer.closed

    @pytest.mark.unit
    @pytest.mark.usefixtures("moto_s3_bucket")
    def test_an_error_that_is_not_transient_is_not_repeated(self, tmp_path):
        # Given
        (tmp_path / "data").write_bytes(b"rows")

        # When / Then
        with pytest.raises(Exception, match="NoSuchBucket|does not exist"):
            S3Upload.upload(str(tmp_path / "data"), "missing-bucket", "data.parquet")
