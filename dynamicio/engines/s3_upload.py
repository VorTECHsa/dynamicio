"""The S3 upload of the polars engine: a boto3 multipart upload that completes only when all its parts are uploaded (change 13 of ADR 0010)."""

import io as std_io
from typing import Union

from boto3.s3.transfer import TransferConfig

from dynamicio.mixins import with_s3


class S3Upload:
    """Upload a file or a buffer to one S3 key with the ``bucket-owner-full-control`` ACL.

    An object of up to ``PART_BYTES`` goes up in one ``PutObject``. A larger object goes up in parts of ``PART_BYTES``, with ``IN_FLIGHT`` parts at a time.
    The boto3 transfer manager completes the multipart upload when each part is uploaded, and aborts it when a part fails, so S3 keeps no part of a
    failed upload and the old object at the key does not change. A transfer that fails on a broken connection runs again from the start, as the other
    S3 transfers of dynamicio do.
    """

    PART_BYTES = 64 * 1024**2
    IN_FLIGHT = 4
    ACL = "bucket-owner-full-control"
    CONFIG = TransferConfig(multipart_threshold=PART_BYTES, multipart_chunksize=PART_BYTES, max_concurrency=IN_FLIGHT, use_threads=True)

    @classmethod
    def upload(cls, source: Union[str, std_io.BytesIO], bucket: str, key: str) -> None:
        """Upload a local file, or the content of a buffer, to ``s3://bucket/key``.

        Args:
            source: The path of a local file, or a buffer. The upload rewinds a buffer before each attempt.
            bucket: The bucket of the object.
            key: The key of the object.
        """

        def attempt():
            client = with_s3._shared_s3_client()  # pylint: disable=protected-access
            if isinstance(source, str):
                client.upload_file(source, bucket, key, ExtraArgs={"ACL": cls.ACL}, Config=cls.CONFIG)
                return
            source.seek(0)
            client.upload_fileobj(source, bucket, key, ExtraArgs={"ACL": cls.ACL}, Config=cls.CONFIG)

        with_s3._retry_transient(attempt, f"s3://{bucket}/{key}")  # pylint: disable=protected-access
