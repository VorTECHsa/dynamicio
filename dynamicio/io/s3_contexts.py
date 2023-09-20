# flake8: noqa: I101
"""Context managers for reading and writing to S3."""
import io
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Generator


@contextmanager
def s3_named_file_reader(boto3_client, s3_bucket: str, s3_key: str) -> Generator:
    """Contextmanager to abstract reading different file types in S3.

    This implementation saves the downloaded data to a temporary file.

    Args:
        s3_bucket: The S3 bucket from where to read the file.
        s3_key: The file-path to the target file to be read.

    Returns:
        The local file path from where the file can be read, once it has been downloaded there by the boto3.client.

    """
    with tempfile.NamedTemporaryFile("wb") as target_file:
        # Download the file from S3
        boto3_client.download_fileobj(s3_bucket, s3_key, target_file)
        # Yield local file path to body of `with` statement
        target_file.flush()
        yield target_file


@contextmanager
def s3_reader(boto3_client, s3_bucket: str, s3_key: Path) -> Generator[io.BytesIO, None, None]:
    """Contextmanager to abstract reading different file types in S3.

     This implementation only retains data in-memory, avoiding creating any temp files.

    Args:
        s3_bucket: The S3 bucket from where to read the file.
        s3_key: The file-path to the target file to be read.

    Returns:
        The local file path from where the file can be read, once it has been downloaded there by the boto3.client.

    """
    fobj = io.BytesIO()
    # Download the file from S3
    boto3_client.download_fileobj(s3_bucket, str(s3_key), fobj)
    # Yield the buffer
    fobj.seek(0)
    yield fobj


@contextmanager
def s3_writer(boto3_client, s3_bucket: str, s3_key: str) -> Generator[IO[bytes], None, None]:
    """Contextmanager to abstract loading different file types to S3.

    Args:
        s3_bucket: The S3 bucket to upload the file to.
        s3_key: The file-path where the target file should be uploaded to.

    Returns:
        The local file path where to actually write the file, to be read and uploaded by boto3.client.
    """
    fobj = io.BytesIO()
    yield fobj
    fobj.seek(0)
    boto3_client.upload_fileobj(fobj, s3_bucket, s3_key, ExtraArgs={"ACL": "bucket-owner-full-control"})
