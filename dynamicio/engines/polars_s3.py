"""The S3 access of the polars engine. polars and boto3 use the credentials, the region and the endpoint of the one boto3 session of the process."""

import fnmatch
import functools
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, NoReturn, Optional, Tuple
from urllib.parse import urlparse

import polars as pl
import pyarrow.parquet as pq
from botocore.exceptions import ClientError, NoCredentialsError

from dynamicio.errors import ObjectNotFoundError, S3KeyNotFoundError
from dynamicio.formats.parquet.footers import ParquetFooters
from dynamicio.mixins import with_s3
from dynamicio.transport.s3.tail import S3Tail


class PolarsS3:
    """The S3 arguments of a polars scan, the check of a key, and the keys and the parquet footers under a prefix."""

    MAX_CONCURRENCY = 32  # the threads of `s3_read_down` on the pandas engine
    # The public endpoints that boto3 makes for a region: `s3.amazonaws.com` and `s3.<region>.amazonaws.com`. polars makes the same endpoint itself
    DEFAULT_ENDPOINT = re.compile(r"^s3(\.[a-z0-9-]+)?\.amazonaws\.com$")

    @staticmethod
    def credentials() -> Tuple[Dict[str, str], Optional[int]]:
        """The credentials of the shared boto3 session and their expiry, as the ``credential_provider`` of a polars scan gets them.

        polars calls the function again after the expiry. A session without an expiry, for example with keys in the environment, gives None.

        Raises:
            NoCredentialsError: The boto3 session has no credentials.
        """
        credentials = with_s3._shared_boto3_session().get_credentials()  # pylint: disable=protected-access
        if credentials is None:
            raise NoCredentialsError()
        frozen = credentials.get_frozen_credentials()
        values = {"aws_access_key_id": frozen.access_key, "aws_secret_access_key": frozen.secret_key}
        if frozen.token:
            values["aws_session_token"] = frozen.token
        # botocore keeps the expiry of refreshable credentials (assumed roles, SSO) in a private attribute only
        expiry = getattr(credentials, "_expiry_time", None)
        return values, int(expiry.timestamp()) if expiry else None

    @staticmethod
    @functools.lru_cache(maxsize=None)
    def bucket_region(bucket: str) -> Optional[str]:
        """The region of a bucket, from the ``x-amz-bucket-region`` header of ``HeadBucket``, or None if S3 does not give it. One call per bucket.

        S3 gives the header with a 301 for a bucket in another region and with a 403, so the region does not need the ``GetBucketLocation`` permission.
        """
        try:
            response = with_s3._shared_s3_client().head_bucket(Bucket=bucket)  # pylint: disable=protected-access
        except ClientError as error:
            response = error.response
        return response.get("ResponseMetadata", {}).get("HTTPHeaders", {}).get("x-amz-bucket-region")

    @classmethod
    def arguments(cls, bucket: str) -> Dict[str, Any]:
        """The S3 arguments of a polars scan of a bucket: the credentials, the region of the bucket, and the endpoint of the shared boto3 client.

        boto3 follows the redirect of S3 to the region of a bucket, and polars does not. For the public endpoint, the region is the region of the
        bucket, and polars makes its own endpoint for it. A bucket without a region header takes the region of the client.

        Another endpoint goes to polars with the region of the client, so polars reads the S3 that boto3 reads. Examples are ``AWS_ENDPOINT_URL``,
        the ``endpoint_url`` of the AWS config, a VPC interface endpoint (``*.vpce.amazonaws.com``), and a FIPS or a dual-stack endpoint.
        """
        client = with_s3._shared_s3_client()  # pylint: disable=protected-access
        region = client.meta.region_name
        endpoint = urlparse(client.meta.endpoint_url)
        options = {}
        if cls.DEFAULT_ENDPOINT.match(endpoint.hostname or ""):
            region = cls.bucket_region(bucket) or region
        else:
            options["aws_endpoint_url"] = client.meta.endpoint_url
            if endpoint.scheme == "http":
                options["aws_allow_http"] = "true"
        if region:
            options["aws_region"] = region
        return {"credential_provider": cls.credentials, "storage_options": options}

    @staticmethod
    def raise_missing(error: ClientError, s3_path: str, file_type: str) -> NoReturn:
        """Raise the dynamicio error of the pandas engine for a missing key, and the error of S3 for other errors, for example a missing bucket.

        Raises:
            S3KeyNotFoundError: The key of a parquet object does not exist.
            ObjectNotFoundError: The key of a CSV or JSON object does not exist.
            ClientError: The error is not a missing key.
        """
        if not S3KeyNotFoundError.is_missing_key(error):
            raise error
        if file_type == "parquet":
            raise S3KeyNotFoundError(s3_path, error) from error
        raise ObjectNotFoundError(f"No object found at {s3_path}") from error

    @classmethod
    def check_key(cls, bucket: str, key: str, file_type: str) -> None:
        """Check that an S3 key exists before a scan, so that ``scan()`` raises the dynamicio error and ``missing_ok`` applies.

        A polars scan reads the object only when the frame is collected, and then raises a polars error. The check gets the first byte of the object:
        ``HeadObject`` gives a bare 404 for a missing bucket too, and ``GetObject`` tells ``NoSuchKey`` from ``NoSuchBucket``.

        Raises:
            S3KeyNotFoundError: The key of a parquet object does not exist.
            ObjectNotFoundError: The key of a CSV or JSON object does not exist.
            ClientError: Another S3 error, for example a missing bucket.
        """

        def attempt():
            try:
                with_s3._shared_s3_client().get_object(Bucket=bucket, Key=key, Range="bytes=0-0")["Body"].close()  # pylint: disable=protected-access
            except ClientError as error:
                if error.response.get("Error", {}).get("Code") != "InvalidRange":  # an empty object has no first byte
                    raise

        s3_path = f"s3://{bucket}/{key}"
        try:
            with_s3._retry_transient(attempt, s3_path)  # pylint: disable=protected-access
        except ClientError as error:
            cls.raise_missing(error, s3_path, file_type)

    @staticmethod
    def keys(source_url: str, include_pattern: Optional[str] = None) -> List[str]:
        """The keys of the objects under a prefix, in listing order, with the filter of ``s3_read_down``: no directory keys, and ``include_pattern`` on the
        key after the prefix."""
        _, key_prefix = with_s3._split_s3_url(source_url)  # pylint: disable=protected-access
        key_prefix = f"{key_prefix.rstrip('/')}/" if key_prefix else ""
        keys = []
        for key in with_s3.S3Keys.list(source_url):
            relative_key = key[len(key_prefix) :]
            if relative_key and not relative_key.endswith("/") and (not include_pattern or fnmatch.fnmatch(relative_key, include_pattern)):
                keys.append(key)
        return keys

    @staticmethod
    def footer_of(file: pq.ParquetFile) -> Tuple[pl.Schema, int]:
        """The polars schema and the number of rows of an open parquet file, from its footer."""
        footer = ParquetFooters.of(file)
        return pl.from_arrow(footer.schema.empty_table()).collect_schema(), footer.rows

    @classmethod
    def footer(cls, url: str) -> Tuple[pl.Schema, int]:
        """The polars schema and the number of rows of a parquet object, from one read of its footer: ``S3Tail`` gets the last 64 KiB in one call."""
        bucket, key = with_s3._split_s3_url(url)  # pylint: disable=protected-access
        return cls.footer_of(pq.ParquetFile(S3Tail(bucket, key)))

    @classmethod
    def footers(cls, urls: List[str]) -> List[Tuple[pl.Schema, int]]:
        """The polars schema and the number of rows of each parquet object, in the order of the URLs. The threads read the footers at the same time."""
        with ThreadPoolExecutor(max_workers=cls.MAX_CONCURRENCY) as pool:
            return list(pool.map(cls.footer, urls))
