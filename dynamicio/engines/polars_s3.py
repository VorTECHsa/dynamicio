"""The S3 access of the polars engine. polars and boto3 use the credentials, the region and the endpoint of the one boto3 session of the process."""

import fnmatch
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List, NoReturn, Optional, Tuple
from urllib.parse import urlparse

import polars as pl
from botocore.exceptions import ClientError, NoCredentialsError

from dynamicio.errors import ObjectNotFoundError, S3KeyNotFoundError
from dynamicio.mixins import with_s3


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

    @classmethod
    def arguments(cls) -> Dict[str, Any]:
        """The S3 arguments of a polars scan: the credentials, and the region and the endpoint of the shared boto3 client.

        The endpoint goes to polars when it is not the public endpoint of a region, so polars reads the S3 that boto3 reads. Examples are
        ``AWS_ENDPOINT_URL``, the ``endpoint_url`` of the AWS config, a VPC interface endpoint (``*.vpce.amazonaws.com``), and a FIPS or a dual-stack
        endpoint. For the public endpoint, polars makes its own endpoint for the region of the bucket.
        """
        client = with_s3._shared_s3_client()  # pylint: disable=protected-access
        options = {"aws_region": client.meta.region_name} if client.meta.region_name else {}
        endpoint = urlparse(client.meta.endpoint_url)
        if not cls.DEFAULT_ENDPOINT.match(endpoint.hostname or ""):
            options["aws_endpoint_url"] = client.meta.endpoint_url
            if endpoint.scheme == "http":
                options["aws_allow_http"] = "true"
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

    @classmethod
    def footers(cls, urls: List[str]) -> List[pl.Schema]:
        """The schema of the footer of each parquet object, in the order of the URLs. The threads read the footers at the same time."""
        arguments = cls.arguments()
        with ThreadPoolExecutor(max_workers=cls.MAX_CONCURRENCY) as pool:
            return list(pool.map(lambda url: pl.scan_parquet(url, **arguments).collect_schema(), urls))

    @classmethod
    def row_counts(cls, urls: List[str]) -> List[int]:
        """The number of rows of each parquet object, from its footer, in the order of the URLs."""
        arguments = cls.arguments()
        with ThreadPoolExecutor(max_workers=cls.MAX_CONCURRENCY) as pool:
            return list(pool.map(lambda url: pl.scan_parquet(url, **arguments).select(pl.len()).collect().item(), urls))
