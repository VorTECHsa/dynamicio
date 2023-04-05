"""S3 File Handlers for dynamicio."""

from copy import deepcopy
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional

import boto3  # type: ignore
import pandas as pd  # type: ignore
from pydantic import Field

from dynamicio import utils
from dynamicio.base import BaseResource
from dynamicio.handlers.s3.contexts import s3_named_file_reader, s3_reader, s3_writer
from dynamicio.handlers.s3.hdf import HdfIO
from dynamicio.inject import check_injections, inject

hdf_lock = Lock()


class BaseS3Resource(BaseResource):
    """S3 Resource - kwargs dict is forwarded.

    S3 Resources try to read data directly into memory if no_disk_space is set to True.
    Otherwise, will use a temporary file.
    """

    bucket: str
    path: Path
    kwargs: Dict[str, Any] = {}

    no_disk_space: bool = Field(
        False,
        description="enable this to use s3fs + fsspec to read data directly into memory",
    )
    pickle_protocol: Optional[int] = Field(None, ge=0, le=5)

    @property
    def path_str(self):
        """Path string, path is often needed as a string and this provides nicer syntax than str(resource.path)."""
        return str(self.path)

    def inject(self, **kwargs) -> "BaseS3Resource":
        """Inject variables into path. Not in place."""
        new = deepcopy(self)
        new.path = inject(str(new.path), **kwargs)  # type: ignore
        new.bucket = inject(new.bucket, **kwargs)
        return new

    @property
    def _full_path(self) -> str:
        """Full path to the resource, including the bucket name."""
        return f"s3://{self.bucket}/{self.path}"

    def _check_injections(self) -> None:
        """Check that all injections have been completed."""
        check_injections(self.bucket)
        check_injections(str(self.path))


class S3HdfResource(BaseS3Resource):
    """S3 Resource for HDF files."""

    pickle_protocol: int = Field(4, ge=0, le=5)  # Default covers python 3.4+

    def _resource_read(self) -> pd.DataFrame:
        if self.no_disk_space:
            with s3_reader(boto3.client("s3"), s3_bucket=self.bucket, s3_key=self.path_str) as fobj:
                if (result := HdfIO().load(fobj)) is not None:  # type: ignore
                    return result
        with s3_named_file_reader(boto3.client("s3"), s3_bucket=self.bucket, s3_key=self.path_str) as target_file:
            with hdf_lock:
                return pd.read_hdf(target_file.name, **self.kwargs)

    def _resource_write(self, df: pd.DataFrame) -> None:
        with s3_writer(boto3.client("s3"), s3_bucket=self.bucket, s3_key=self.path_str) as fobj, utils.pickle_protocol(
            protocol=self.pickle_protocol
        ):
            HdfIO().save(df, fobj, **self.kwargs)  # pylint: disable=E1101


class S3CsvResource(BaseS3Resource):
    """S3 Resource for CSV files."""

    def _resource_read(self) -> pd.DataFrame:
        if self.no_disk_space:
            if (result := pd.read_csv(self._full_path, **self.kwargs)) is not None:
                return result

        with s3_named_file_reader(boto3.client("s3"), s3_bucket=self.bucket, s3_key=self.path_str) as target_file:
            return pd.read_csv(target_file.name, **self.kwargs)

    def _resource_write(self, df: pd.DataFrame) -> None:
        df.to_csv(self._full_path, **self.kwargs)


class S3JsonResource(BaseS3Resource):
    """S3 Resource for JSON files."""

    def _resource_read(self) -> pd.DataFrame:
        if self.no_disk_space:
            if (result := pd.read_json(self._full_path, **self.kwargs)) is not None:
                return result

        with s3_named_file_reader(boto3.client("s3"), s3_bucket=self.bucket, s3_key=self.path_str) as target_file:
            return pd.read_json(target_file.name, **self.kwargs)

    def _resource_write(self, df: pd.DataFrame) -> None:
        df.to_json(self._full_path, **self.kwargs)


class S3ParquetResource(BaseS3Resource):
    """S3 Resource for Parquet files."""

    def _resource_read(self) -> pd.DataFrame:
        if self.no_disk_space:
            if (result := pd.read_parquet(self._full_path, **self.kwargs)) is not None:
                return result

        with s3_named_file_reader(boto3.client("s3"), s3_bucket=self.bucket, s3_key=self.path_str) as target_file:
            return pd.read_parquet(target_file.name, **self.kwargs)

    def _resource_write(self, df: pd.DataFrame) -> None:
        df.to_parquet(self._full_path, **self.kwargs)
