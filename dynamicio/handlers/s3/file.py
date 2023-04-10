"""S3 File Handlers for dynamicio."""

from copy import deepcopy
from pathlib import Path
from threading import Lock
from typing import Any, Callable, Dict

import boto3  # type: ignore
import pandas as pd  # type: ignore
from pydantic import Field

from dynamicio.base import BaseResource
from dynamicio.handlers.s3.contexts import s3_named_file_reader
from dynamicio.inject import check_injections, inject

hdf_lock = Lock()


class BaseS3Resource(BaseResource):
    """S3 Resource - kwargs dict is forwarded.

    S3 Resources try to read data directly into memory if no_disk_space is set to True.
    Otherwise, will use a temporary file.

    Attributes:
        bucket: The name of the bucket.
        path: The path to the file.
        kwargs: A dictionary of kwargs to be passed to the read/write function.
        no_disk_space: If True, will read data directly into memory. (uses s3fs + fsspec)
    """

    bucket: str
    path: Path
    kwargs: Dict[str, Any] = {}

    no_disk_space: bool = False

    _file_read_method: Callable[[Path, Any], Any]  # must be declared as staticmethod
    _file_write_method: Callable[[pd.DataFrame, Path, Any], Any]  # must be declared as staticmethod

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

    def _resource_read(self) -> pd.DataFrame:
        if self.no_disk_space:
            result = self._file_read_method(self._full_path, **self.kwargs)  # type: ignore
            if result is not None:
                return result

        with s3_named_file_reader(boto3.client("s3"), s3_bucket=self.bucket, s3_key=str(self.path)) as target_file:
            return self._file_read_method(target_file.name, **self.kwargs)  # type: ignore

    def _resource_write(self, df: pd.DataFrame) -> None:
        self._file_write_method(df, self._full_path, **self.kwargs)  # type: ignore


class S3CsvResource(BaseS3Resource):
    """S3 Resource for CSV files."""

    write_kwargs: Dict[str, Any] = Field(
        default_factory=lambda: {"index": False}
    )  # TODO: I don't like this inconsistency

    _file_read_method = staticmethod(pd.read_csv)  # type: ignore
    _file_write_method = staticmethod(pd.DataFrame.to_csv)  # type: ignore

    def _resource_write(self, df: pd.DataFrame) -> None:
        """Write to file."""
        write_kwargs = self.kwargs.copy()
        write_kwargs.update(self.write_kwargs)
        self._file_write_method(df, self._full_path, **write_kwargs)  # type: ignore


class S3JsonResource(BaseS3Resource):
    """S3 Resource for JSON files."""

    _file_read_method = staticmethod(pd.read_json)  # type: ignore
    _file_write_method = staticmethod(pd.DataFrame.to_json)  # type: ignore


class S3ParquetResource(BaseS3Resource):
    """S3 Resource for Parquet files."""

    _file_read_method = staticmethod(pd.read_parquet)  # type: ignore
    _file_write_method = staticmethod(pd.DataFrame.to_parquet)  # type: ignore
