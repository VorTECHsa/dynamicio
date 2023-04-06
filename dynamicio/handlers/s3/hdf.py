# pylint: disable=no-member, protected-access, too-few-public-methods
# flake8: noqa: I101
"""S3 HDF specific hacks."""
import uuid
from contextlib import contextmanager
from typing import IO, Generator, Optional

import boto3  # type: ignore
import pandas as pd  # type: ignore
import tables  # type: ignore
from pydantic import Field

from dynamicio import utils
from dynamicio.handlers.s3.contexts import s3_named_file_reader, s3_reader, s3_writer
from dynamicio.handlers.s3.file import BaseS3Resource, hdf_lock


class S3HdfResource(BaseS3Resource):
    """S3 Resource for HDF files.

    Attributes:
        pickle_protocol: The pickle protocol to use when writing to HDF. Default is 4, which covers python 3.4+.
    """

    pickle_protocol: int = Field(4, ge=0, le=5)

    _file_read_method = staticmethod(pd.read_hdf)  # type: ignore
    _file_write_method = None  # type: ignore

    def _resource_read(self) -> pd.DataFrame:
        if self.no_disk_space:
            with s3_reader(boto3.client("s3"), s3_bucket=self.bucket, s3_key=str(self.path)) as fobj:  # type: ignore
                if (result := HdfIO().load(fobj)) is not None:  # type: ignore
                    return result
        with s3_named_file_reader(boto3.client("s3"), s3_bucket=self.bucket, s3_key=str(self.path)) as target_file:
            with hdf_lock:
                return self._file_read_method(target_file.name, **self.kwargs)  # type: ignore

    def _resource_write(self, df: pd.DataFrame) -> None:
        with s3_writer(boto3.client("s3"), s3_bucket=self.bucket, s3_key=str(self.path)) as fobj, utils.pickle_protocol(
            protocol=self.pickle_protocol
        ):
            HdfIO().save(df, fobj, **self.kwargs)  # pylint: disable=E1101


class InMemStore(pd.io.pytables.HDFStore):
    """A subclass of pandas HDFStore that does not manage the pytables File object."""

    _in_mem_table = None

    def __init__(self, path: str, table: tables.File, mode: str = "r"):
        """..."""
        self._in_mem_table = table
        super().__init__(path=path, mode=mode)  # type: ignore

    def open(self, *_args, **_kwargs):  # noqa: D102
        pd.io.pytables._tables()
        self._handle = self._in_mem_table

    def close(self, *_args, **_kwargs):  # noqa: D102
        pass

    @property
    def is_open(self):  # noqa: D102
        return self._handle is not None


class HdfIO:  # noqa: D102
    """Class providing stream support for HDF tables."""

    @contextmanager
    def create_file(self, label: str, mode: str, data: Optional[bytes] = None) -> Generator[tables.File, None, None]:
        """Create an in-memory pytables table."""
        extra_kw = {}
        if data:
            extra_kw["driver_core_image"] = data
        file_handle = tables.File(
            f"{label}_{uuid.uuid4()}.h5",
            mode,
            title=label,
            root_uep="/",
            filters=None,
            driver="H5FD_CORE",
            driver_core_backing_store=0,
            **extra_kw,
        )
        try:
            yield file_handle
        finally:
            file_handle.close()

    def load(self, fobj: IO[bytes], label: str = "unknown_file.h5") -> pd.DataFrame:
        """Load the dataframe from an file-like object."""
        with self.create_file(label, mode="r", data=fobj.read()) as file_handle:
            return pd.read_hdf(InMemStore(label, file_handle))  # type: ignore

    def save(
        self,
        df: pd.DataFrame,
        fobj: IO[bytes],
        label: str = "unknown_file.h5",
        **kwargs,
    ):
        """Load the dataframe to a file-like object."""
        if not kwargs:
            kwargs = {}
        with self.create_file(label, mode="w", data=fobj.read()) as file_handle:
            store = InMemStore(path=label, table=file_handle, mode="w")
            store.put(key="df", value=df, **kwargs)
            fobj.write(file_handle.get_file_image())
