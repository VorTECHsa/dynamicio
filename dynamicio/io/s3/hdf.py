# pylint: disable=no-member, protected-access, too-few-public-methods
# flake8: noqa: I101
"""S3 HDF specific hacks."""
from __future__ import annotations

import uuid
from contextlib import contextmanager
from copy import deepcopy
from pathlib import Path
from threading import Lock
from typing import IO, Any, Dict, Generator, Optional, Type

import boto3  # type: ignore
import pandas as pd  # type: ignore
import tables  # type: ignore
from pandera import SchemaModel
from pydantic import BaseModel, Field

from dynamicio import utils
from dynamicio.inject import check_injections, inject
from dynamicio.io.s3.contexts import s3_named_file_reader, s3_reader, s3_writer

hdf_lock = Lock()


class S3HdfConfig(BaseModel):
    """HDF Config."""

    bucket: str
    path: Path
    pickle_protocol: int = Field(4, ge=0, le=5)  # Default covers python 3.4+
    force_read_to_memory: bool = False
    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}

    def inject(self, **kwargs) -> "S3HdfConfig":
        """Inject variables into path. Immutable."""
        clone = deepcopy(self)
        clone.bucket = inject(clone.bucket, **kwargs)
        clone.path = inject(clone.path, **kwargs)
        return clone

    def check_injections(self) -> None:
        """Check that all injections have been completed."""
        check_injections(self.bucket)
        check_injections(self.path)

    @property
    def full_path(self) -> str:
        """Full path to the resource, including the bucket name."""
        return f"s3://{self.bucket}/{self.path}"


class S3HdfResource:
    """HDF Resource."""

    def __init__(self, config: S3HdfConfig, pa_schema: Type[SchemaModel] | None = None):
        """Initialize the HDF Resource."""
        config.check_injections()
        self.config = config
        self.pa_schema = pa_schema

    def read(self) -> pd.DataFrame:
        """Read HDF from S3."""
        df = None
        if self.config.force_read_to_memory:
            with s3_reader(boto3.client("s3"), s3_bucket=self.config.bucket, s3_key=str(self.config.path)) as fobj:  # type: ignore
                df = HdfIO().load(fobj)
        if df is None:
            with s3_named_file_reader(
                boto3.client("s3"), s3_bucket=self.config.bucket, s3_key=str(self.config.path)
            ) as target_file:
                with hdf_lock:
                    df = pd.read_hdf(target_file.name, **self.config.read_kwargs)  # type: ignore

        if schema := self.pa_schema:
            df = schema.validate(df)  # type: ignore

        return df

    def write(self, df: pd.DataFrame) -> None:
        """Write HDF to s3."""
        if schema := self.pa_schema:
            df = schema.validate(df)  # type: ignore

        with s3_writer(
            boto3.client("s3"), s3_bucket=self.config.bucket, s3_key=str(self.config.path)
        ) as fobj, utils.pickle_protocol(protocol=self.config.pickle_protocol):
            HdfIO().save(df, fobj, **self.config.write_kwargs)


class InMemStore(pd.io.pytables.HDFStore):
    """A subclass of pandas HDFStore that does not manage the pytables File object."""

    _in_mem_table = None

    def __init__(self, path: str, table: tables.File, mode: str = "r"):
        """Initialize the store."""
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
        """Load the dataframe from a file-like object."""
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
