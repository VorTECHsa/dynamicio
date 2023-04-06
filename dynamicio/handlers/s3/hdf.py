# pylint: disable=no-member, protected-access, too-few-public-methods
# flake8: noqa: I101
"""S3 HDF specific hacks."""
import uuid
from contextlib import contextmanager
from typing import IO, Generator, Optional

import pandas as pd  # type: ignore
import tables  # type: ignore


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
