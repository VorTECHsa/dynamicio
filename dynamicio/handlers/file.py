# pylint: disable=protected-access
"""File handlers for dynamicio."""
from copy import deepcopy
from pathlib import Path
from threading import Lock
from typing import Any, Callable, Dict

import pandas as pd
from pydantic import Field

from dynamicio import utils
from dynamicio.base import BaseResource
from dynamicio.inject import check_injections, inject

hdf_lock = Lock()


class BaseFileResource(BaseResource):
    """Base class for file resources."""

    path: Path
    kwargs: Dict[str, Any] = {}
    _file_read_method: Callable[[Path, Any], Any]  # must be declared as staticmethod
    _file_write_method: Callable[[pd.DataFrame, Path, Any], Any]  # must be declared as staticmethod

    def inject(self, **kwargs) -> "BaseFileResource":
        """Inject variables into path. Immutable."""
        new = deepcopy(self)
        new.path = inject(str(new.path), **kwargs)  # type: ignore
        return new

    def _check_injections(self) -> None:
        """Check that all injections have been completed."""
        check_injections(str(self.path))

    def _resource_read(self) -> pd.DataFrame:
        """Read from file."""
        return self._file_read_method(self.path, **self.kwargs)  # type: ignore

    def _resource_write(self, df: pd.DataFrame) -> None:
        """Write to file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file_write_method(df, self.path, **self.kwargs)  # type: ignore


class CsvFileResource(BaseFileResource):
    """CSV file resource."""

    _file_read_method = staticmethod(pd.read_csv)  # type: ignore
    _file_write_method = staticmethod(pd.DataFrame.to_csv)  # type: ignore


class JsonFileResource(BaseFileResource):
    """JSON file resource."""

    _file_read_method = staticmethod(pd.read_json)  # type: ignore
    _file_write_method = staticmethod(pd.DataFrame.to_json)  # type: ignore


class ParquetFileResource(BaseFileResource):
    """Parquet file resource."""

    _file_read_method = staticmethod(pd.read_parquet)  # type: ignore
    _file_write_method = staticmethod(pd.DataFrame.to_parquet)  # type: ignore


class HdfFileResource(BaseFileResource):
    """HDF file resource."""

    _file_read_method = staticmethod(pd.read_hdf)  # type: ignore
    _file_write_method = staticmethod(pd.DataFrame.to_hdf)  # type: ignore

    pickle_protocol: int = Field(4, ge=0, le=5)  # Default covers python 3.4+

    def _resource_read(self) -> pd.DataFrame:
        """Read from HDF file."""
        with hdf_lock:
            return super()._resource_read()

    def _resource_write(self, df: pd.DataFrame) -> None:
        """Write to HDF file."""
        with utils.pickle_protocol(protocol=self.pickle_protocol), hdf_lock:
            self._file_write_method(df, self.path, key="df", mode="w", **self.kwargs)  # type: ignore
