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
    _file_read_method: Callable[[Path, Any], Any]
    _file_write_method: Callable[[pd.DataFrame, Path, Any], Any]

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
        return self.__class__._file_read_method(self.path, **self.kwargs)  # type: ignore

    def _resource_write(self, df: pd.DataFrame) -> None:
        """Write to file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.__class__._file_write_method(df, self.path, **self.kwargs)  # type: ignore


class HdfFileResource(BaseFileResource):
    """HDF file resource."""

    pickle_protocol: int = Field(4, ge=0, le=5)  # Default covers python 3.4+

    def _resource_read(self) -> pd.DataFrame:
        """Read from HDF file."""
        with hdf_lock:
            return super()._resource_read()

    def _resource_write(self, df: pd.DataFrame) -> None:
        """Write to HDF file."""
        with utils.pickle_protocol(protocol=self.pickle_protocol), hdf_lock:
            df.to_hdf(self.path, key="df", mode="w", **self.kwargs)


class CsvFileResource(BaseFileResource):
    """CSV file resource."""

    _file_read_method = pd.read_csv  # type: ignore
    _file_write_method = pd.DataFrame.to_csv


class JsonFileResource(BaseFileResource):
    """JSON file resource."""

    _file_read_method = pd.read_json  # type: ignore
    _file_write_method = pd.DataFrame.to_json


class ParquetFileResource(BaseFileResource):
    """Parquet file resource."""

    _file_read_method = pd.read_parquet
    _file_write_method = pd.DataFrame.to_parquet