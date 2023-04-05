"""File handlers for dynamicio."""
from copy import deepcopy
from pathlib import Path
from threading import Lock
from typing import Any, Dict

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

    def _check_injections(self) -> None:
        """Check that all injections have been completed."""
        check_injections(str(self.path))

    def inject(self, **kwargs) -> "BaseFileResource":
        """Inject variables into path. Not in place."""
        new = deepcopy(self)
        new.path = inject(str(new.path), **kwargs)  # type: ignore
        return new


class HdfFileResource(BaseFileResource):
    """HDF file resource."""

    pickle_protocol: int = Field(4, ge=0, le=5)

    def _resource_read(self) -> pd.DataFrame:
        """Read from HDF file."""
        with hdf_lock:
            return pd.read_hdf(self.path, **self.kwargs)

    def _resource_write(self, df: pd.DataFrame) -> None:
        """Write to HDF file."""
        with utils.pickle_protocol(protocol=self.pickle_protocol), hdf_lock:
            df.to_hdf(self.path, key="df", mode="w", **self.kwargs)


class CsvFileResource(BaseFileResource):
    """CSV file resource."""

    def _resource_read(self) -> pd.DataFrame:
        """Read from CSV file."""
        return pd.read_csv(self.path, **self.kwargs)

    def _resource_write(self, df: pd.DataFrame) -> None:
        """Write to CSV file."""
        df.to_csv(self.path, **self.kwargs)


class JsonFileResource(BaseFileResource):
    """JSON file resource."""

    def _resource_read(self) -> pd.DataFrame:
        """Read from JSON file."""
        return pd.read_json(self.path, **self.kwargs)

    def _resource_write(self, df: pd.DataFrame) -> None:
        """Write to JSON file."""
        df.to_json(self.path, **self.kwargs)


class ParquetFileResource(BaseFileResource):
    """Parquet file resource."""

    def _resource_read(self) -> pd.DataFrame:
        """Read from Parquet file."""
        return pd.read_parquet(self.path, **self.kwargs)

    def _resource_write(self, df: pd.DataFrame) -> None:
        """Write to Parquet file."""
        df.to_parquet(self.path, **self.kwargs)
