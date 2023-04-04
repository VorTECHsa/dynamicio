"""File handlers for dynamicio."""

from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional

import pandas as pd
from pydantic import Field

from dynamicio import utils
from dynamicio.base import BaseResource
from dynamicio.inject import inject

hdf_lock = Lock()


class BaseFileResource(BaseResource):
    """Base class for file resources."""

    _injected_path: Optional[Path] = None  # needed
    path: Path
    kwargs: Dict[str, Any] = {}

    @property
    def _final_path(self) -> Path:
        """Final path after injection."""
        if self._injected_path is not None:
            return self._injected_path
        return self.path

    def _check_injections(self) -> None:
        """Check that all injections have been completed."""
        if self._injected_path is None:
            inject(str(self.path))

    def inject(self, **kwargs) -> None:
        """Inject variables into path."""
        super().inject(**kwargs)
        path_str = str(self.path)
        path_str = inject(path_str, **kwargs)
        self._injected_path = Path(path_str)


class HdfFileResource(BaseFileResource):
    """HDF file resource."""

    pickle_protocol: Optional[int] = Field(None, ge=0, le=5)

    def _resource_read(self) -> pd.DataFrame:
        """Read from HDF file."""
        with hdf_lock:
            return pd.read_hdf(self._final_path, **self.kwargs)

    def _resource_write(self, df: pd.DataFrame) -> None:
        """Write to HDF file."""
        with utils.pickle_protocol(protocol=self.pickle_protocol), hdf_lock:
            df.to_hdf(self._final_path, key="df", mode="w", **self.kwargs)


class CsvFileResource(BaseFileResource):
    """CSV file resource."""

    def _resource_read(self) -> pd.DataFrame:
        """Read from CSV file."""
        return pd.read_csv(self._final_path, **self.kwargs)

    def _resource_write(self, df: pd.DataFrame) -> None:
        """Write to CSV file."""
        df.to_csv(self._final_path, **self.kwargs)


class JsonFileResource(BaseFileResource):
    """JSON file resource."""

    def _resource_read(self) -> pd.DataFrame:
        """Read from JSON file."""
        return pd.read_json(self._final_path, **self.kwargs)

    def _resource_write(self, df: pd.DataFrame) -> None:
        """Write to JSON file."""
        df.to_json(self._final_path, **self.kwargs)


class ParquetFileResource(BaseFileResource):
    """Parquet file resource."""

    def _resource_read(self) -> pd.DataFrame:
        """Read from Parquet file."""
        return pd.read_parquet(self._final_path, **self.kwargs)

    def _resource_write(self, df: pd.DataFrame) -> None:
        """Write to Parquet file."""
        df.to_parquet(self._final_path, **self.kwargs)
