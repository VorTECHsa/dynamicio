"""Hdf config and resource."""
from __future__ import annotations

from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional

import pandas as pd
from pydantic import BaseModel
from uhura import Readable, Writable

from dynamicio import utils
from dynamicio.serde import HdfSerde
from dynamicio.serde import ValidatedSerde

hdf_lock = Lock()


class HdfReaderWriter(BaseModel, Readable[pd.DataFrame], Writable[pd.DataFrame]):
    path: Path
    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}
    test_path: Optional[Path] = None
    _cache_key: str

    def read(self) -> pd.DataFrame:
        """Read the HDF file."""
        with hdf_lock:
            df = pd.read_hdf(self.path, **self.read_kwargs)
        return df

    def write(self, df: pd.DataFrame) -> None:
        """Write the HDF file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with utils.pickle_protocol(protocol=4), hdf_lock:
            df.to_hdf(self.path, key="df", mode="w", **self.write_kwargs)

    def cache_key(self) -> str:
        return self._cache_key

    def get_serde(self):
        serde = HdfSerde(read_kwargs=self.read_kwargs, write_kwargs=self.write_kwargs)
        return ValidatedSerde(self.validate, serde)
