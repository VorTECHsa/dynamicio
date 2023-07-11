# pylint: disable=protected-access
"""Hdf config and resource."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional, Type

import pandas as pd
from pandera import SchemaModel
from pydantic import BaseModel, Field
from uhura import Readable, Writable

from dynamicio import utils
from dynamicio.inject import check_injections, inject
from dynamicio.serde import HdfSerde

hdf_lock = Lock()


class HdfResource(BaseModel, Readable[pd.DataFrame], Writable[pd.DataFrame]):
    """HDF Resource."""

    path: Path
    pickle_protocol: int = Field(4, ge=0, le=5)  # Default covers python 3.4+
    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}
    pa_schema: Optional[Type[SchemaModel]] = None
    test_path: Optional[Path] = None

    def inject(self, **kwargs) -> "HdfResource":
        """Inject variables into path. Immutable."""
        clone = deepcopy(self)
        clone.path = inject(clone.path, **kwargs)
        return clone

    def check_injections(self) -> None:
        """Check that all injections have been completed."""
        check_injections(self.path)

    def read(self) -> pd.DataFrame:
        """Read the HDF file."""
        with hdf_lock:
            df = pd.read_hdf(self.path, **self.read_kwargs)
        df = self.validate(df)
        return df

    def write(self, df: pd.DataFrame) -> None:
        """Write the HDF file."""
        df = self.validate(df)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with utils.pickle_protocol(protocol=self.pickle_protocol), hdf_lock:
            df.to_hdf(self.path, key="df", mode="w", **self.write_kwargs)

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        if schema := self.pa_schema:
            df = schema.validate(df)
        return df

    def cache_key(self):
        if self.test_path:
            return str(self.test_path)
        return f"file/{self.path}"

    def get_serde(self):
        return HdfSerde(self.read_kwargs, self.write_kwargs, self.validate, self.pickle_protocol)
