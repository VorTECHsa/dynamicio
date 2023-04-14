# pylint: disable=protected-access
"""Hdf config and resource."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Type

import pandas as pd
from pandera import SchemaModel
from pydantic import BaseModel, Field

from dynamicio import utils
from dynamicio.inject import check_injections, inject

hdf_lock = Lock()


class HdfConfig(BaseModel):
    """HDF Config."""

    path: Path
    pickle_protocol: int = Field(4, ge=0, le=5)  # Default covers python 3.4+
    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}

    def inject(self, **kwargs) -> "HdfConfig":
        """Inject variables into path. Immutable."""
        clone = deepcopy(self)
        clone.path = inject(clone.path, **kwargs)
        return clone

    def check_injections(self) -> None:
        """Check that all injections have been completed."""
        check_injections(self.path)


class HdfResource:
    """HDF Resource."""

    def __init__(self, config: HdfConfig, pa_schema: Type[SchemaModel] | None = None):
        """Initialize the HDF Resource."""
        config.check_injections()
        self.config = config
        self.pa_schema = pa_schema

    def read(self) -> pd.DataFrame:
        """Read the HDF file."""
        with hdf_lock:
            df = pd.read_hdf(self.config.path, **self.config.read_kwargs)
        if schema := self.pa_schema:
            df = schema.validate(df)
        return df

    def write(self, df: pd.DataFrame) -> None:
        """Write the HDF file."""
        if schema := self.pa_schema:
            df = schema.validate(df)  # type: ignore
        self.config.path.parent.mkdir(parents=True, exist_ok=True)

        with utils.pickle_protocol(protocol=self.config.pickle_protocol), hdf_lock:
            df.to_hdf(self.config.path, key="df", mode="w", **self.config.write_kwargs)
