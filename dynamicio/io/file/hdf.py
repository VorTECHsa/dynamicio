# pylint: disable=protected-access
"""HDF config and resource."""
from __future__ import annotations
from typing import Optional, ClassVar
from threading import Lock

import pandas as pd
from pydantic import BaseModel, Field  # type: ignore
from uhura.serde import Serde

from dynamicio import utils
from dynamicio.serde import HdfSerde

from dynamicio.io.file.base import BaseFileResource

hdf_lock = Lock()


class HdfResource(BaseFileResource):
    """HDF Resource."""
    serde: ClassVar[Optional[Serde]] = HdfSerde

    pickle_protocol: int = Field(4, ge=0, le=5)  # Default covers python 3.4+

    def _concrete_resource_read(self) -> pd.DataFrame:
        """Read the HDF file."""
        with hdf_lock:
            return pd.read_hdf(self.path, **self.read_kwargs)

    def _concrete_resource_write(self, df: pd.DataFrame) -> None:
        """Write the HDF file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with utils.pickle_protocol(protocol=self.pickle_protocol), hdf_lock:
            df.to_hdf(self.path, key="df", mode="w", **self.write_kwargs)

    def get_serde(self):
        return self.serde(self.validate, self.read_kwargs, self.write_kwargs, self.pickle_protocol)
