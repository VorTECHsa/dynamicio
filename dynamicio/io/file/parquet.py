# pylint: disable=protected-access
"""PARQUET config and resource."""
from __future__ import annotations
from typing import Optional, ClassVar

import pandas as pd
from pydantic import BaseModel  # type: ignore
from uhura.serde import Serde

from dynamicio.serde import ParquetSerde

from dynamicio.io.file.base import BaseFileResource


class ParquetResource(BaseFileResource):
    """PARQUET Resource."""
    serde: ClassVar[Optional[Serde]] = ParquetSerde

    def _concrete_resource_read(self) -> pd.DataFrame:
        """Read the PARQUET file."""
        return pd.read_parquet(self.path, **self.read_kwargs)

    def _concrete_resource_write(self, df: pd.DataFrame) -> None:
        """Write the PARQUET file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(self.path, **self.write_kwargs)
