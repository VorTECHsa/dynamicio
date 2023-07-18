# pylint: disable=protected-access
"""Csv config and resource."""
from __future__ import annotations

from typing import Optional, ClassVar

import pandas as pd
from pydantic import BaseModel  # type: ignore
from uhura.serde import Serde

from dynamicio.serde import CsvSerde
from dynamicio.io.file.base import BaseFileResource


class CsvResource(BaseFileResource):
    """CSV Resource."""
    serde: ClassVar[Optional[Serde]] = CsvSerde

    def _concrete_resource_read(self) -> pd.DataFrame:
        """Read the CSV file."""
        return pd.read_csv(self.path, **self.read_kwargs)

    def _concrete_resource_write(self, df: pd.DataFrame) -> None:
        """Write the CSV file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.path, **self.write_kwargs)