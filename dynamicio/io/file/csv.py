"""Csv config and resource."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import pandas as pd
from pydantic import BaseModel
from uhura import Readable, Writable

from dynamicio.serde import CsvSerde


class CsvReaderWriter(BaseModel, Readable[pd.DataFrame], Writable[pd.DataFrame]):
    path: Path
    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}
    fixture_path: Path

    def read(self) -> pd.DataFrame:
        """Read the CSV file."""
        df = pd.read_csv(self.path, **self.read_kwargs)
        return df

    def write(self, df: pd.DataFrame) -> None:
        """Write the CSV file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.path, **(self.write_kwargs if self.write_kwargs else {"index": False}))

    def cache_key(self) -> str:
        return self.fixture_path

    def get_serde(self):
        return CsvSerde(read_kwargs=self.read_kwargs, write_kwargs=self.write_kwargs)
