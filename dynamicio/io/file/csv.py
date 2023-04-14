# pylint: disable=protected-access
"""Csv config and resource."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Type

import pandas as pd
from pandera import SchemaModel
from pydantic import BaseModel  # type: ignore

from dynamicio.inject import check_injections, inject


class CsvConfig(BaseModel):
    """CSV Config."""

    path: Path
    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {"index": False}

    def inject(self, **kwargs) -> "CsvConfig":
        """Inject variables into path. Immutable."""
        clone = deepcopy(self)
        clone.path = inject(clone.path, **kwargs)
        return clone

    def check_injections(self) -> None:
        """Check that all injections have been completed."""
        check_injections(self.path)


class CsvResource:
    """CSV Resource."""

    def __init__(self, config: CsvConfig, pa_schema: Type[SchemaModel] | None = None):
        """Initialize the CSV Resource."""
        config.check_injections()
        self.config = config
        self.pa_schema = pa_schema

    def read(self) -> pd.DataFrame:
        """Read the CSV file."""
        df = pd.read_csv(self.config.path, **self.config.read_kwargs)
        if schema := self.pa_schema:
            df = schema.validate(df)
        return df

    def write(self, df: pd.DataFrame) -> None:
        """Write the CSV file."""
        if schema := self.pa_schema:
            df = schema.validate(df)  # type: ignore
        self.config.path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.config.path, **self.config.write_kwargs)
