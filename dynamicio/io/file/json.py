# pylint: disable=protected-access
"""Json config and resource."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional, Type

import pandas as pd
from pandera import SchemaModel
from pydantic import BaseModel  # type: ignore
from uhura import Readable, Writable

from dynamicio.inject import check_injections, inject
from dynamicio.serde import JsonSerde


class JsonResource(BaseModel, Readable[pd.DataFrame], Writable[pd.DataFrame]):
    """JSON Resource."""

    path: Path
    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}
    pa_schema: Optional[Type[SchemaModel]] = None
    test_path: Optional[Path] = None

    def inject(self, **kwargs) -> "JsonResource":
        """Inject variables into path. Immutable."""
        clone = deepcopy(self)
        clone.path = inject(clone.path, **kwargs)
        clone.test_path = inject(clone.test_path, **kwargs)
        return clone

    def check_injections(self) -> None:
        """Check that all injections have been completed."""
        check_injections(self.path)

    def read(self) -> pd.DataFrame:
        """Read the JSON file."""
        self.check_injections()
        df = pd.read_json(self.path, **self.read_kwargs)
        df = self.validate(df)
        return df

    def write(self, df: pd.DataFrame) -> None:
        """Write the JSON file."""
        self.check_injections()
        df = self.validate(df)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        df.to_json(self.path, **self.write_kwargs)

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        if schema := self.pa_schema:
            df = schema.validate(df)
        return df

    def cache_key(self):
        if self.test_path:
            return str(self.test_path)
        return f"file/{self.path}"

    def get_serde(self):
        return JsonSerde(self.read_kwargs, self.write_kwargs, self.validate)
