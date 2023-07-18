# pylint: disable=protected-access
"""Json config and resource."""
from __future__ import annotations
from typing import Optional, ClassVar

import pandas as pd
from pydantic import BaseModel  # type: ignore
from uhura.serde import Serde

from dynamicio.serde import JsonSerde

from dynamicio.io.file.base import BaseFileResource


class JsonResource(BaseFileResource):
    """JSON Resource."""
    serde: ClassVar[Optional[Serde]] = JsonSerde

    def _concrete_resource_read(self) -> pd.DataFrame:
        """Read the JSON file."""
        return pd.read_json(self.path, **self.read_kwargs)

    def _concrete_resource_write(self, df: pd.DataFrame) -> None:
        """Write the JSON file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        df.to_json(self.path, **self.write_kwargs)