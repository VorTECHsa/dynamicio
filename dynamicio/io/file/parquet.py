# pylint: disable=protected-access
"""Parquet config and resource."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional, Type

import pandas as pd
from pandera import SchemaModel
from pydantic import BaseModel  # type: ignore
from uhura import Readable, Writable

from dynamicio.inject import check_injections, inject
from dynamicio.serde import ParquetSerde


class ParquetResource(BaseModel):
    """PARQUET Resource."""

    path: Path
    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}
    pa_schema: Optional[Type[SchemaModel]] = None
    test_path: Optional[Path] = None

    def inject(self, **kwargs) -> "ParquetResource":
        """Inject variables into path. Immutable."""
        clone = deepcopy(self)
        clone.path = inject(clone.path, **kwargs)
        clone.test_path = inject(clone.test_path, **kwargs)
        return clone

    def check_injections(self) -> None:
        """Check that all injections have been completed."""
        check_injections(self.path)

    def read(self) -> pd.DataFrame:
        """Read the PARQUET file."""
        self.check_injections()
        df = UhuraParquetResource(path=self.path, read_kwargs=self.read_kwargs, test_path=self.test_path).read()
        df = self.validate(df)
        return df

    def write(self, df: pd.DataFrame) -> None:
        """Write the PARQUET file."""
        self.check_injections()
        df = self.validate(df)
        UhuraParquetResource(path=self.path, write_kwargs=self.write_kwargs, test_path=self.test_path).write(df)

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        if schema := self.pa_schema:
            df = schema.validate(df)  # type: ignore
        return df


from threading import Lock
from typing import Any, Callable, Dict, Optional

import pandas as pd
from uhura.serde import Serde

from dynamicio import utils
from dynamicio.inject import check_injections


class NewParquetSerde(Serde[pd.DataFrame]):
    file_extension = ".parquet"  # Used in cache key

    def __init__(
        self,
        read_kwargs: dict[str, Any],
        write_kwargs: dict[str, Any],
    ):
        self.read_kwargs = read_kwargs
        self.write_kwargs = write_kwargs

    def read_from_file(self, file: str) -> pd.DataFrame:
        check_injections(file)
        return pd.read_parquet(file, **self.read_kwargs)

    def write_to_file(self, file: str, obj: pd.DataFrame):
        check_injections(file)
        return obj.to_parquet(file, **self.write_kwargs)


class UhuraParquetResource(Readable[pd.DataFrame], Writable[pd.DataFrame]):
    def __init__(
        self,
        path: Path,
        read_kwargs: Dict[str, Any] | None = None,
        write_kwargs: Dict[str, Any] | None = None,
        test_path: Optional[Path] = None,
    ):
        self.path = path
        self.read_kwargs = read_kwargs or {}
        self.write_kwargs = write_kwargs or {}
        self.test_path = test_path

    def read(self) -> pd.DataFrame:
        """Read the PARQUET file."""
        df = pd.read_parquet(self.path, **self.read_kwargs)
        return df

    def write(self, df: pd.DataFrame) -> None:
        """Write the PARQUET file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(self.path, **self.write_kwargs)

    def cache_key(self):
        if self.test_path:
            return str(self.test_path)
        return f"file/{self.path}"

    def get_serde(self):
        return NewParquetSerde(self.read_kwargs, self.write_kwargs)
