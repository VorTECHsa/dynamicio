"""File resource."""
from __future__ import annotations

from copy import deepcopy
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Type, Union

import pandas as pd
from pandera import SchemaModel
from pydantic import BaseModel, Field  # type: ignore

from dynamicio.io.file.csv import CsvReaderWriter
from dynamicio.io.file.hdf import HdfReaderWriter
from dynamicio.io.file.json import JsonReaderWriter
from dynamicio.io.file.parquet import ParquetReaderWriter


class FileType(Enum):  # todo: check str stuff work or separate map needed...
    PARQUET = ParquetReaderWriter
    CSV = CsvReaderWriter
    JSON = JsonReaderWriter
    HDF = HdfReaderWriter


class FileResource(BaseModel):
    path: Path
    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}
    pa_schema: Optional[Type[SchemaModel]] = None
    test_path: Optional[Path] = None
    file_type: Union[FileType, str, None] = None

    def inject(self, **kwargs) -> "FileResource":
        """Inject variables into path. Immutable function."""
        clone = deepcopy(self)
        clone.path = str(clone.path).format(**kwargs)
        clone.test_path = str(clone.test_path).format(**kwargs)
        return clone

    def read(self) -> pd.DataFrame:
        """Read the PARQUET file."""
        df = self._build_reader_writer().read()
        df = self.validate(df)
        return df

    def write(self, df: pd.DataFrame) -> None:
        """Write the PARQUET file."""
        df = self.validate(df)
        self._build_reader_writer().write(df)

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        if schema := self.pa_schema:
            df = schema.validate(df)  # type: ignore
        return df

    def _get_reader_writer_class(self) -> FileType:
        if isinstance(self.file_type, FileType):
            return self.file_type
        if self.file_type is None:
            suffix = self.path.suffix
            suffix = suffix[1:] if suffix.startswith(".") else suffix
        elif isinstance(self.file_type, str):
            suffix = self.file_type
        else:
            raise ValueError("file_type must be FileType, str or None.")

        reader_writer = {
            "parquet": FileType.PARQUET,
            "csv": FileType.CSV,
            "json": FileType.JSON,
            "hdf": FileType.HDF,
        }.get(suffix, None)
        if reader_writer is None:
            raise ValueError(f"Unknown file type {suffix}")
        return reader_writer

    def _build_reader_writer(self):
        reader_writer_class: FileType = self._get_reader_writer_class()
        return reader_writer_class.value(
            path=self.path, read_kwargs=self.read_kwargs, write_kwargs=self.write_kwargs, test_path=self.test_path
        )

    class Config:
        validate_assignment = True
