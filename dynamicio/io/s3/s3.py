"""File resource."""
from __future__ import annotations

from copy import deepcopy
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Optional, Type, Union

import pandas as pd
from pandera import SchemaModel
from pydantic import BaseModel, Field  # type: ignore

from dynamicio.io.s3.csv import S3CsvReaderWriter
from dynamicio.io.s3.hdf import S3HdfReaderWriter
from dynamicio.io.s3.json import S3JsonReaderWriter
from dynamicio.io.s3.parquet import S3ParquetReaderWriter


class S3FileType(Enum):
    PARQUET = S3ParquetReaderWriter
    CSV = S3CsvReaderWriter
    JSON = S3JsonReaderWriter
    HDF = S3HdfReaderWriter


class S3Resource(BaseModel):
    bucket: str
    path: Path
    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}
    pa_schema: Optional[Type[SchemaModel]] = None
    test_path: Optional[Path] = None
    file_type: Union[S3FileType, str, None] = None

    def inject(self, **kwargs) -> "S3Resource":
        """Inject variables into path. Immutable function."""
        clone = deepcopy(self)
        clone.bucket = str(clone.bucket).format(**kwargs)
        clone.path = str(clone.path).format(**kwargs)
        if clone.test_path is not None:
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

    def _get_reader_writer_class(self) -> S3FileType:
        if isinstance(self.file_type, S3FileType):
            return self.file_type
        if self.file_type is None:
            suffix = self.path.suffix
            suffix = suffix[1:] if suffix.startswith(".") else suffix
        elif isinstance(self.file_type, str):
            suffix = self.file_type
        else:
            raise ValueError("file_type must be S3FileType, str or None.")

        reader_writer = {
            "parquet": S3FileType.PARQUET,
            "csv": S3FileType.CSV,
            "json": S3FileType.JSON,
            "hdf": S3FileType.HDF,
        }.get(suffix, None)
        if reader_writer is None:
            raise ValueError(f"Unknown file type {suffix}")
        return reader_writer

    @property
    def fixture_path(self) -> Path:
        """Return test path."""
        return self.test_path or Path("s3") / self.bucket / self.path

    def _build_reader_writer(self):
        reader_writer_class: S3FileType = self._get_reader_writer_class()
        return reader_writer_class.value(
            bucket=self.bucket,
            path=self.path,
            read_kwargs=self.read_kwargs,
            write_kwargs=self.write_kwargs,
            test_path=self.test_path,
            fixture_path=self.fixture_path,
        )

    class Config:
        validate_assignment = True
