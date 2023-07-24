"""Parquet ReaderWriter."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

import boto3
import pandas as pd
from pydantic import BaseModel
from uhura import Readable, Writable

from dynamicio.io.s3.contexts import s3_named_file_reader
from dynamicio.serde import ParquetSerde


class S3ParquetReaderWriter(BaseModel, Readable[pd.DataFrame], Writable[pd.DataFrame]):
    bucket: str
    path: Path
    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}
    fixture_path: Path

    force_read_to_memory: bool = False

    @property
    def _s3_path(self) -> str:
        return f"s3://{self.bucket}/{self.path}"

    def read(self) -> pd.DataFrame:
        """Read PARQUET from S3."""
        if self.force_read_to_memory:
            df = pd.read_parquet(self.full_path, **self.read_kwargs)  # type: ignore
            if df is not None:
                return df

        with s3_named_file_reader(boto3.client("s3"), s3_bucket=self.bucket, s3_key=str(self.path)) as target_file:
            df = pd.read_parquet(target_file.name, **self.read_kwargs)  # type: ignore

        return df

    def write(self, df: pd.DataFrame) -> None:
        """Write PARQUET to S3."""
        df.to_parquet(self._s3_path, **self.write_kwargs)

    def cache_key(self):
        return self.fixture_path

    def get_serde(self):
        return ParquetSerde(read_kwargs=self.read_kwargs, write_kwargs=self.write_kwargs)
