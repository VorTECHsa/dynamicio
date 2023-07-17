# pylint: disable=protected-access
"""Parquet Resource and resource."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional, Type

import boto3  # type: ignore
import pandas as pd
from pandera import SchemaModel
from pydantic import BaseModel  # type: ignore
from uhura import Readable, Writable

from dynamicio.inject import check_injections, inject
from dynamicio.io.s3.contexts import s3_named_file_reader
from dynamicio.serde import ParquetSerde


class S3ParquetResource(BaseModel, Readable[pd.DataFrame], Writable[pd.DataFrame]):
    """S3 PARQUET Resource."""

    bucket: str
    path: Path
    force_read_to_memory: bool = False
    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}
    pa_schema: Optional[Type[SchemaModel]] = None
    test_path: Optional[Path] = None

    def inject(self, **kwargs) -> "S3ParquetResource":
        """Inject variables into path. Immutable."""
        clone = deepcopy(self)
        clone.bucket = inject(clone.bucket, **kwargs)
        clone.path = inject(clone.path, **kwargs)
        clone.test_path = inject(clone.test_path, **kwargs)
        return clone

    def check_injections(self) -> None:
        """Check that all injections have been completed."""
        check_injections(self.bucket)
        check_injections(self.path)

    @property
    def full_path(self) -> str:
        """Full path to the resource, including the bucket name."""
        return f"s3://{self.bucket}/{self.path}"

    def read(self) -> pd.DataFrame:
        """Read PARQUET from S3."""
        self.check_injections()
        df = None

        if self.force_read_to_memory:
            df = pd.read_parquet(self.full_path, **self.read_kwargs)  # type: ignore

        if df is None:
            with s3_named_file_reader(boto3.client("s3"), s3_bucket=self.bucket, s3_key=str(self.path)) as target_file:
                df = pd.read_parquet(target_file.name, **self.read_kwargs)  # type: ignore

        df = self.validate(df)
        return df

    def write(self, df: pd.DataFrame) -> None:
        """Write PARQUET to S3."""
        self.check_injections()
        df = self.validate(df)
        df.to_parquet(self.full_path, **self.write_kwargs)

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        if schema := self.pa_schema:
            df = schema.validate(df)  # type: ignore
        return df

    def cache_key(self):
        if self.test_path:
            return str(self.test_path)
        return f"s3/{self.bucket}/{self.path}"

    def get_serde(self):
        return ParquetSerde(self.validate, self.read_kwargs, self.write_kwargs)
