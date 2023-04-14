# pylint: disable=protected-access
"""Parquet config and resource."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Type

import boto3  # type: ignore
import pandas as pd
from pandera import SchemaModel
from pydantic import BaseModel  # type: ignore

from dynamicio.inject import check_injections, inject
from dynamicio.io.s3.contexts import s3_named_file_reader


class S3ParquetConfig(BaseModel):
    """PARQUET Config."""

    bucket: str
    path: Path
    force_read_to_memory: bool = False
    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}

    def inject(self, **kwargs) -> "S3ParquetConfig":
        """Inject variables into path. Immutable."""
        clone = deepcopy(self)
        clone.bucket = inject(clone.bucket, **kwargs)
        clone.path = inject(clone.path, **kwargs)
        return clone

    def check_injections(self) -> None:
        """Check that all injections have been completed."""
        check_injections(self.bucket)
        check_injections(self.path)

    @property
    def full_path(self) -> str:
        """Full path to the resource, including the bucket name."""
        return f"s3://{self.bucket}/{self.path}"


class S3ParquetResource:
    """PARQUET Resource."""

    def __init__(self, config: S3ParquetConfig, pa_schema: Type[SchemaModel] | None = None):
        """Initialize the PARQUET Resource."""
        config.check_injections()
        self.config = config
        self.pa_schema = pa_schema

    def read(self) -> pd.DataFrame:
        """Read PARQUET from S3."""
        df = None

        if self.config.force_read_to_memory:
            df = pd.read_parquet(self.config.full_path, **self.config.read_kwargs)  # type: ignore

        if df is None:
            with s3_named_file_reader(
                boto3.client("s3"), s3_bucket=self.config.bucket, s3_key=str(self.config.path)
            ) as target_file:
                df = pd.read_parquet(target_file.name, **self.config.read_kwargs)  # type: ignore

        if schema := self.pa_schema:
            df = schema.validate(df)  # type: ignore

        return df

    def write(self, df: pd.DataFrame) -> None:
        """Write PARQUET to S3."""
        if schema := self.pa_schema:
            df = schema.validate(df)  # type: ignore
        df.to_parquet(self.config.full_path, **self.config.write_kwargs)
