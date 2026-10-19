"""This module provides mixins that support AWS Athena I/O."""

# pylint: disable=no-member, protected-access, too-few-public-methods

from typing import Any, MutableMapping

import awswrangler as wr
import boto3
import pandas as pd
from magic_logger import logger

# Application Imports
from dynamicio.config.pydantic import AthenaDataEnvironment
from dynamicio.mixins.utils import allow_options
from dynamicio.mixins.with_s3 import _shared_boto3_session


class WithAthena:
    """Handles I/O operations for AWS Athena using AWS Wrangler.

    Note:
        The `__abstractmethods__ = frozenset()` is used to silence false positives from pylint,
        which might wrongly assume this class has abstract methods due to NotImplementedError.
        This class is *not* an abstract base class and does not use `abc.ABC`.
    """

    __abstractmethods__ = frozenset()

    sources_config: AthenaDataEnvironment
    options: MutableMapping[str, Any]

    def _read_from_athena(self) -> pd.DataFrame:
        """Reads data from AWS Athena using awswrangler with validated kwargs.

        Expected config:
            - s3_output (or the deprecated `s3_staging_dir`)
            - database (defaults to Athena's `default` database)
            - region_name, workgroup (optional)
            - query (or the `query` option)

        Returns:
            DataFrame. awswrangler returns pandas nullable dtypes (e.g. `Int64`, `string`); declare the column dtypes in the
            resource schema to have them cast on read.
        """
        cfg = self.sources_config.athena
        query = self.options.pop("query", None) or cfg.query
        if not query:
            raise ValueError("A 'query' must be provided for Athena reads")

        options = dict(self.options)
        options.setdefault("database", cfg.database)
        options.setdefault("s3_output", cfg.s3_output)
        if cfg.workgroup:
            options.setdefault("workgroup", cfg.workgroup)
        if "boto3_session" not in options:
            options["boto3_session"] = boto3.Session(region_name=cfg.region_name) if cfg.region_name else _shared_boto3_session()

        logger.info(f"[athena] Executing query on database '{options['database']}': {query}")
        return self._run_wr_athena_query_wrapped(sql=query, **options)

    @staticmethod
    @allow_options(wr.athena.read_sql_query)
    def _run_wr_athena_query_wrapped(sql: str, **options: Any) -> pd.DataFrame:
        return wr.athena.read_sql_query(sql, **options)

    def _write_to_athena(self, df: pd.DataFrame):
        """Athena does not support direct writing. Raise NotImplementedError."""
        raise NotImplementedError("Athena does not support direct writes. Use S3/Glue for persistence.")
