# pylint: disable=no-member, protected-access, too-few-public-methods

"""This module provides mixins that support AWS Athena I/O."""

from typing import Any, MutableMapping

import pandas as pd  # type: ignore
from magic_logger import logger
from pyathena import connect  # type: ignore
from pyathena.pandas.cursor import PandasCursor

from dynamicio.config.pydantic import AthenaDataEnvironment


class WithAthena:
    """Handles I/O operations for AWS Athena."""

    sources_config: AthenaDataEnvironment
    options: MutableMapping[str, Any]

    def _read_from_athena(self) -> pd.DataFrame:
        """Reads data from AWS Athena.

        Expected config:
            - aws_access_key_id
            - aws_secret_access_key
            - s3_staging_dir
            - region_name
            - database
            - query or table_name
        """
        cfg = self.sources_config.athena

        query = self.options.pop("query", None)
        assert query, "A 'query' must be provided for Athena read"

        conn = connect(
            aws_access_key_id=cfg.aws_access_key_id,
            aws_secret_access_key=cfg.aws_secret_access_key,
            s3_staging_dir=cfg.s3_staging_dir,
            region_name=cfg.region_name,
            cursor_class=PandasCursor,
        )

        logger.info(f"[athena] Executing query: {query}")
        return conn.cursor().execute(query).fetch_df()

    def _write_to_athena(self, df: pd.DataFrame):
        """Athena does not support direct writing. Raise NotImplementedError."""
        raise NotImplementedError("Athena does not support direct writes via pandas. Consider writing to S3 or using Glue instead.")
