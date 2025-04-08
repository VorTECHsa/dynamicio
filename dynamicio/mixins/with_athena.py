# pylint: disable=no-member, protected-access, too-few-public-methods

"""This module provides mixins that support AWS Athena I/O."""
import inspect
from typing import Any, MutableMapping

import pandas as pd
from magic_logger import logger
from pyathena import connect
from pyathena.connection import Connection
from pyathena.pandas.cursor import PandasCursor
from pyathena.pandas.result_set import AthenaPandasResultSet

# Application Imports
from dynamicio.config.pydantic import AthenaDataEnvironment
from dynamicio.mixins.utils import allow_options

allowed_athena_options = set(inspect.signature(AthenaPandasResultSet.__init__).parameters.keys()) - {"self"}


class WithAthena:
    """Handles I/O operations for AWS Athena."""

    sources_config: AthenaDataEnvironment
    options: MutableMapping[str, Any]

    def _read_from_athena(self) -> pd.DataFrame:
        """Reads data from AWS Athena.

        Expected config:
            - query
            - s3_staging_dir
            - region_name
        """
        cfg = self.sources_config.athena
        query = self.options.pop("query", None)

        assert query, "A 'query' must be provided for Athena read"

        conn = connect(s3_staging_dir=cfg.s3_staging_dir, region_name=cfg.region_name, cursor_class=PandasCursor)
        return self._run_query(conn, query, **self.options)

    @allow_options(allowed_athena_options)
    def _run_query(self, conn: Connection, query: str, **options: Any) -> pd.DataFrame:
        logger.info(f"[athena] Executing query: {query}")
        cursor = conn.cursor()
        cursor.execute(query)

        rows = cursor.fetchall(**options)
        columns = [col[0] for col in cursor.description]
        return pd.DataFrame(rows, columns=columns)

    def _write_to_athena(self, df: pd.DataFrame):
        """Athena does not support direct writing. Raise NotImplementedError."""
        raise NotImplementedError("Athena does not support direct writes via pandas. Consider writing to S3 or using Glue instead.")
