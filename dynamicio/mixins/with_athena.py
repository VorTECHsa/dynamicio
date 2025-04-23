"""This module provides mixins that support AWS Athena I/O."""

# pylint: disable=no-member, protected-access, too-few-public-methods

from typing import Any, MutableMapping, cast

import awswrangler as wr
import pandas as pd

# Application Imports
from dynamicio.config.pydantic import AthenaDataEnvironment
from dynamicio.mixins.utils import allow_options


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
            - query
            - s3_output
        """
        cfg = self.sources_config.athena
        raw_query = self.options.pop("query", None)
        if raw_query is None:
            raise ValueError("A 'query' must be provided for Athena reads")

        query = cast(str, raw_query)
        # Pull config, add required positional args back
        return self._run_wr_athena_query_wrapped(
            sql=query,
            s3_output=cfg.s3_output,
            **self.options,
        )

    @staticmethod
    @allow_options(wr.athena.read_sql_query)
    def _run_wr_athena_query_wrapped(sql: str, s3_output: str, **options: Any) -> pd.DataFrame:
        return wr.athena.read_sql_query(sql, s3_output, **options)

    def _write_to_athena(self, df: pd.DataFrame):
        """Athena does not support direct writing. Raise NotImplementedError."""
        raise NotImplementedError("Athena does not support direct writes. Use S3/Glue for persistence.")
