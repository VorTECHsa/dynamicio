"""The Athena reads of the polars engine (decision 8 of ADR 0010).

awswrangler runs the query as on the pandas engine, with the same options, and the engine changes its pandas frame to a polars frame. The user gets a polars frame only.
"""

from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:  # pragma: no cover
    from dynamicio.core import DynamicDataIO


class PolarsAthena:
    """The ``athena`` reads of the polars engine."""

    SOURCES = ("athena",)

    @staticmethod
    def read(io: "DynamicDataIO") -> pl.DataFrame:
        """The result of the query of a resource, as a polars frame. The engine casts it to the model after."""
        return pl.from_pandas(io._read_from_athena())  # pylint: disable=protected-access
