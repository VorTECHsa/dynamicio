"""The Kafka writes of the polars engine (decisions 4 and 7 of ADR 0010).

A polars frame has no index, so the option ``key_column`` gives the message keys. A write sends the frame in chunks of ``CHUNK_ROWS`` rows to the producer of
the pandas engine. Each chunk changes to pandas with the pandas dtypes of the model first, so ``key_generator``, ``document_transformer`` and the serializers get
the Python types of the pandas engine, and a ``Datetime("ns")`` value keeps its nanoseconds. The producer flushes once, after the last chunk.
"""

from typing import TYPE_CHECKING

import polars as pl

from dynamicio.engines.polars_files import PolarsFiles
from dynamicio.errors import EngineResourceError

if TYPE_CHECKING:  # pragma: no cover
    from dynamicio.core import DynamicDataIO


class PolarsKafka:
    """The ``kafka`` writes of the polars engine."""

    SOURCES = ("kafka",)
    CHUNK_ROWS = 100_000

    @staticmethod
    def check(io: "DynamicDataIO") -> None:
        """Require a ``key_column`` that is a column of the model.

        Raises:
            EngineResourceError: The resource has no ``key_column``, or the model has no column of that name.
        """
        key_column = io.options.get("key_column")
        if key_column is None:
            raise EngineResourceError(f"The polars engine needs the option 'key_column' for the Kafka resource {io.name!r}: a polars frame has no index to give the message keys")
        if not isinstance(key_column, str) or key_column not in io.schema.columns:
            raise EngineResourceError(f"The 'key_column' {key_column!r} of the Kafka resource {io.name!r} is not a column of its model: the columns are {list(io.schema.columns)}")

    @classmethod
    def write(cls, io: "DynamicDataIO", frame: pl.DataFrame) -> None:
        """Send one message for each row of a frame, as the pandas engine sends the rows of the same frame with ``set_index(key_column)``."""
        io._write_to_kafka(PolarsFiles.to_pandas_chunks(io, frame, cls.CHUNK_ROWS))  # pylint: disable=protected-access
