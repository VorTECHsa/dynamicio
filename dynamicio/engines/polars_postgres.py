"""The Postgres reads and writes of the polars engine (change 21 of ADR 0010).

A read runs the query of the pandas engine as ``pd.read_sql`` runs it, so a ``sql_query`` and its ``params`` mean what they mean on pandas.
A write sends the frame in chunks of ``CHUNK_ROWS`` rows to the COPY of the pandas engine, in one transaction. Each chunk changes to pandas with the
pandas dtypes of the model first, so the NULL rule and the text of each value are those of the pandas engine.
"""

from typing import TYPE_CHECKING, Any, Iterator

import pandas as pd
import polars as pl
from sqlalchemy.exc import ProgrammingError

from dynamicio.engines.polars_files import PolarsFiles
from dynamicio.errors import EngineResourceError
from dynamicio.mixins import utils
from dynamicio.mixins.with_postgres import WithPostgres, session_for

if TYPE_CHECKING:  # pragma: no cover
    from dynamicio.core import DynamicDataIO


class PolarsPostgres:
    """The ``postgres`` reads and writes of the polars engine."""

    SOURCES = ("postgres",)
    CHUNK_ROWS = 100_000
    # The options of `pd.read_sql` that change the frame that pandas builds. polars builds its own frame, and the cast of the model gives the types
    PANDAS_READ_OPTIONS = frozenset(utils.args_of(pd.read_sql)) - {"sql", "con", "params"}

    @classmethod
    def check(cls, io: "DynamicDataIO") -> None:
        """Refuse a ``pd.read_sql`` option that has no polars equivalent.

        Raises:
            EngineResourceError: An option of the resource is one of ``PANDAS_READ_OPTIONS``.
        """
        refused = sorted(cls.PANDAS_READ_OPTIONS & set(io.options))
        if refused:
            raise EngineResourceError(f"The polars engine has no Postgres option {', '.join(map(repr, refused))} of {io.name!r}: the cast of the model gives the column types")

    @classmethod
    def read(cls, io: "DynamicDataIO") -> pl.DataFrame:
        """The rows of the query of a resource, as a polars frame. The engine casts it to the model after.

        Raises:
            ProgrammingError: The query fails, for example because the table does not exist.
        """
        connection_string, table_schema, query, options = io._postgres_read_plan()  # pylint: disable=protected-access
        params = options.get("params")
        with session_for(connection_string) as session:
            try:
                result = cls.execute(session.connection(), WithPostgres._statement(session, query), params)  # pylint: disable=protected-access
            except ProgrammingError as error:
                io._explain_missing_table(error, table_schema)  # pylint: disable=protected-access
                raise
            columns = list(result.keys())
            return pl.DataFrame(result.fetchall(), schema=columns, orient="row", infer_schema_length=None)

    @staticmethod
    def execute(connection: Any, statement: Any, params: Any) -> Any:
        """Run a statement as ``pd.read_sql`` runs it: a ``str`` goes to the driver with the ``params`` in the style of the driver."""
        args = () if params is None else (params,)
        if isinstance(statement, str):
            return connection.exec_driver_sql(statement, *args)
        return connection.execute(statement, *args)

    @classmethod
    def write(cls, io: "DynamicDataIO", frame: pl.DataFrame) -> None:
        """Replace the rows of the table with the rows of a frame, in one transaction, as the pandas engine does."""
        io._write_to_postgres(cls.chunks(io, frame))  # pylint: disable=protected-access

    @classmethod
    def chunks(cls, io: "DynamicDataIO", frame: pl.DataFrame) -> Iterator[pd.DataFrame]:
        """The pandas frames of the chunks of a frame. A frame without rows gives one chunk without rows, so the COPY still names the columns."""
        return PolarsFiles.to_pandas_chunks(io, frame, cls.CHUNK_ROWS)
