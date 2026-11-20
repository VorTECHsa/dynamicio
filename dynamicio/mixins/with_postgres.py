# pylint: disable=no-member, protected-access, too-few-public-methods

"""This module provides mixins that are providing Postgres I/O support."""

import csv
import logging
import re
import tempfile
import threading
from contextlib import contextmanager
from typing import Any, Dict, Generator, Iterable, MutableMapping, Optional, Tuple, Union

import pandas as pd
from sqlalchemy import BigInteger, Boolean, Column, Date, DateTime, Float, Integer, MetaData, String, Table, create_engine, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Query
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.orm.session import Session as SqlAlchemySession
from sqlalchemy.orm.session import sessionmaker

# Application Imports
from dynamicio.config.pydantic import DataframeSchema, PostgresDataEnvironment
from dynamicio.errors import TableNameNotFoundError
from dynamicio.mixins import utils

logger = logging.getLogger(__name__)

Session = sessionmaker(autoflush=True)  # pylint: disable=invalid-name

Base = declarative_base()

# One declarative class per table definition, each on its own declarative base (so its own registry and metadata). Building a new
# class for every read or write piled up same-named classes in the registry of the shared `Base`, and SQLAlchemy's weakref cleanup
# of them raised a RecursionError during garbage collection. A separate base also keeps two definitions of one table (say, with
# and without an extra column) from altering each other's `__table__`. `async_read` and `async_write` call this from a thread
# pool, hence the lock.
_models: Dict[Any, DeclarativeMeta] = {}
_models_lock = threading.Lock()

_type_lookup = {
    # Booleans
    "bool": Boolean,  # native pandas (non-nullable)
    "boolean": Boolean,  # pandas nullable boolean dtype
    # Strings / objects
    "object": String(255),  # general fallback for str columns
    "string": String(255),  # newer pandas string dtype
    # Integers (nullable or not)
    "int": Integer,  # generic int fallback
    "int64": BigInteger,  # standard numpy int64
    "Int64": BigInteger,  # pandas nullable integer
    # Floats
    "float": Float,  # generic float fallback
    "float64": Float,
    # Dates and times
    "date": Date,
    "datetime": DateTime,
    "datetime64[ns]": DateTime,  # pandas datetime dtype
    "datetime64[us]": DateTime,  # the default unit of a polars `Datetime`, and the unit of a Postgres `timestamp`
    "datetime64[ms]": DateTime,
    # Optional extras
    "bigint": BigInteger,  # explicit large integer support
    "json": JSONB,  # optional if dealing with structured objects
}


@contextmanager
def session_for(connection_string: str) -> Generator[SqlAlchemySession, None, None]:
    """Connect to a database using `connection_string` and returns an active session to that connection.

    Args:
        connection_string:

    Yields:
        Active session
    """
    engine = create_engine(connection_string)
    session = Session(bind=engine)

    try:
        yield session
    finally:
        try:
            session.close()  # pylint: disable=no-member
        finally:
            engine.dispose()  # a new engine on each use: without this, its pool keeps the connection open until garbage collection


class WithPostgres:
    """Handles I/O operations for Postgres.

    Write options (passed as keyword arguments to `write`, or set under `options` in the resource definition):

    - `truncate_and_append: bool`: If True, truncates the table with CASCADE and appends new rows (faster but destructive).
      If False (default), safely deletes all existing rows and then appends new rows.
      Use truncate only if you are certain views and FK constraints can be dropped or tolerated.
    - `create_table: bool`: If True, creates the table from the schema before writing when it does not exist.
      The table has no primary key, because the schema does not say which columns form it.
    """

    sources_config: PostgresDataEnvironment
    schema: DataframeSchema
    options: MutableMapping[str, Any]

    def _read_from_postgres(self) -> pd.DataFrame:
        """Read data from postgres as a `DataFrame`.

        The configuration object is expected to have the following keys:
            - `db_user`
            - `db_password`
            - `db_host`
            - `db_port`
            - `db_name`

        Returns:
            DataFrame
        """
        connection_string, table_schema, query, options = self._postgres_read_plan()
        with session_for(connection_string) as session:
            try:
                return self._read_database(session, query, **options)
            except ProgrammingError as error:
                self._explain_missing_table(error, table_schema)
                raise

    def _postgres_connection_string(self) -> str:
        postgres_config = self.sources_config.postgres
        return f"postgresql://{postgres_config.db_user}:{postgres_config.db_password}@{postgres_config.db_host}:{postgres_config.db_port}/{postgres_config.db_name}"

    def _postgres_read_plan(self) -> Tuple[str, DataframeSchema, Union[str, Query], Dict[str, Any]]:
        """The connection string, the table schema, the query and the reader options of a read on both engines. It logs the start of the read.

        A `sql_query` option is the query, and names its own tables, so the model needs no table name. Without it, the query selects the model columns
        of the `__table_name__` table.
        """
        options = dict(self.options)
        sql_query = options.pop("sql_query", None)

        if sql_query:  # the query names its own tables, so the model needs none
            table_schema = self.schema
            query = sql_query
        else:
            table_schema = self._table_schema()
            query = Query(self._get_table_columns(self._generate_model_from_schema(table_schema)))
        postgres_config = self.sources_config.postgres
        logger.info(f"[postgres] Started downloading table: {table_schema.table_name} from: {postgres_config.db_host}:{postgres_config.db_name}")
        return self._postgres_connection_string(), table_schema, query, options

    @staticmethod
    def _explain_missing_table(error: ProgrammingError, table_schema: DataframeSchema) -> None:
        """Log which attribute named a table that does not exist, since a wrong `__table_name__` is the usual cause."""
        if table_schema.table_name and getattr(error.orig, "pgcode", None) == "42P01":  # undefined_table
            logger.error(
                f"[postgres] Table {table_schema.table_name!r} does not exist: it is the `__table_name__` of the model of dataset {table_schema.name!r}, "
                "which must be the exact name in the database"
            )

    def _table_schema(self) -> DataframeSchema:
        """Returns the schema describing the SQL table: the one compiled from the Pandera model, which names the table in `__table_name__`."""
        assert isinstance(self.schema, DataframeSchema), "The model must set __table_name__ for a Postgres resource"
        if not self.schema.table_name:  # only a `sql_query` read gets this far without one
            raise TableNameNotFoundError(self.schema.name)
        return self.schema

    @staticmethod
    def split_table_name(table_name: str) -> Tuple[Optional[str], str]:
        """The Postgres schema (None for the default one) and the table of a `__table_name__`: `sales.orders` is table `orders` of schema `sales`."""
        schema, _, table = table_name.rpartition(".")
        return schema or None, table

    @staticmethod
    def _generate_model_from_schema(schema: DataframeSchema) -> DeclarativeMeta:
        json_cls_schema: Dict[str, Any] = {"tablename": schema.table_name, "columns": []}

        for col in schema.columns.values():
            sql_type = _type_lookup.get(col.data_type)
            if not sql_type:
                raise ValueError(f"Unsupported data_type '{col.data_type}' for column '{col.name}' in schema '{schema.name}' (table '{schema.table_name}')")
            json_cls_schema["columns"].append({"name": col.name, "type": sql_type})

        key = (schema.table_name, tuple((column["name"], column["type"]) for column in json_cls_schema["columns"]))
        with _models_lock:
            if key not in _models:
                db_schema, table = WithPostgres.split_table_name(schema.table_name)
                class_name = "".join(word.capitalize() or "_" for word in re.split(r"[_.]", schema.table_name)) + "Model"

                class_dict = {"clsname": class_name, "__tablename__": table, "__table_args__": {"extend_existing": True, "schema": db_schema}}
                class_dict.update(
                    {column["name"]: Column(column["type"], primary_key=True) if idx == 0 else Column(column["type"]) for idx, column in enumerate(json_cls_schema["columns"])}
                )

                _models[key] = type(class_name, (declarative_base(),), class_dict)
            return _models[key]

    @staticmethod
    def _get_table_columns(model):
        tables_colums = []
        if model:
            for col in list(model.__table__.columns):
                tables_colums.append(getattr(model, col.name))
        return tables_colums

    @utils.allow_options(pd.read_sql)
    def _read_database(self, session: SqlAlchemySession, query: Union[str, Query], **options: Any) -> pd.DataFrame:
        """Run `query` against active `session` and returns the result as a `DataFrame`.

        Args:
            session: Active session
            query: If a `Query` object is given, it should be unbound. If a `str` is given, the
                value is used as-is.

        Returns:
            DataFrame
        """
        return pd.read_sql(sql=self._statement(session, query), con=session.get_bind(), **options)

    @staticmethod
    def _statement(session: SqlAlchemySession, query: Union[str, Query]) -> Any:
        """The SQL of a query: a `str` as is, and a `Query` as a `text` clause with its values in the SQL."""
        if isinstance(query, Query):
            query = query.with_session(session).statement

        if hasattr(query, "compile"):
            # Required for compatibility with pandas >= 2.0
            query = text(str(query.compile(compile_kwargs={"literal_binds": True})))
        return query

    def _write_to_postgres(self, df: Union[pd.DataFrame, Iterable[pd.DataFrame]]):
        """Write a dataframe to postgres based on the {file_type} of the config_io configuration.

        Args:
            df: The dataframe to be written, or its chunks: the polars engine gives chunks, and all the chunks go to the table in one transaction.
        """
        table_schema = self._table_schema()
        model = self._generate_model_from_schema(table_schema)

        postgres_config = self.sources_config.postgres
        logger.info(f"[postgres] Started uploading table: {table_schema.table_name} from: {postgres_config.db_host}:{postgres_config.db_name}")
        self._write_model(self._postgres_connection_string(), model, df, **self.options)

    @staticmethod
    @utils.allow_options(["truncate_and_append", "create_table"])
    def _write_model(connection_string: str, model: DeclarativeMeta, df: Union[pd.DataFrame, Iterable[pd.DataFrame]], truncate_and_append: bool = False, create_table: bool = False):
        if not isinstance(create_table, bool):
            raise TypeError(f"create_table must be a bool, got {create_table!r}")
        with session_for(connection_string) as session:
            if create_table:
                WithPostgres._create_table(session, model)
            WithPostgres._write_to_database(session, model.__table__, df, truncate_and_append)  # type: ignore

    @staticmethod
    def _create_table(session: SqlAlchemySession, model: DeclarativeMeta):
        """Create the table of `model` without a primary key if it does not exist, inside the write transaction.

        The generated model makes the first column a primary key, which tables holding many rows per first-column value
        violate. Its string columns are `String(255)`, a limit the schema does not state, so the new table uses unbounded
        strings. Each column keeps only its name and type: the table has no constraints, all columns are nullable with
        no defaults, and it is created in the default schema of the connection. Postgres DDL is transactional, so a
        failed write also rolls back the new table.
        """
        columns = [Column(column.name, String() if isinstance(column.type, String) else column.type) for column in model.__table__.columns]  # type: ignore
        table = Table(model.__tablename__, MetaData(), *columns, schema=model.__table__.schema)  # type: ignore
        table.create(session.connection(), checkfirst=True)

    @staticmethod
    def _write_to_database(session: SqlAlchemySession, table: Table, df: Union[pd.DataFrame, Iterable[pd.DataFrame]], is_truncate_and_append: bool):
        """Write a dataframe to any database provided a session with a data model and a table name.

        Args:
            session: Generated from a data model and a table name
            table: The table to write to (its name and schema are quoted as Postgres needs, so `Orders` stays `Orders`)
            df: The dataframe to be written out, or its chunks. Each chunk is one COPY, and the commit comes after the last chunk.
            is_truncate_and_append: If True, truncates the table with CASCADE and appends new rows (faster but destructive).
                                    If False (default), safely deletes all existing rows and then appends new rows.
                                    Use truncate only if you are certain views and FK constraints can be dropped or tolerated.
        """
        preparer = postgresql.dialect().identifier_preparer
        table_name = preparer.format_table(table)
        if is_truncate_and_append:
            session.execute(text(f"TRUNCATE TABLE {table_name} CASCADE;"))
        else:
            session.execute(text(f"DELETE FROM {table_name};"))

        for frame in [df] if isinstance(df, pd.DataFrame) else df:
            WithPostgres._copy(session, table_name, frame)

        session.commit()

    @staticmethod
    def _copy(session: SqlAlchemySession, table_name: str, df: pd.DataFrame):
        """Send the rows of a dataframe to a table with one COPY. An empty value is NULL, so an empty string and a missing value are both NULL."""
        preparer = postgresql.dialect().identifier_preparer
        # Upload data using PostgreSQL's COPY FROM CSV mechanism for performance
        # `newline=""`: a text file without it reads a `\r` in a value back as `\n`, so COPY would get a different value.
        # `lineterminator="\r\n"`: with QUOTE_NONE, Python 3.10 escapes only the characters of the line terminator, so a `\r` in a value would end the line.
        with tempfile.NamedTemporaryFile(mode="r+", encoding="utf-8", newline="") as temp_file:
            df.to_csv(temp_file, index=False, header=False, sep="\t", doublequote=False, escapechar="\\", quoting=csv.QUOTE_NONE, lineterminator="\r\n")
            temp_file.flush()
            temp_file.seek(0)

            cur = session.connection().connection.cursor()
            # `copy_from` quotes its table argument whole, so it cannot target `schema.table`: COPY names the table itself.
            columns = ", ".join(preparer.quote(str(column)) for column in df.columns)
            cur.copy_expert(f"COPY {table_name} ({columns}) FROM STDIN WITH (FORMAT text, NULL '')", temp_file)
