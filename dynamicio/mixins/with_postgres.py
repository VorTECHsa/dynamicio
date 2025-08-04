# pylint: disable=no-member, protected-access, too-few-public-methods

"""This module provides mixins that are providing Postgres I/O support."""

import csv
import tempfile
from contextlib import contextmanager
from typing import Any, Dict, Generator, MutableMapping, Union

import pandas as pd
from magic_logger import logger
from sqlalchemy import BigInteger, Boolean, Column, Date, DateTime, Float, Integer, String, create_engine, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Query
from sqlalchemy.orm.decl_api import DeclarativeMeta
from sqlalchemy.orm.session import Session as SqlAlchemySession
from sqlalchemy.orm.session import sessionmaker

# Application Imports
from dynamicio.config.pydantic import DataframeSchema, PostgresDataEnvironment
from dynamicio.mixins import utils

Session = sessionmaker(autoflush=True)

Base = declarative_base()

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
        session.close()  # pylint: disable=no-member


class WithPostgres:
    """Handles I/O operations for Postgres.

    Args:
       - options:
           - `truncate_and_append: bool`: If True, truncates the table with CASCADE and appends new rows (faster but destructive).
                                    If False (default), safely deletes all existing rows and then appends new rows.
                                    Use truncate only if you are certain views and FK constraints can be dropped or tolerated.
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
        postgres_config = self.sources_config.postgres
        db_user = postgres_config.db_user
        db_password = postgres_config.db_password
        db_host = postgres_config.db_host
        db_port = postgres_config.db_port
        db_name = postgres_config.db_name

        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

        sql_query = self.options.pop("sql_query", None)

        assert self.sources_config.dynamicio_schema is not None, "The schema must be specified for SQL tables"
        model = self._generate_model_from_schema(self.sources_config.dynamicio_schema)

        query = Query(self._get_table_columns(model))
        if sql_query:
            query = sql_query
        logger.info(f"[postgres] Started downloading table: {self.sources_config.dynamicio_schema.name} from: {db_host}:{db_name}")
        with session_for(connection_string) as session:
            return self._read_database(session, query, **self.options)

    @staticmethod
    def _generate_model_from_schema(schema: DataframeSchema) -> DeclarativeMeta:
        json_cls_schema: Dict[str, Any] = {"tablename": schema.name, "columns": []}

        for col in schema.columns.values():
            sql_type = _type_lookup.get(col.data_type)
            if not sql_type:
                raise ValueError(f"Unsupported data_type '{col.data_type}' for column '{col.name}' in schema '{schema.name}'")
            json_cls_schema["columns"].append({"name": col.name, "type": sql_type})

        class_name = "".join(word.capitalize() or "_" for word in schema.name.split("_")) + "Model"

        class_dict = {"clsname": class_name, "__tablename__": schema.name, "__table_args__": {"extend_existing": True}}
        class_dict.update({column["name"]: Column(column["type"], primary_key=True) if idx == 0 else Column(column["type"]) for idx, column in enumerate(json_cls_schema["columns"])})

        return type(class_name, (Base,), class_dict)

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
        postgres_config = self.sources_config.postgres
        db_host = postgres_config.db_host
        db_name = postgres_config.db_name

        if isinstance(query, Query):
            query = query.with_session(session).statement

        if hasattr(query, "compile"):
            # Required for compatibility with pandas >= 2.0
            query = text(str(query.compile(compile_kwargs={"literal_binds": True})))

        logger.info(f"[postgres] Started downloading table: {self.sources_config.dynamicio_schema.name} from: {db_host}:{db_name}")
        return pd.read_sql(sql=query, con=session.get_bind(), **options)

    def _write_to_postgres(self, df: pd.DataFrame):
        """Write a dataframe to postgres based on the {file_type} of the config_io configuration.

        Args:
            df: The dataframe to be written
        """
        postgres_config = self.sources_config.postgres
        db_user = postgres_config.db_user
        db_password = postgres_config.db_password
        db_host = postgres_config.db_host
        db_port = postgres_config.db_port
        db_name = postgres_config.db_name

        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

        assert self.sources_config.dynamicio_schema is not None, "The schema must be specified for SQL tables"
        model = self._generate_model_from_schema(self.sources_config.dynamicio_schema)

        is_truncate_and_append = self.options.get("truncate_and_append", False)

        logger.info(f"[postgres] Started uploading table: {self.sources_config.dynamicio_schema.name} from: {db_host}:{db_name}")
        with session_for(connection_string) as session:
            self._write_to_database(session, model.__tablename__, df, is_truncate_and_append)  # type: ignore

    @staticmethod
    def _write_to_database(session: SqlAlchemySession, table_name: str, df: pd.DataFrame, is_truncate_and_append: bool):
        """Write a dataframe to any database provided a session with a data model and a table name.

        Args:
            session: Generated from a data model and a table name
            table_name: The name of the table to write to
            df: The dataframe to be written out
            is_truncate_and_append: If True, truncates the table with CASCADE and appends new rows (faster but destructive).
                                    If False (default), safely deletes all existing rows and then appends new rows.
                                    Use truncate only if you are certain views and FK constraints can be dropped or tolerated.
        """
        if is_truncate_and_append:
            session.execute(text(f"TRUNCATE TABLE {table_name} CASCADE;"))
        else:
            session.execute(text(f"DELETE FROM {table_name};"))

        # Upload data using PostgreSQL's COPY FROM CSV mechanism for performance
        with tempfile.NamedTemporaryFile(mode="r+") as temp_file:
            df.to_csv(temp_file, index=False, header=False, sep="\t", doublequote=False, escapechar="\\", quoting=csv.QUOTE_NONE)
            temp_file.flush()
            temp_file.seek(0)

            cur = session.connection().connection.cursor()
            cur.copy_from(temp_file, table_name, columns=df.columns, null="")

        session.commit()
