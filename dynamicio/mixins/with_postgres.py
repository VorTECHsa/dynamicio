# pylint: disable=no-member, protected-access, too-few-public-methods

"""This module provides mixins that are providing Postgres I/O support."""

import csv
import tempfile
from contextlib import contextmanager
from typing import Any, Dict, Generator, MutableMapping, Union

import pandas as pd  # type: ignore
from magic_logger import logger
from sqlalchemy import BigInteger, Boolean, Column, create_engine, Date, DateTime, Float, Integer, String  # type: ignore
from sqlalchemy.ext.declarative import declarative_base  # type: ignore
from sqlalchemy.orm import Query  # type: ignore
from sqlalchemy.orm.decl_api import DeclarativeMeta  # type: ignore
from sqlalchemy.orm.session import Session as SqlAlchemySession  # type: ignore
from sqlalchemy.orm.session import sessionmaker  # type: ignore

from dynamicio.config.pydantic import DataframeSchema, PostgresDataEnvironment
from dynamicio.mixins import utils

Session = sessionmaker(autoflush=True)

Base = declarative_base()
_type_lookup = {
    "bool": Boolean,
    "boolean": Boolean,
    "object": String(64),
    "int64": Integer,
    "float64": Float,
    "int": Integer,
    "date": Date,
    "datetime64[ns]": DateTime,
    "bigint": BigInteger,
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
           - `truncate_and_append: bool`: If set to `True`, truncates the table and then appends the new rows. Otherwise, it drops the table and recreates it with the new rows.
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
            if sql_type:
                json_cls_schema["columns"].append({"name": col.name, "type": sql_type})

        class_name = "".join(word.capitalize() or "_" for word in schema.name.split("_")) + "Model"

        class_dict = {"clsname": class_name, "__tablename__": schema.name, "__table_args__": {"extend_existing": True}}
        class_dict.update({column["name"]: Column(column["type"], primary_key=True) if idx == 0 else Column(column["type"]) for idx, column in enumerate(json_cls_schema["columns"])})

        generated_model = type(class_name, (Base,), class_dict)
        return generated_model

    @staticmethod
    def _get_table_columns(model):
        tables_colums = []
        if model:
            for col in list(model.__table__.columns):
                tables_colums.append(getattr(model, col.name))
        return tables_colums

    @staticmethod
    @utils.allow_options(pd.read_sql)
    def _read_database(session: SqlAlchemySession, query: Union[str, Query], **options: Any) -> pd.DataFrame:
        """Run `query` against active `session` and returns the result as a `DataFrame`.

        Args:
            session: Active session
            query: If a `Query` object is given, it should be unbound. If a `str` is given, the
                value is used as-is.

        Returns:
            DataFrame
        """
        if isinstance(query, Query):
            query = query.with_session(session).statement
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

        logger.info(f"[postgres] Started downloading table: {self.sources_config.dynamicio_schema.name} from: {db_host}:{db_name}")
        with session_for(connection_string) as session:
            self._write_to_database(session, model.__tablename__, df, is_truncate_and_append)  # type: ignore

    @staticmethod
    def _write_to_database(session: SqlAlchemySession, table_name: str, df: pd.DataFrame, is_truncate_and_append: bool):
        """Write a dataframe to any database provided a session with a data model and a table name.

        Args:
            session: Generated from a data model and a table name
            table_name: The name of the table to read from a DB
            df: The dataframe to be written out
            is_truncate_and_append: Supply to truncate the table and append new rows to it; otherwise, delete and replace
        """
        if is_truncate_and_append:
            session.execute(f"TRUNCATE TABLE {table_name};")

            # Below is a speedup hack in place of `df.to_csv` with the multipart option. As of today, even with
            # `method="multi"`, uploading to Postgres is painfully slow. Hence, we're resorting to dumping the file as
            # csv and using Postgres's CSV import function.
            # https://stackoverflow.com/questions/2987433/how-to-import-csv-file-data-into-a-postgresql-table
            with tempfile.NamedTemporaryFile(mode="r+") as temp_file:
                df.to_csv(temp_file, index=False, header=False, sep="\t", doublequote=False, escapechar="\\", quoting=csv.QUOTE_NONE)
                temp_file.flush()
                temp_file.seek(0)

                cur = session.connection().connection.cursor()
                cur.copy_from(temp_file, table_name, columns=df.columns, null="")
        else:
            df.to_sql(name=table_name, con=session.get_bind(), if_exists="replace", index=False)

        session.commit()
