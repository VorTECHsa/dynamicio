"""I/O functions and Resource class for postgres targeted operations."""
from __future__ import annotations

import csv
import tempfile
from contextlib import contextmanager
from copy import deepcopy
from typing import Any, Dict, Generator, List, Optional, Type

import pandas as pd  # type: ignore
from magic_logger import logger
from pandera import SchemaModel
from pydantic import BaseModel, Field  # pylint: disable=no-name-in-module
from sqlalchemy import create_engine  # type: ignore
from sqlalchemy.orm import Session as SqlAlchemySession  # type: ignore
from sqlalchemy.orm import sessionmaker  # type: ignore

from dynamicio.inject import check_injections, inject

Session = sessionmaker()


@contextmanager
def session_scope(connection_string: str) -> Generator[SqlAlchemySession, None, None]:
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
        session.commit()
    except Exception as exc:
        session.rollback()
        raise exc
    finally:
        session.close()  # pylint: disable=no-member


class ConfigurationError(Exception):
    """Raised when configuration is wrong."""


class PostgresResource(BaseModel):
    """Postgres Resource class.

    This resource handles reading and writing to postgres databases. If a pa_schema has been given, it will
    construct a sql query that only fetches the columns specified in the schema, this pushes down
    the filtering to the database, prevents us from unnecessarily loading data.

    Warning: Special case, where even if you explicitly don't validate, but give a pa_schema with strict="filter",
    the generated sql query will only query specified columns.
    """

    db_user: str
    db_password: Optional[str]
    db_host: str
    db_port: int
    db_name: str
    db_schema: str = "public"
    table_name: Optional[str] = Field(None, description="SQL table name. Needs to be given if no sql_query is given")
    sql_query: Optional[str] = Field(
        None, description="SQL query. Will fetch schema defined columns if this is not given."
    )
    truncate_and_append: bool = False
    kwargs: Dict[str, Any] = {}
    pa_schema: Optional[Type[SchemaModel]] = None

    @property
    def connection_string(self) -> str:
        """Build connection string out of components."""
        password = f":{self.db_password}" if self.db_password else ""
        return f"postgresql://{self.db_user}{password}@{self.db_host}:{self.db_port}/{self.db_name}"

    @property
    def final_table_name(self) -> str:
        """Return schema and table name in a format of schema.table_name."""
        return f"{self.db_schema}.{self.table_name}"

    def inject(self, **kwargs) -> "PostgresResource":
        """Inject variables into stuff. Not in place."""
        clone = deepcopy(self)
        clone.db_user = inject(clone.db_user, **kwargs)
        clone.db_password = inject(clone.db_password, **kwargs)
        clone.db_host = inject(clone.db_host, **kwargs)
        clone.db_name = inject(clone.db_name, **kwargs)
        clone.table_name = inject(clone.table_name, **kwargs)
        clone.sql_query = inject(clone.sql_query, **kwargs)
        clone.db_schema = inject(clone.db_schema, **kwargs)
        return clone

    def check_injections(self) -> None:
        """Check that all injections have been completed."""
        check_injections(self.db_user)
        check_injections(self.db_password)
        check_injections(self.db_host)
        check_injections(self.db_name)
        check_injections(self.table_name)
        check_injections(self.sql_query)
        check_injections(self.db_schema)

    def read(self) -> pd.DataFrame:
        """Handles Read operations for Postgres."""
        if not bool(self.sql_query) ^ bool(self.table_name):  # Xor
            raise ConfigurationError("PostgresResource must define EITHER sql_query OR table_name.")

        if self.pa_schema is not None and (not self.sql_query and self.pa_schema.Config.strict):
            # filtering can now be done at sql level
            columns: List[str] = list(self.pa_schema.to_schema().columns.keys())  # type: ignore
            columns_str = ", ".join(columns)
            sql_query = f"SELECT {columns_str} FROM {self.final_table_name}"
        elif self.sql_query is None:
            sql_query = f"SELECT * FROM {self.final_table_name}"
        else:
            sql_query = self.sql_query

        logger.info(f"Downloading table: {self.final_table_name} from: {self.db_host}:{self.db_name}")
        with session_scope(self.connection_string) as session:
            df = pd.read_sql(sql=sql_query, con=session.get_bind(), **self.kwargs)

        if self.pa_schema is not None:
            df = self.pa_schema.validate(df)

        return df

        # TODO: sqlalchemy 2.0 breaks here
        #       session.get_bind() is probably the culprit
        # TODO: Check if type coercion is needed here
        #       For example objects -> String(64)s
        #       For example date goes to Date and datetime64[ns] -> DateTime

    def write(self, df: pd.DataFrame):
        """Handles Write operations for Postgres."""
        if not self.table_name:
            raise ConfigurationError("PostgresResource must specify table_name for writing.")

        if schema := self.pa_schema:
            df = schema.validate(df)  # type: ignore

        with session_scope(self.connection_string) as session:
            session: SqlAlchemySession  # type: ignore # this is done for IDE purposes
            if self.truncate_and_append:
                logger.info(f"Writing to table (csv-hack): {self.final_table_name} from: {self.db_host}:{self.db_name}")
                session.execute(f"TRUNCATE TABLE {self.final_table_name};")

                # Speed hack: dump file as csv, use Postgres' CSV import function.
                # https://stackoverflow.com/questions/2987433/how-to-import-csv-file-data-into-a-postgresql-table
                with tempfile.NamedTemporaryFile(mode="r+") as temp_file:
                    df.to_csv(
                        temp_file,
                        index=False,
                        header=False,
                        sep="\t",
                        doublequote=False,
                        escapechar="\\",
                        quoting=csv.QUOTE_NONE,
                    )
                    temp_file.flush()
                    temp_file.seek(0)

                    cur = session.connection().connection.cursor()
                    cur.execute(f"SET search_path TO {self.db_schema};")
                    cur.copy_from(temp_file, self.table_name, columns=df.columns, null="")
            else:
                logger.info(f"Writing to table: {self.final_table_name} from: {self.db_host}:{self.db_name}")
                df.to_sql(
                    name=self.table_name,
                    con=session.get_bind(),
                    if_exists="replace",
                    index=False,
                    schema=self.db_schema,
                )
