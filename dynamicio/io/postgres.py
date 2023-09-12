import csv
import logging
import tempfile
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Type

import pandas as pd  # type: ignore
from pandera import SchemaModel
from pydantic import Field  # pylint: disable=no-name-in-module
from sqlalchemy import create_engine  # type: ignore
from sqlalchemy.orm import Session as SqlAlchemySession  # type: ignore
from sqlalchemy.orm import sessionmaker  # type: ignore

from dynamicio.io.resource import BaseResource
from dynamicio.io.serde import ParquetSerde

Session = sessionmaker()


@contextmanager
def session_scope(connection_string: str, application_name: Optional[str]) -> Generator[SqlAlchemySession, None, None]:
    """Connect to a database using `connection_string` and returns an active session to that connection.

    Args:
        connection_string:
        application_name [optional]: Name of the application that is connecting to the database (repo name).


    Yields:
        Active session
    """
    application_name = application_name or "unknown-dynamicio-app"
    engine = create_engine(connection_string, connect_args={"application_name": application_name})
    session = Session(bind=engine)

    try:
        yield session
        session.commit()
    except Exception as exc:
        session.rollback()
        raise exc
    finally:
        session.close()  # pylint: disable=no-member


class PostgresResource(BaseResource):
    # Postgres Connection
    db_user: str
    db_password: Optional[str]
    db_host: str
    db_port: int = 5432
    db_name: str
    db_schema: str = "public"
    application_name: Optional[str] = Field(None, description="Application name to use for postgres connection.")

    # Postgres IO
    truncate_and_append: bool = False
    table_name: Optional[str] = Field(None, description="SQL table name. Needs to be given if no sql_query is given")
    sql_query: Optional[str] = Field(
        None, description="SQL query. Will fetch schema defined columns if this is not given."
    )
    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}

    # Resource
    injectables: List[str] = ["table_name", "sql_query", "db_user", "db_password", "db_host", "db_name", "db_schema"]
    pa_schema: Optional[Type[SchemaModel]] = None
    test_path: Optional[str] = None

    @property
    def connection_string(self) -> str:
        """Build connection string out of components."""
        password = f":{self.db_password}" if self.db_password else ""
        return f"postgresql://{self.db_user}{password}@{self.db_host}:{self.db_port}/{self.db_name}"

    @property
    def final_table_name(self) -> str:
        """Return schema and table name in a format of schema.table_name."""
        return f"{self.db_schema}.{self.table_name}"

    def _read(self) -> pd.DataFrame:
        """Handles Read operations for Postgres."""
        if not (bool(self.sql_query) ^ bool(self.table_name)):  # Xor
            raise ValueError("PostgresResource must define EITHER sql_query OR table_name.")

        if self.pa_schema is not None and (not self.sql_query and self.pa_schema.Config.strict):
            # filtering can now be done at sql level
            columns: List[str] = list(self.pa_schema.to_schema().columns.keys())  # type: ignore
            columns_str = ", ".join(columns)
            sql_query = f"SELECT {columns_str} FROM {self.final_table_name}"
        elif self.sql_query is None:
            sql_query = f"SELECT * FROM {self.final_table_name}"
        else:
            sql_query = self.sql_query

        logging.info(f"Downloading table: {self.final_table_name} from: {self.db_host}:{self.db_name}")
        with session_scope(self.connection_string, self.application_name) as session:
            df = pd.read_sql(sql=sql_query, con=session.get_bind(), **self.read_kwargs)

        return df

    def _write(self, df: pd.DataFrame) -> None:
        """Handles Write operations for Postgres."""
        if not self.table_name:
            raise ValueError("PostgresResource must specify table_name for writing.")

        with session_scope(self.connection_string, self.application_name) as session:
            session: SqlAlchemySession  # type: ignore # this is done for IDE purposes
            if self.truncate_and_append:
                logging.info(
                    f"Writing to table (csv-hack): {self.final_table_name} from: {self.db_host}:{self.db_name}"
                )
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
                logging.info(f"Writing to table: {self.final_table_name} from: {self.db_host}:{self.db_name}")
                df.to_sql(
                    name=self.table_name,
                    con=session.get_bind(),
                    if_exists="replace",
                    index=False,
                    schema=self.db_schema,
                )

    def cache_key(self) -> Path:
        if self.test_path is not None:
            return self.test_path
        elif self.table_name:
            return Path("postgres") / self.final_table_name
        elif self.sql_query:
            raise ValueError("test_path must be set if using custom sql query.")

    @property
    def serde_class(self):
        """Postgres uses a plain ParquetSerde for testing."""
        return ParquetSerde
