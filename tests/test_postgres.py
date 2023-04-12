from unittest.mock import ANY, MagicMock, Mock, patch

import pandas as pd
import pytest

from dynamicio.handlers import PostgresResource
from tests import constants
from tests.resources.schemas import PgSampleSchema

sample_path = f"{constants.TEST_RESOURCES}/data/input/pg_parquet_sample.parquet"


@pytest.fixture()
def postgres_table_resource() -> PostgresResource:
    return PostgresResource(
        db_user="test_user",
        db_host="test_host",
        db_port=1234,
        db_name="test_db",
        table_name="test_table",
        allow_no_schema=True,
    )


@pytest.fixture()
def postgres_query_resource() -> PostgresResource:
    return PostgresResource(
        db_user="test_user",
        db_host="test_host",
        db_port=1234,
        db_name="test_db",
        sql_query="SELECT * FROM other_table",
        allow_no_schema=True,
    )


@pytest.fixture
def mock_cursor():
    return MagicMock()


@pytest.fixture
def mock_binding():
    return "mock_binding"


@pytest.fixture
def mocked_session(mock_cursor, mock_binding):
    mock_session = MagicMock()
    mock_session.connection.return_value.connection.cursor.return_value = mock_cursor
    mock_session.get_bind.return_value = mock_binding
    mock_session_maker = Mock(return_value=mock_session)
    with patch("dynamicio.handlers.postgres.Session", mock_session_maker):
        yield mock_session


@pytest.fixture()
def postgres_df(postgres_table_resource) -> pd.DataFrame:
    return pd.read_parquet(sample_path)


@pytest.fixture
def read_sql_mock(postgres_df):
    with patch("pandas.read_sql", return_value=postgres_df) as mock:
        yield mock


@pytest.fixture
def to_sql_mock(postgres_df):
    with patch("pandas.DataFrame.to_sql", return_value=None) as mock:
        yield mock


def test_postgres_resource_read(postgres_table_resource, postgres_df, read_sql_mock, mocked_session, mock_binding):
    df = postgres_table_resource.read()
    read_sql_mock.assert_called_once_with(sql="SELECT * FROM test_table", con=mock_binding)
    pd.testing.assert_frame_equal(df, postgres_df)


def test_postgres_resource_read_with_schema(
    postgres_table_resource, postgres_df, read_sql_mock, mocked_session, mock_binding
):
    df = postgres_table_resource.read(pa_schema=PgSampleSchema)
    read_sql_mock.assert_called_once_with(sql="SELECT * FROM test_table", con=mock_binding)
    pd.testing.assert_frame_equal(df, postgres_df)


class PgFilterSampleSchema(PgSampleSchema):
    class Config:
        strict = "filter"


# TODO: passing a pa_schema directly does not work currently
def test_postgres_resource_read_with_filter_schema(
    postgres_table_resource, postgres_df, read_sql_mock, mocked_session, mock_binding
):
    postgres_table_resource.pa_schema = PgFilterSampleSchema
    df = postgres_table_resource.read()
    read_sql_mock.assert_called_once_with(
        sql="SELECT test_table.id, test_table.foo, test_table.bar, test_table.baz \nFROM test_table", con=mock_binding
    )
    pd.testing.assert_frame_equal(df, postgres_df)


def test_postgres_query_resource_read(
    postgres_query_resource, postgres_df, read_sql_mock, mocked_session, mock_binding
):
    df = postgres_query_resource.read()
    read_sql_mock.assert_called_once_with(sql="SELECT * FROM other_table", con=mock_binding)
    pd.testing.assert_frame_equal(df, postgres_df)


# --- Write tests ---


def test_postgres_resource_write(
    postgres_table_resource, postgres_df, to_sql_mock, mocked_session, mock_binding, mock_cursor
):
    postgres_table_resource.write(postgres_df)
    to_sql_mock.assert_called_once_with(name="test_table", con=mock_binding, if_exists="replace", index=False)


def test_postgres_resource_write_truncate_and_append(
    postgres_table_resource, postgres_df, to_sql_mock, mocked_session, mock_binding, mock_cursor
):
    postgres_table_resource.truncate_and_append = True
    postgres_table_resource.write(postgres_df)
    mocked_session.execute.assert_called_once_with("TRUNCATE TABLE test_table;")
    mock_cursor.copy_from.assert_called_once_with(ANY, "test_table", columns=postgres_df.columns, null="")
