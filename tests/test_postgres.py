from unittest.mock import ANY, MagicMock, Mock, patch

import pandas as pd
import pytest

from dynamicio import PostgresConfig, PostgresResource
from dynamicio.inject import InjectionError
from dynamicio.io.postgres import ConfigurationError
from tests import constants
from tests.resources.schemas import PgSampleSchema

sample_path = f"{constants.TEST_RESOURCES}/data/input/pg_parquet_sample.parquet"


@pytest.fixture
def postgres_table_config() -> PostgresConfig:
    return PostgresConfig(
        db_user="test_user",
        db_host="test_host",
        db_port=1234,
        db_name="test_db",
        db_schema="republic",
        table_name="test_table",
    )


@pytest.fixture()
def postgres_table_resource(postgres_table_config) -> PostgresResource:
    return PostgresResource(postgres_table_config)


@pytest.fixture()
def postgres_query_resource() -> PostgresResource:
    config = PostgresConfig(
        db_user="test_user",
        db_host="test_host",
        db_port=1234,
        db_name="test_db",
        db_schema="republic",
        sql_query="SELECT * FROM other_table",
    )
    return PostgresResource(config)


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
    with patch("dynamicio.io.postgres.Session", mock_session_maker):
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
    read_sql_mock.assert_called_once_with(sql="SELECT * FROM republic.test_table", con=mock_binding)
    pd.testing.assert_frame_equal(df, postgres_df)


def test_postgres_resource_read_with_schema(
    postgres_table_config, postgres_df, read_sql_mock, mocked_session, mock_binding
):
    resource = PostgresResource(postgres_table_config, pa_schema=PgSampleSchema)
    df = resource.read()
    read_sql_mock.assert_called_once_with(sql="SELECT * FROM republic.test_table", con=mock_binding)
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
        sql="SELECT id, foo, bar, baz FROM republic.test_table",
        con=mock_binding,
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
    to_sql_mock.assert_called_once_with(
        name="test_table", con=mock_binding, if_exists="replace", index=False, schema="republic"
    )


def test_postgres_resource_write_truncate_and_append(
    postgres_table_resource, postgres_df, to_sql_mock, mocked_session, mock_binding, mock_cursor
):
    postgres_table_resource.config.truncate_and_append = True
    postgres_table_resource.write(postgres_df)
    mocked_session.execute.assert_called_once_with("TRUNCATE TABLE republic.test_table;")
    mock_cursor.execute.assert_called_once_with("SET search_path TO republic;")
    mock_cursor.copy_from.assert_called_once_with(ANY, "test_table", columns=postgres_df.columns, null="")


def test_postgres_resource_inject_and_read(postgres_df, read_sql_mock, mocked_session, mock_binding):
    config = PostgresConfig(
        db_user="[[db_user]]",
        db_host="{db_host}",
        db_port=1234,
        db_name="that_{db_name}",
        db_schema="[[republic]]",
        table_name="[[table]]",
    )
    config = config.inject(
        db_user="test_user", db_host="test_host", db_name="test_db", table="test_table", republic="republic"
    )
    resource = PostgresResource(config)
    df = resource.read()
    read_sql_mock.assert_called_once_with(sql="SELECT * FROM republic.test_table", con=mock_binding)
    pd.testing.assert_frame_equal(df, postgres_df)


def test_postgres_resource_inject_and_read_query(postgres_df, read_sql_mock, mocked_session, mock_binding):
    config = PostgresConfig(
        db_user="[[db_user]]",
        db_host="{db_host}",
        db_port=1234,
        db_name="that_{db_name}",
        db_schema="[[republic]]",
        sql_query="SELECT * FROM [[republic]].[[table]]",
    )
    config = config.inject(
        db_user="test_user", db_host="test_host", db_name="test_db", table="test_table", republic="republic"
    )
    resource = PostgresResource(config)
    df = resource.read()
    read_sql_mock.assert_called_once_with(sql="SELECT * FROM republic.test_table", con=mock_binding)
    pd.testing.assert_frame_equal(df, postgres_df)


@pytest.mark.parametrize("arg_to_miss_out", ["db_user", "db_host", "db_name", "table"])
def test_postgres_resource_check_raises_on_incomplete_injection(
    arg_to_miss_out, postgres_df, read_sql_mock, mocked_session, mock_binding
):
    args = {"db_user": "test_user", "db_host": "test_host", "db_name": "test_db", "table": "test_table"}
    args.pop(arg_to_miss_out)
    config = PostgresConfig(
        db_user="[[db_user]]",
        db_host="{db_host}",
        db_port=1234,
        db_name="that_{db_name}",
        table_name="[[table]]",
    )
    config = config.inject(**args)
    with pytest.raises(InjectionError):
        config.check_injections()


def test_postgres_resource_raises_on_wrong_read_configuration(postgres_df, read_sql_mock, mocked_session, mock_binding):
    config = PostgresConfig(
        db_user="test_user",
        db_host="test_host",
        db_port=1234,
        db_name="test_db",
        table_name="test_table",
        sql_query="SELECT * FROM other_table",
    )
    resource = PostgresResource(config)
    with pytest.raises(ConfigurationError):
        resource.read()


def test_postgres_resource_raises_on_wrong_write_configuration(
    postgres_df, read_sql_mock, mocked_session, mock_binding, to_sql_mock, mock_cursor
):
    config = PostgresConfig(
        db_user="test_user",
        db_host="test_host",
        db_port=1234,
        db_name="test_db",
        sql_query="SELECT * FROM other_table",
    )
    resource = PostgresResource(config)
    with pytest.raises(ConfigurationError):
        resource.write(postgres_df)
