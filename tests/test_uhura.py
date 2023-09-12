from pathlib import Path

import pandas as pd
import pytest
from uhura.modes import fixture_builder_mode, task_test_mode

from dynamicio import KafkaResource, LocalFileResource, PostgresResource, S3Resource


@pytest.fixture()
def resources(file_name, tmpdir):
    file_resource = LocalFileResource(path=tmpdir / "actual" / file_name)
    s3_resource = S3Resource(bucket="bucket", path=file_name)
    return file_resource, s3_resource


@pytest.fixture
def file_resource(resources):
    return resources[0]


@pytest.fixture
def s3_resource(resources):
    return resources[1]


def test_uhura_file(test_df, tmpdir, file_name):
    file_resource = LocalFileResource(path=tmpdir / "actual" / file_name)
    file_resource.write(test_df)
    pd.testing.assert_frame_equal(file_resource.read(), test_df)

    with fixture_builder_mode(input_path=tmpdir / "uhura" / "input", known_good_path=tmpdir / "uhura" / "output"):
        file_resource.read()
        file_resource.write(test_df)

    with task_test_mode(input_path=tmpdir / "uhura" / "input", known_good_path=tmpdir / "uhura" / "output"):
        df = file_resource.read()
        file_resource.write(df)
        with pytest.raises(AssertionError):
            file_resource.write(df.drop("a", axis=1))


@pytest.fixture
def s3_fixtures(file_name, tmpdir, test_df):
    # Fixtures setup for s3 test
    file_resource = LocalFileResource(path=tmpdir / "actual" / file_name)
    file_resource.path = Path(tmpdir / "uhura" / "input" / "s3" / "bucket" / file_name)
    file_resource.write(test_df)
    file_resource.path = Path(tmpdir / "uhura" / "output" / "s3" / "bucket" / file_name)
    file_resource.write(test_df)


def test_uhura_s3(test_df, tmpdir, file_name, s3_fixtures):
    # Actual test
    s3_resource = S3Resource(bucket="bucket", path=file_name)
    with task_test_mode(input_path=tmpdir / "uhura" / "input", known_good_path=tmpdir / "uhura" / "output"):
        df = s3_resource.read()
        s3_resource.write(df)

        # Check that, in test mode, the dfs are being compared and if not the same -> fail.
        with pytest.raises(AssertionError):
            s3_resource.write(df.drop("a", axis=1))


def test_postgres_uhura(tmpdir, test_df):
    postgres_resource = PostgresResource(db_user="asdf", db_host="asdf", db_name="asdf", table_name="tabular_table")
    LocalFileResource(path=tmpdir / "uhura" / "input" / "postgres" / "public.tabular_table.parquet").write(test_df)
    LocalFileResource(path=tmpdir / "uhura" / "output" / "postgres" / "public.tabular_table.parquet").write(test_df)
    with task_test_mode(input_path=tmpdir / "uhura" / "input", known_good_path=tmpdir / "uhura" / "output"):
        postgres_resource.read()
        postgres_resource.write(test_df)
        with pytest.raises(AssertionError):
            postgres_resource.write(test_df.drop("a", axis=1))


def test_kafka_uhura(tmpdir, test_df):
    kafka_resource = KafkaResource(topic="tropico", server="asdf")
    LocalFileResource(path=tmpdir / "uhura" / "output" / "kafka" / "tropico.json").write(test_df)
    with task_test_mode(input_path=tmpdir / "uhura" / "input", known_good_path=tmpdir / "uhura" / "output"):
        kafka_resource.write(test_df)
        with pytest.raises(AssertionError):
            kafka_resource.write(test_df.drop("a", axis=1))
