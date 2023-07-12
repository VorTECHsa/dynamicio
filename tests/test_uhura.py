from pathlib import Path

import pandas as pd
import pytest
from uhura.modes import fixture_builder_mode, task_test_mode

from dynamicio import (
    CsvResource,
    HdfResource,
    JsonResource,
    KafkaResource,
    ParquetResource,
    PostgresResource,
    S3CsvResource,
    S3HdfResource,
    S3JsonResource,
    S3ParquetResource,
)

# get test df

# get temp dir
# write to df to temp dir
# read from temp dir
# assert df is the same

# with fixture builder mode
# read from temp dir
# write to temp dir
# with test mode read from temp dir
# with test mode write to temp dir
# with test mode write to temp dir but wrong data


@pytest.fixture
def test_df():
    return pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


@pytest.fixture(
    params=[
        (CsvResource, S3CsvResource),
        (JsonResource, S3JsonResource),
        (ParquetResource, S3ParquetResource),
        (HdfResource, S3HdfResource),
    ]
)
def resources(request, tmpdir):
    file_resource = request.param[0](path=tmpdir / "actual" / "some_file.extension")
    s3_resource = request.param[1](bucket="bucket", path="some_file.extension")
    return file_resource, s3_resource


@pytest.fixture
def file_resource(resources):
    return resources[0]


@pytest.fixture
def s3_resource(resources):
    return resources[1]


def test_uhura_file_and_s3(test_df, file_resource, s3_resource, tmpdir):
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

    # Write for s3 test
    file_resource.path = Path(tmpdir / "uhura" / "input" / "s3" / "bucket" / "some_file.extension")
    file_resource.write(test_df)
    file_resource.path = Path(tmpdir / "uhura" / "output" / "s3" / "bucket" / "some_file.extension")
    file_resource.write(test_df)

    with task_test_mode(input_path=tmpdir / "uhura" / "input", known_good_path=tmpdir / "uhura" / "output"):
        df = s3_resource.read()
        s3_resource.write(df)
        with pytest.raises(AssertionError):
            s3_resource.write(df.drop("a", axis=1))


def test_postgres_uhura(tmpdir, test_df):
    postgres_resource = PostgresResource(db_user="asdf", db_host="asdf", db_name="asdf", table_name="tabular_table")
    ParquetResource(path=tmpdir / "uhura" / "input" / "postgres" / "tabular_table.parquet").write(test_df)
    ParquetResource(path=tmpdir / "uhura" / "output" / "postgres" / "tabular_table.parquet").write(test_df)
    with task_test_mode(input_path=tmpdir / "uhura" / "input", known_good_path=tmpdir / "uhura" / "output"):
        postgres_resource.read()
        postgres_resource.write(test_df)
        with pytest.raises(AssertionError):
            postgres_resource.write(test_df.drop("a", axis=1))


def test_kafka_uhura(tmpdir, test_df):
    kafka_resource = KafkaResource(topic="tropico", server="asdf")
    ParquetResource(path=tmpdir / "uhura" / "output" / "kafka" / "tropico").write(test_df)
    with task_test_mode(input_path=tmpdir / "uhura" / "input", known_good_path=tmpdir / "uhura" / "output"):
        kafka_resource.write(test_df)
        with pytest.raises(AssertionError):
            kafka_resource.write(test_df.drop("a", axis=1))
