from contextlib import contextmanager
from pathlib import Path
from typing import Generator
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from dynamicio import S3CsvConfig, S3CsvResource, S3JsonConfig, S3JsonResource, S3ParquetConfig, S3ParquetResource
from tests import constants
from tests.resources.schemas import SampleSchema

input_path = constants.TEST_RESOURCES / "data/input"
s3_bucket = "my_bucket"


@pytest.fixture(
    params=[
        (S3CsvConfig, "csv_sample.csv", pd.read_csv, S3CsvResource, "csv"),
        (S3ParquetConfig, "parquet_sample.parquet", pd.read_parquet, S3ParquetResource, "parquet"),
        (S3JsonConfig, "json_sample.json", pd.read_json, S3JsonResource, "json"),
    ],
    ids=lambda v: v[1],
)
def config_file_function(request):
    return request.param


@pytest.fixture
def mock_reader(config_file_function):
    return Mock(return_value=type("FileObj", (object,), {"name": input_path / config_file_function[1]})())


@pytest.fixture
def s3_named_file_reader(mock_reader, config_file_function):
    @contextmanager
    def plain_s3_reader(s3_client, s3_bucket: str, s3_key: str) -> Generator:
        yield mock_reader(s3_client, s3_bucket, s3_key)

    module = config_file_function[4]

    with patch(f"dynamicio.io.s3.{module}.s3_named_file_reader", new=plain_s3_reader) as target:
        yield target


# --- Tests ---


def test_s3_config_read(s3_stubber, s3_named_file_reader, config_file_function):
    config_class, file_name, read_func, resource_class, _ = config_file_function
    resource = resource_class(
        config_class(
            bucket=s3_bucket,
            path=f"some/{file_name}",
        )
    )
    expected_df = read_func(input_path / file_name)
    df = resource.read()
    pd.testing.assert_frame_equal(df, expected_df)


def test_s3_config_read_with_schema(s3_stubber, s3_named_file_reader, config_file_function):
    config_class, file_name, read_func, resource_class, _ = config_file_function
    config = config_class(
        bucket=s3_bucket,
        path=f"some/{file_name}",
    )

    expected_df = read_func(input_path / file_name)

    df = resource_class(config, SampleSchema).read()

    pd.testing.assert_frame_equal(df, expected_df)


def test_s3_config_write(s3_stubber, s3_named_file_reader, tmpdir, config_file_function):
    config_class, file_name, read_func, resource_class, _ = config_file_function

    class MockConfig(config_class):
        @property
        def full_path(self) -> Path:
            return self.path

    config = MockConfig(
        bucket=s3_bucket,
        path=f"some/{file_name}",
        allow_no_schema=True,
    )

    expected_df = read_func(input_path / file_name)

    target_location = tmpdir / "sample"
    config.path = target_location

    resource = resource_class(config)
    resource.write(expected_df)
    df = resource.read()
    pd.testing.assert_frame_equal(df, expected_df)
