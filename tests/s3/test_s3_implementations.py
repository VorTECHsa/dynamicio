from contextlib import contextmanager
from pathlib import Path
from typing import Generator
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from dynamicio.handlers.s3 import S3CsvResource, S3ParquetResource
from dynamicio.handlers.s3.file import BaseS3Resource, S3JsonResource
from tests import constants
from tests.resources.schemas import SampleSchema

input_path = constants.TEST_RESOURCES / "data/input"
s3_bucket = "my_bucket"


@pytest.fixture(
    params=[
        (S3CsvResource, "csv_sample.csv", pd.read_csv),
        (S3ParquetResource, "parquet_sample.parquet", pd.read_parquet),
        (S3JsonResource, "json_sample.json", pd.read_json),
    ],
    ids=lambda v: v[1],
)
def resource_file_function(request):
    return request.param


@pytest.fixture
def mock_reader(resource_file_function):
    return Mock(return_value=type("FileObj", (object,), {"name": input_path / resource_file_function[1]})())


@pytest.fixture
def s3_named_file_reader(mock_reader):
    @contextmanager
    def plain_s3_reader(s3_client, s3_bucket: str, s3_key: str) -> Generator:
        yield mock_reader(s3_client, s3_bucket, s3_key)

    with patch("dynamicio.handlers.s3.file.s3_named_file_reader", new=plain_s3_reader) as target:
        yield target


@pytest.fixture
def resource_and_expected_df(resource_file_function) -> (BaseS3Resource, pd.DataFrame):
    resource_class, file_name, read_func = resource_file_function
    return resource_class(
        bucket=s3_bucket,
        path=f"some/{file_name}",
        allow_no_schema=True,
    ), read_func(input_path / file_name)


def test_s3_resource_read(s3_stubber, resource_and_expected_df, s3_named_file_reader):
    resource, expected_df = resource_and_expected_df
    df = resource.read()
    pd.testing.assert_frame_equal(df, expected_df)


def test_s3_resource_read_with_schema(s3_stubber, resource_and_expected_df, s3_named_file_reader):
    resource, expected_df = resource_and_expected_df
    df = resource.read(pa_schema=SampleSchema)
    pd.testing.assert_frame_equal(df, expected_df)


def test_s3_resource_write(s3_stubber, resource_and_expected_df, s3_named_file_reader, tmpdir):
    resource, expected_df = resource_and_expected_df

    class MockResource(resource.__class__):
        @property
        def _full_path(self) -> Path:
            return self.path

    resource = MockResource(
        bucket=s3_bucket,
        path=f"some/{resource.path.name}",
        allow_no_schema=True,
    )
    target_location = tmpdir / "sample"
    resource.path = target_location
    resource.write(expected_df)
    df = resource.read()
    pd.testing.assert_frame_equal(df, expected_df)
