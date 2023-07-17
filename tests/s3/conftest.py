"""S3 Read functions are mocked via s3_named_file_reader and read the sample data in tests.
S3 Write functions are mocked by virtue of overriding full_path and writing to a temporary directory."""

from contextlib import contextmanager
from typing import Generator
from unittest.mock import Mock, patch

import boto3
import pandas as pd
import pytest
from botocore.stub import Stubber

from dynamicio import S3CsvResource, S3JsonResource, S3ParquetResource
from tests import constants


@pytest.fixture
def s3_stubber():
    s3_client = boto3.client("s3")
    Stubber(s3_client)

    with patch("boto3.client"):
        yield s3_client


@pytest.fixture
def input_path():
    return constants.TEST_RESOURCES / "data/input"


@pytest.fixture(
    params=[
        ("csv_sample.csv", pd.read_csv, pd.DataFrame.to_csv, S3CsvResource, "csv"),
        ("parquet_sample.parquet", pd.read_parquet, pd.DataFrame.to_parquet, S3ParquetResource, "parquet"),
        ("json_sample.json", pd.read_json, pd.DataFrame.to_json, S3JsonResource, "json"),
    ],
    ids=lambda v: v[1],
)
def config_file_function(request):
    return request.param


@pytest.fixture
def mock_reader(config_file_function, input_path):
    return Mock(return_value=type("FileObj", (object,), {"name": input_path / config_file_function[0]})())


@pytest.fixture
def s3_named_file_reader(mock_reader, file_string):
    @contextmanager
    def plain_s3_reader(s3_client, s3_bucket: str, s3_key: str) -> Generator:
        yield mock_reader(s3_client, s3_bucket, s3_key)

    with patch(f"dynamicio.io.s3.{file_string}.s3_named_file_reader", new=plain_s3_reader) as target:
        yield target


@pytest.fixture
def file_name(s3_stubber, s3_named_file_reader, config_file_function):
    file_name, read_func, write_func, resource_class, _ = config_file_function
    return file_name


@pytest.fixture
def read_func(s3_stubber, s3_named_file_reader, config_file_function):
    file_name, read_func, write_func, resource_class, _ = config_file_function
    return read_func


@pytest.fixture
def write_func(s3_stubber, s3_named_file_reader, config_file_function):
    file_name, read_func, write_func, resource_class, _ = config_file_function
    return write_func


@pytest.fixture
def resource_class(s3_stubber, s3_named_file_reader, config_file_function):
    file_name, read_func, write_func, resource_class, _ = config_file_function

    class NewResourceClass(resource_class):
        @property
        def full_path(self):
            return f"{self.bucket}/{self.path}"

    return NewResourceClass


@pytest.fixture
def file_string(config_file_function):
    file_name, read_func, write_func, resource_class, file_string = config_file_function
    return file_string
