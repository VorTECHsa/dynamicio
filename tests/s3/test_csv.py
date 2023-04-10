from contextlib import contextmanager
from pathlib import Path
from typing import Generator
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from dynamicio.handlers.s3 import S3CsvResource
from tests import constants
from tests.resources.schemas import SampleSchema

sample_path = constants.TEST_RESOURCES / "data/input/csv_sample.csv"


@pytest.fixture
def mock_reader():
    return Mock(return_value=type("FileObj", (object,), {"name": sample_path})())


@pytest.fixture
def s3_named_file_reader(mock_reader):
    @contextmanager
    def plain_s3_reader(s3_client, s3_bucket: str, s3_key: str) -> Generator:
        yield mock_reader(s3_client, s3_bucket, s3_key)

    with patch("dynamicio.handlers.s3.file.s3_named_file_reader", new=plain_s3_reader) as target:
        yield target


@pytest.fixture()
def csv_s3_resource() -> S3CsvResource:
    return S3CsvResource(
        bucket="my_bucket",
        path="some/path.csv",
        allow_no_schema=True,
    )


@pytest.fixture()
def csv_df(csv_s3_resource) -> pd.DataFrame:
    return pd.read_csv(sample_path)


def test__resource_read(s3_stubber, csv_s3_resource, s3_named_file_reader):
    df = csv_s3_resource.read()
    expected_df = pd.read_csv(sample_path)
    pd.testing.assert_frame_equal(df, expected_df)


def test__resource_read_with_schema(s3_stubber, csv_s3_resource, csv_df, s3_named_file_reader):
    df = csv_s3_resource.read(pa_schema=SampleSchema)
    pd.testing.assert_frame_equal(df, csv_df)


class MockS3CsvResource(S3CsvResource):
    @property
    def _full_path(self) -> Path:
        return self.path


def test__resource_write(s3_stubber, csv_df, s3_named_file_reader, tmpdir):
    target_location = tmpdir / "csv_sample.csv"
    resource = MockS3CsvResource(
        bucket="my_bucket",
        path=target_location,
        allow_no_schema=True,
    )
    resource.write(csv_df)
    df = pd.read_csv(resource.path)
    pd.testing.assert_frame_equal(df, csv_df)
