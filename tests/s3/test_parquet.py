from contextlib import contextmanager
from pathlib import Path
from typing import Generator
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from dynamicio.handlers.s3 import S3ParquetResource
from tests import constants
from tests.resources.schemas import SampleSchema

sample_path = constants.TEST_RESOURCES / "data/input/parquet_sample.parquet"


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
def parquet_s3_resource() -> S3ParquetResource:
    return S3ParquetResource(
        bucket="my_bucket",
        path="some/path.parquet",
        allow_no_schema=True,
    )


@pytest.fixture()
def parquet_df(parquet_s3_resource) -> pd.DataFrame:
    return pd.read_parquet(sample_path)


def test__resource_read(s3_stubber, parquet_s3_resource, parquet_df, s3_named_file_reader):
    df = parquet_s3_resource.read()
    pd.testing.assert_frame_equal(df, parquet_df)


def test__resource_read_with_schema(s3_stubber, parquet_s3_resource, parquet_df, s3_named_file_reader):
    df = parquet_s3_resource.read(pa_schema=SampleSchema)
    pd.testing.assert_frame_equal(df, parquet_df)


class MockS3ParquetResource(S3ParquetResource):
    @property
    def _full_path(self) -> Path:
        return self.path


def test__resource_write(s3_stubber, parquet_df, s3_named_file_reader, tmpdir):
    tmp_location = tmpdir / "parquet_sample.parquet"
    resource = MockS3ParquetResource(
        bucket="my_bucket",
        path=tmp_location,
        allow_no_schema=True,
    )
    resource.write(parquet_df)
    df = pd.read_parquet(tmp_location)
    pd.testing.assert_frame_equal(df, parquet_df)
