# flake8: noqa: I101

import io
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Generator
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from dynamicio import S3HdfResource
from tests import constants
from tests.resources.schemas import SampleSchema

sample_path = constants.TEST_RESOURCES / "data/input/hdf_sample.h5"


@pytest.fixture
def mock_reader():
    return Mock(return_value=type("FileObj", (object,), {"name": sample_path})())


@pytest.fixture
def mock_s3_named_file_reader(mock_reader):
    @contextmanager
    def plain_s3_reader(s3_client, s3_bucket: str, s3_key: str) -> Generator:
        yield mock_reader(s3_client, s3_bucket, s3_key)

    with patch("dynamicio.io.s3.hdf.s3_named_file_reader", new=plain_s3_reader) as target:
        yield target


@pytest.fixture
def mock_s3_reader(mock_reader):
    @contextmanager
    def plain_s3_reader(s3_client, s3_bucket, s3_key: str) -> Generator[IO[bytes], None, None]:
        with open(sample_path, "rb") as fh:
            yield io.BytesIO(fh.read())

    with patch("dynamicio.io.s3.hdf.s3_reader", new=plain_s3_reader) as target:
        yield target


@pytest.fixture
def mock_s3_writer(mock_reader, tmp_path):
    @contextmanager
    def plain_s3_writer(s3_client, s3_bucket, s3_key: str) -> Generator[IO[bytes], None, None]:
        fobj = io.BytesIO()
        yield fobj
        fobj.seek(0)
        with open(tmp_path / s3_key, "wb") as f:
            f.write(fobj.getbuffer())

    with patch("dynamicio.io.s3.hdf.s3_writer", new=plain_s3_writer) as target:
        yield target


@pytest.fixture()
def hdf_s3_resource() -> S3HdfResource:
    return S3HdfResource(
        bucket="my_bucket",
        path="some/path.hdf",
    )


@pytest.fixture()
def hdf_s3_config() -> dict:
    return {
        "bucket": "my_bucket",
        "path": "some/path.hdf",
    }


@pytest.fixture()
def hdf_df(hdf_s3_resource) -> pd.DataFrame:
    return pd.read_hdf(sample_path)


def test__resource_read(s3_stubber, hdf_s3_resource, hdf_df, mock_s3_named_file_reader):
    df = hdf_s3_resource.read()
    pd.testing.assert_frame_equal(df, hdf_df)


def test__resource_read_with_schema(s3_stubber, hdf_s3_config, hdf_df, mock_s3_named_file_reader):
    df = S3HdfResource(**hdf_s3_config, pa_schema=SampleSchema).read()
    pd.testing.assert_frame_equal(df, hdf_df)


def test__resource_read_no_disk_space(s3_stubber, hdf_s3_config, hdf_df, mock_s3_reader):
    hdf_s3_config["force_read_to_memory"] = True
    df = S3HdfResource(**hdf_s3_config).read()
    pd.testing.assert_frame_equal(df, hdf_df)


class MockS3HdfResource(S3HdfResource):
    @property
    def _full_path(self) -> Path:
        return self.path


def test__resource_write(s3_stubber, hdf_df, mock_s3_writer, tmp_path):
    tmp_location = tmp_path / "sample_file.hdf"
    resource = MockS3HdfResource(
        bucket="my_bucket",
        path=tmp_location,
    )
    resource.write(hdf_df)
    df = pd.read_hdf(tmp_location)
    pd.testing.assert_frame_equal(df, hdf_df)
