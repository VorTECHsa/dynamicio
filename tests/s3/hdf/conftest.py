# flake8: noqa: I101

import io
from contextlib import contextmanager
from typing import IO, Generator
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from dynamicio import S3HdfResource
from tests import constants

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


@pytest.fixture
def hdf_s3_resource() -> S3HdfResource:
    return S3HdfResource(
        bucket="bucket",
        path="some/path.hdf",
    )


@pytest.fixture
def hdf_s3_config() -> dict:
    return {
        "bucket": "bucket",
        "path": "some/path.hdf",
    }


@pytest.fixture
def hdf_df(hdf_s3_resource) -> pd.DataFrame:
    return pd.read_hdf(sample_path)
