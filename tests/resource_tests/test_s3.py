from contextlib import contextmanager
from typing import Generator

import boto3
import pandas as pd
import pytest
from botocore.stub import Stubber
from unittest.mock import patch

from dynamicio.io import S3Resource
from tests import constants
from tests.fixtures.schemas import SampleSchema


@pytest.fixture
def with_s3_stubber():
    s3_client = boto3.client("s3")
    Stubber(s3_client)

    with patch("boto3.client"):
        yield s3_client


@pytest.fixture
def with_mocked_named_reader():
    @contextmanager
    def mocked_named_reader(s3_client, s3_bucket: str, s3_key: str) -> Generator:
        name = s3_bucket + "/" + s3_key
        target_file = type("MockNamedTemporaryFile", (object,), {"name": name})()
        yield target_file

    with patch(f"dynamicio.io.s3.s3_named_file_reader", new=mocked_named_reader) as target:
        yield target


@contextmanager
def mocked_s3_generator(s3_client, s3_bucket: str, s3_key: str) -> Generator:
    yield s3_bucket + "/" + s3_key


@pytest.fixture
def with_mocked_reader():
    with patch(f"dynamicio.io.s3.s3_reader", new=mocked_s3_generator) as target:
        yield target


@pytest.fixture
def with_mocked_writer():
    with patch(f"dynamicio.io.s3.s3_writer", new=mocked_s3_generator) as target:
        yield target


@pytest.fixture(autouse=True)
def with_mocked_s3(with_mocked_named_reader, with_mocked_reader, with_mocked_writer, with_s3_stubber):
    yield


def test_read(test_df, file_name):
    resource = S3Resource(bucket=str(constants.TEST_FIXTURES), path=file_name)
    df = resource.read()
    pd.testing.assert_frame_equal(df, test_df)


def test_read_with_schema(test_df, file_name):
    resource = S3Resource(bucket=str(constants.TEST_FIXTURES), path=file_name, pa_schema=SampleSchema)
    df = resource.read()
    pd.testing.assert_frame_equal(df, test_df)


def test_write(test_df, tmpdir, file_name):
    resource = S3Resource(bucket=str(tmpdir), path=file_name)
    resource.write(test_df)
    # reading should probably not be done with the config here
    df = resource.read()
    pd.testing.assert_frame_equal(df, test_df)
