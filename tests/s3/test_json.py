from contextlib import contextmanager
from pathlib import Path
from typing import Generator
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from dynamicio.handlers.s3 import S3JsonResource
from tests import constants
from tests.resources.schemas import SampleSchema

sample_path = constants.TEST_RESOURCES / "data/input/json_sample.json"


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
def json_s3_resource() -> S3JsonResource:
    return S3JsonResource(
        bucket="my_bucket",
        path="some/path.json",
        allow_no_schema=True,
    )


@pytest.fixture()
def json_df(json_s3_resource) -> pd.DataFrame:
    return pd.read_json(sample_path)


def test__resource_read(s3_stubber, json_s3_resource, json_df, s3_named_file_reader):
    df = json_s3_resource.read()
    pd.testing.assert_frame_equal(df, json_df)


def test__resource_read_with_schema(s3_stubber, json_s3_resource, json_df, s3_named_file_reader):
    df = json_s3_resource.read(pa_schema=SampleSchema)
    pd.testing.assert_frame_equal(df, json_df)


class MockS3JsonResource(S3JsonResource):
    @property
    def _full_path(self) -> Path:
        return self.path


def test__resource_write(s3_stubber, json_df, s3_named_file_reader, tmpdir):
    tmp_location = tmpdir / "json_sample.json"
    resource = MockS3JsonResource(
        bucket="my_bucket",
        path=tmp_location,
        allow_no_schema=True,
    )
    resource.write(json_df)
    df = pd.read_json(tmp_location)
    pd.testing.assert_frame_equal(df, json_df)
