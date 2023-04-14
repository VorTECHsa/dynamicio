from contextlib import contextmanager
from typing import Generator
from unittest.mock import Mock, patch

import boto3
import pytest
from botocore.stub import Stubber


@pytest.fixture
def s3_stubber():
    s3_client = boto3.client("s3")
    Stubber(s3_client)

    with patch("boto3.client"):
        yield s3_client


@pytest.fixture
def mock_reader():
    return Mock(return_value=type("FileObj", (object,), {"name": "mock_name"})())


@pytest.fixture
def s3_named_file_reader(mock_reader):
    @contextmanager
    def plain_s3_reader(s3_client, s3_bucket: str, s3_key: str) -> Generator:
        yield mock_reader(s3_client, s3_bucket, s3_key)

    with patch("dynamicio.io.s3.file.s3_named_file_reader", new=plain_s3_reader) as target:
        yield target
