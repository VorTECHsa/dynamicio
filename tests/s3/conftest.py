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
