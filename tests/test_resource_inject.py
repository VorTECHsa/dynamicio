from pathlib import Path

import pandas as pd
import pytest

from dynamicio import (
    CsvResource,
    HdfResource,
    JsonResource,
    KafkaResource,
    ParquetResource,
    PostgresResource,
    S3CsvResource,
    S3HdfResource,
    S3JsonResource,
    S3ParquetResource,
)
from dynamicio.inject import InjectionError


@pytest.fixture(
    params=[
        CsvResource,
        JsonResource,
        ParquetResource,
        HdfResource,
    ]
)
def file_resource(tmpdir, request):
    return request.param(path=tmpdir / "actual" / "some_file.extension")


def test_file_resource_inject_fail(file_resource, injectable_string, failing_injections, test_df):
    file_resource.path = Path(injectable_string)
    file_resource = file_resource.inject(**failing_injections)
    with pytest.raises(InjectionError):
        file_resource.read()
    with pytest.raises(InjectionError):
        file_resource.write(test_df)


def test_file_resource_inject_success(file_resource, injectable_string, passing_injections, test_df):
    file_resource.path = Path(injectable_string)
    file_resource = file_resource.inject(**passing_injections)
    file_resource.write(test_df)
    file_resource.read()
