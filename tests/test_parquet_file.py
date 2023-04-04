# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, R0801, W0621
import pandas as pd
import pytest

from dynamicio.handlers.file import ParquetFileResource
from tests import constants
from tests.resources.schemas import SomeParquetToRead


@pytest.fixture()
def parquet_file_resource() -> ParquetFileResource:
    return ParquetFileResource(
        path=f"{constants.TEST_RESOURCES}/data/input/some_parquet_to_read.parquet",
        allow_no_schema=True,
    )


@pytest.fixture()
def parquet_df(parquet_file_resource) -> pd.DataFrame:
    return parquet_file_resource.read()


@pytest.fixture()
def parquet_write_resource() -> ParquetFileResource:
    return ParquetFileResource(
        path=f"{constants.TEST_RESOURCES}/data/processed/some_parquet_to_read.parquet",
        allow_no_schema=True,
    )


def test__resource_read(parquet_file_resource, parquet_df):
    df = parquet_file_resource.read()
    pd.testing.assert_frame_equal(df, parquet_df)


def test__resource_read_with_schema(parquet_file_resource, parquet_df):
    df = parquet_file_resource.read(pa_schema=SomeParquetToRead)
    pd.testing.assert_frame_equal(df, parquet_df)


def test__resource_write(parquet_write_resource, parquet_df):
    parquet_write_resource.write(parquet_df)
    df = pd.read_parquet(parquet_write_resource._final_path)
    pd.testing.assert_frame_equal(df, parquet_df)
