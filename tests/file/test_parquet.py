import pandas as pd
import pytest

from dynamicio.handlers.file import ParquetFileResource
from tests import constants
from tests.resources.schemas import SampleSchema


@pytest.fixture()
def parquet_file_resource() -> ParquetFileResource:
    return ParquetFileResource(
        path=f"{constants.TEST_RESOURCES}/data/input/parquet_sample.parquet",
        allow_no_schema=True,
    )


@pytest.fixture()
def parquet_df(parquet_file_resource) -> pd.DataFrame:
    return parquet_file_resource.read()


@pytest.fixture()
def parquet_write_resource() -> ParquetFileResource:
    return ParquetFileResource(
        path=f"{constants.TEST_RESOURCES}/data/processed/parquet_sample.parquet",
        allow_no_schema=True,
    )


def test__resource_read(parquet_file_resource, parquet_df):
    df = parquet_file_resource.read()
    pd.testing.assert_frame_equal(df, parquet_df)


def test__resource_read_with_schema(parquet_file_resource, parquet_df):
    df = parquet_file_resource.read(pa_schema=SampleSchema)
    pd.testing.assert_frame_equal(df, parquet_df)


def test__resource_write(parquet_df, tmpdir):
    target_location = tmpdir / "parquet_sample.parquet"
    resource = ParquetFileResource(
        path=target_location,
        allow_no_schema=True,
    )
    resource.write(parquet_df)
    df = pd.read_parquet(resource.path)
    pd.testing.assert_frame_equal(df, parquet_df)
