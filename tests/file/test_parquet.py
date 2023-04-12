import pandas as pd
import pytest

from dynamicio.handlers.file import ParquetFileResource
from tests import constants
from tests.resources.schemas import SampleSchema

sample_path = f"{constants.TEST_RESOURCES}/data/input/parquet_sample.parquet"


@pytest.fixture()
def parquet_file_resource() -> ParquetFileResource:
    return ParquetFileResource(
        path=sample_path,
        allow_no_schema=True,
    )


@pytest.fixture()
def parquet_df(parquet_file_resource) -> pd.DataFrame:
    return pd.read_parquet(sample_path)


def test_parquet_resource_read(parquet_file_resource, parquet_df):
    df = parquet_file_resource.read()
    pd.testing.assert_frame_equal(df, parquet_df)


def test_parquet_resource_read_with_schema(parquet_file_resource, parquet_df):
    df = parquet_file_resource.read(pa_schema=SampleSchema)
    pd.testing.assert_frame_equal(df, parquet_df)


def test_parquet_resource_write(parquet_df, tmpdir):
    tmp_location = tmpdir / "parquet_sample.parquet"
    resource = ParquetFileResource(
        path=tmp_location,
        allow_no_schema=True,
    )
    resource.write(parquet_df)
    df = pd.read_parquet(tmp_location)
    pd.testing.assert_frame_equal(df, parquet_df)
