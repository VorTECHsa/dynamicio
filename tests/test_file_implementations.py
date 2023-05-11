import pandas as pd
import pytest

from dynamicio.io.file import CsvResource, HdfResource, JsonResource, ParquetResource
from tests import constants
from tests.resources.schemas import SampleSchema

input_path = constants.TEST_RESOURCES / "data/input"

test_cases = [
    ("csv_sample.csv", CsvResource),
    ("parquet_sample.parquet", ParquetResource),
    ("json_sample.json", JsonResource),
    ("hdf_sample.h5", HdfResource),
]


@pytest.fixture()
def df() -> pd.DataFrame:
    return pd.read_parquet(input_path / "parquet_sample.parquet")


@pytest.mark.parametrize(
    "file_name, resource_class",
    test_cases,
)
def test_config_read(df, file_name, resource_class):
    resource = resource_class(path=input_path / file_name)
    df = resource.read()
    pd.testing.assert_frame_equal(df, df)


@pytest.mark.parametrize(
    "file_name, resource_class",
    test_cases,
)
def test_config_read_with_schema(df, file_name, resource_class):
    resource = resource_class(path=input_path / file_name, pa_schema=SampleSchema)
    df = resource.read()
    pd.testing.assert_frame_equal(df, df)


@pytest.mark.parametrize(
    "file_name, resource_class",
    test_cases,
)
def test_config_write(df, tmpdir, resource_class, file_name):
    resource = resource_class(path=tmpdir / "sample")
    resource.write(df)
    # reading should probably not be done with the config here
    df = resource.read()
    pd.testing.assert_frame_equal(df, df)
