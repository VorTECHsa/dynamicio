import pandas as pd
import pytest

from dynamicio.io.file import (
    CsvConfig,
    CsvResource,
    HdfConfig,
    HdfResource,
    JsonConfig,
    JsonResource,
    ParquetConfig,
    ParquetResource,
)
from tests import constants
from tests.resources.schemas import SampleSchema

input_path = constants.TEST_RESOURCES / "data/input"

test_cases = [
    (CsvConfig, "csv_sample.csv", CsvResource),
    (ParquetConfig, "parquet_sample.parquet", ParquetResource),
    (JsonConfig, "json_sample.json", JsonResource),
    (HdfConfig, "hdf_sample.h5", HdfResource),
]


@pytest.fixture()
def df() -> pd.DataFrame:
    return pd.read_parquet(input_path / "parquet_sample.parquet")


@pytest.mark.parametrize(
    "config_class, file_name, resource_class",
    test_cases,
)
def test_config_read(df, config_class, file_name, resource_class):
    config = config_class(path=input_path / file_name)
    resource = resource_class(config)
    df = resource.read()
    pd.testing.assert_frame_equal(df, df)


@pytest.mark.parametrize(
    "config_class, file_name, resource_class",
    test_cases,
)
def test_config_read_with_schema(df, config_class, file_name, resource_class):
    config = config_class(path=input_path / file_name)
    resource = resource_class(config, SampleSchema)
    df = resource.read()
    pd.testing.assert_frame_equal(df, df)


@pytest.mark.parametrize(
    "config_class, file_name, resource_class",
    test_cases,
)
def test_config_write(df, tmpdir, config_class, resource_class, file_name):
    target_location = tmpdir / "sample"
    resource = resource_class(
        config_class(
            path=target_location,
        )
    )
    resource.write(df)
    # reading should probably not be done with the config here
    df = resource.read()
    pd.testing.assert_frame_equal(df, df)
