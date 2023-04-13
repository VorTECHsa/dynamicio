import pandas as pd
import pytest

from dynamicio.handlers.file import (
    CsvConfig,
    CsvHandler,
    HdfConfig,
    HdfHandler,
    JsonConfig,
    JsonHandler,
    ParquetConfig,
    ParquetHandler,
)
from tests import constants
from tests.resources.schemas import SampleSchema

input_path = constants.TEST_RESOURCES / "data/input"

test_cases = [
    (CsvConfig, "csv_sample.csv", CsvHandler),
    (ParquetConfig, "parquet_sample.parquet", ParquetHandler),
    (JsonConfig, "json_sample.json", JsonHandler),
    (HdfConfig, "hdf_sample.h5", HdfHandler),
]


@pytest.fixture()
def df() -> pd.DataFrame:
    return pd.read_parquet(input_path / "parquet_sample.parquet")


@pytest.mark.parametrize(
    "config_class, file_name, handler_class",
    test_cases,
)
def test_config_read(df, config_class, file_name, handler_class):
    config = config_class(path=input_path / file_name)
    handler = handler_class(config)
    df = handler.read()
    pd.testing.assert_frame_equal(df, df)


@pytest.mark.parametrize(
    "config_class, file_name, handler_class",
    test_cases,
)
def test_config_read_with_schema(df, config_class, file_name, handler_class):
    config = config_class(path=input_path / file_name)
    handler = handler_class(config, SampleSchema)
    df = handler.read()
    pd.testing.assert_frame_equal(df, df)


@pytest.mark.parametrize(
    "config_class, file_name, handler_class",
    test_cases,
)
def test_config_write(df, tmpdir, config_class, handler_class, file_name):
    target_location = tmpdir / "sample"
    handler = handler_class(
        config_class(
            path=target_location,
        )
    )
    handler.write(df)
    # reading should probably not be done with the config here
    df = handler.read()
    pd.testing.assert_frame_equal(df, df)
