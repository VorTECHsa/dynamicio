import pandas as pd
import pytest

from dynamicio.handlers.file import CsvFileResource
from tests import constants
from tests.resources.schemas import SampleSchema


@pytest.fixture()
def csv_file_resource() -> CsvFileResource:
    return CsvFileResource(
        path=f"{constants.TEST_RESOURCES}/data/input/csv_sample.csv",
        allow_no_schema=True,
    )


@pytest.fixture()
def csv_df(csv_file_resource) -> pd.DataFrame:
    return csv_file_resource.read()


@pytest.fixture()
def csv_write_resource() -> CsvFileResource:
    return CsvFileResource(
        path=f"{constants.TEST_RESOURCES}/data/processed/csv_sample.csv",
        allow_no_schema=True,
    )


def test__resource_read(csv_file_resource, csv_df):
    df = csv_file_resource.read()
    pd.testing.assert_frame_equal(df, csv_df)


def test__resource_read_with_schema(csv_file_resource, csv_df):
    df = csv_file_resource.read(pa_schema=SampleSchema)
    pd.testing.assert_frame_equal(df, csv_df)


def test__resource_write(csv_df, tmpdir):
    target_location = tmpdir / "csv_sample.csv"
    resource = CsvFileResource(
        path=target_location,
        allow_no_schema=True,
    )
    resource.write(csv_df)
    df = pd.read_csv(resource.path)
    pd.testing.assert_frame_equal(df, csv_df)
