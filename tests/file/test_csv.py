import pandas as pd
import pytest

from dynamicio.handlers.file import CsvFileResource
from tests import constants
from tests.resources.schemas import SampleSchema

sample_path = constants.TEST_RESOURCES / "data/input/csv_sample.csv"


@pytest.fixture()
def csv_file_resource() -> CsvFileResource:
    return CsvFileResource(
        path=sample_path,
        allow_no_schema=True,
    )


@pytest.fixture()
def csv_df(csv_file_resource) -> pd.DataFrame:
    return pd.read_csv(sample_path)


def test_csv_resource_read(csv_file_resource, csv_df):
    df = csv_file_resource.read()
    pd.testing.assert_frame_equal(df, csv_df)


def test_csv_resource_read_with_schema(csv_file_resource, csv_df):
    df = csv_file_resource.read(pa_schema=SampleSchema)
    pd.testing.assert_frame_equal(df, csv_df)


def test_csv_resource_write(csv_df, tmpdir):
    target_location = tmpdir / "csv_sample.csv"
    resource = CsvFileResource(
        path=target_location,
        allow_no_schema=True,
    )
    resource.write(csv_df)
    df = pd.read_csv(resource.path)
    pd.testing.assert_frame_equal(df, csv_df)
