import pandas as pd
import pytest

from dynamicio.handlers.file import CsvFileResource, HdfFileResource, JsonFileResource, ParquetFileResource
from tests import constants
from tests.resources.schemas import SampleSchema

input_path = constants.TEST_RESOURCES / "data/input"

test_cases = [
    (CsvFileResource, "csv_sample.csv"),
    (ParquetFileResource, "parquet_sample.parquet"),
    (JsonFileResource, "json_sample.json"),
    (HdfFileResource, "hdf_sample.h5"),
]


@pytest.fixture(
    params=test_cases,
    ids=lambda v: v[1],
)
def file_resource(request):
    resource_class, file_name = request.param
    return resource_class(
        path=input_path / file_name,
        allow_no_schema=True,
    )


@pytest.fixture()
def df() -> pd.DataFrame:
    return pd.read_parquet(input_path / "parquet_sample.parquet")


def test_resource_read(file_resource, df):
    df = file_resource.read()
    pd.testing.assert_frame_equal(df, df)


def test_resource_read_with_schema(file_resource, df):
    df = file_resource.read(pa_schema=SampleSchema)
    pd.testing.assert_frame_equal(df, df)


@pytest.mark.parametrize(
    "resource_class, file_name",
    test_cases,
)
def test_resource_write(df, tmpdir, resource_class, file_name):
    target_location = tmpdir / "sample"
    resource = resource_class(
        path=target_location,
        allow_no_schema=True,
    )
    resource.write(df)
    # reading should probably not be done with the resource here
    df = resource.read()
    pd.testing.assert_frame_equal(df, df)
