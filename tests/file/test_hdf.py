import pandas as pd
import pytest

from dynamicio.handlers.file import HdfFileResource
from tests import constants
from tests.resources.schemas import SampleSchema


@pytest.fixture()
def hdf_file_resource() -> HdfFileResource:
    return HdfFileResource(
        path=f"{constants.TEST_RESOURCES}/data/input/hdf_sample.h5",
        allow_no_schema=True,
    )


@pytest.fixture()
def hdf_df(hdf_file_resource) -> pd.DataFrame:
    return hdf_file_resource.read()


@pytest.fixture()
def hdf_write_resource() -> HdfFileResource:
    return HdfFileResource(
        path=f"{constants.TEST_RESOURCES}/data/processed/hdf_sample.h5",
        allow_no_schema=True,
    )


def test__resource_read(hdf_file_resource, hdf_df):
    df = hdf_file_resource.read()
    pd.testing.assert_frame_equal(df, hdf_df)


def test__resource_read_with_schema(hdf_file_resource, hdf_df):
    df = hdf_file_resource.read(pa_schema=SampleSchema)
    pd.testing.assert_frame_equal(df, hdf_df)


def test__resource_write(hdf_df, tmpdir):
    target_location = tmpdir / "hdf_sample.hdf"
    resource = HdfFileResource(
        path=target_location,
        allow_no_schema=True,
    )
    resource.write(hdf_df)
    df = pd.read_hdf(resource.path)
    pd.testing.assert_frame_equal(df, hdf_df)
