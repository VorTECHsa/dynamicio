import pandas as pd
import pytest

from dynamicio.handlers.file import HdfFileResource
from tests import constants
from tests.resources.schemas import SampleSchema

sample_path = constants.TEST_RESOURCES / "data/input/hdf_sample.h5"


@pytest.fixture()
def hdf_file_resource() -> HdfFileResource:
    return HdfFileResource(
        path=sample_path,
        allow_no_schema=True,
    )


@pytest.fixture()
def hdf_df(hdf_file_resource) -> pd.DataFrame:
    return pd.read_hdf(sample_path)


def test__resource_read(hdf_file_resource, hdf_df):
    df = hdf_file_resource.read()
    pd.testing.assert_frame_equal(df, hdf_df)


def test__resource_read_with_schema(hdf_file_resource, hdf_df):
    df = hdf_file_resource.read(pa_schema=SampleSchema)
    pd.testing.assert_frame_equal(df, hdf_df)


def test__resource_write(hdf_df, tmpdir):
    tmp_location = tmpdir / "hdf_sample.hdf"
    resource = HdfFileResource(
        path=tmp_location,
        allow_no_schema=True,
    )
    resource.write(hdf_df)
    df = pd.read_hdf(tmp_location)
    pd.testing.assert_frame_equal(df, hdf_df)
