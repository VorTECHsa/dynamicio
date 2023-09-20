import pandas as pd

from dynamicio.io import LocalFileResource
from tests import constants
from tests.fixtures.schemas import SampleSchema


def test_read(test_df, file_name):
    resource = LocalFileResource(path=constants.TEST_FIXTURES / file_name)
    df = resource.read()
    pd.testing.assert_frame_equal(df, test_df)


def test_read_with_schema(test_df, file_name):
    resource = LocalFileResource(path=constants.TEST_FIXTURES / file_name, pa_schema=SampleSchema)
    df = resource.read()
    pd.testing.assert_frame_equal(df, test_df)


def test_write(test_df, tmpdir, file_name):
    resource = LocalFileResource(path=tmpdir / file_name)
    resource.write(test_df)
    # reading should probably not be done with the config here
    df = resource.read()
    pd.testing.assert_frame_equal(df, test_df)


# TODO: test float json thing
