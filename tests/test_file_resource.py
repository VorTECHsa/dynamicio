import pandas as pd
import pytest
from pandera import SchemaModel
from pandera.typing import Series

from dynamicio.io.file import FileResource
from tests.constants import TEST_FIXTURES


@pytest.fixture
def test_df_with_float():
    return pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"], "c": [True, False, True], "d": [7.0, 8.0, 9.0]})


@pytest.fixture(
    params=[
        "parquet",
        "csv",
        "json",
        "hdf",
    ]
)
def file_type(request):
    return request.param


def test_read_file_resource(file_type, test_df):
    resource = FileResource(path=TEST_FIXTURES / f"sample.{file_type}")
    df = resource.read()
    pd.testing.assert_frame_equal(df, test_df)


def test_read_write_file_resource(tmpdir, file_type, test_df):
    resource = FileResource(path=tmpdir / f"sample_with_float.{file_type}")
    resource.write(test_df)
    df = resource.read()
    pd.testing.assert_frame_equal(df, test_df)


def test_read_write_file_resource_with_schema(tmpdir, test_df, file_type):
    class SampleSchema(SchemaModel):
        a: Series[int]
        b: Series[str]
        c: Series[bool]

    resource = FileResource(path=tmpdir / f"sample.{file_type}", pa_schema=SampleSchema)
    resource.write(test_df)
    df = resource.read()
    pd.testing.assert_frame_equal(df, test_df)


def test_read_write_file_resource_with_float_fails_json(tmpdir, test_df_with_float):
    """This test would fail on json since the floats would get read back in as ints"""

    resource = FileResource(path=tmpdir / f"sample_with_float.json")
    resource.write(test_df_with_float)
    df = resource.read()
    with pytest.raises(AssertionError):
        pd.testing.assert_frame_equal(df, test_df_with_float)


def test_read_write_file_resource_with_float_with_schema(tmpdir, test_df_with_float, file_type):
    """This test would fail on json since the floats would get read back in as ints"""

    class SampleSchema(SchemaModel):
        a: Series[int]
        b: Series[str]
        c: Series[bool]
        d: Series[float]

        class Config:
            coerce = True

    resource = FileResource(path=tmpdir / f"sample_with_float.{file_type}", pa_schema=SampleSchema)
    resource.write(test_df_with_float)
    df = resource.read()
    pd.testing.assert_frame_equal(df, test_df_with_float)
