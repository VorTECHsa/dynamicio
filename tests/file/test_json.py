import pandas as pd
import pytest

from dynamicio.handlers.file import JsonFileResource
from tests import constants
from tests.resources.schemas import SampleSchema

sample_path = constants.TEST_RESOURCES / "data/input/json_sample.json"


@pytest.fixture()
def json_file_resource() -> JsonFileResource:
    return JsonFileResource(
        path=sample_path,
        allow_no_schema=True,
    )


@pytest.fixture()
def json_df(json_file_resource) -> pd.DataFrame:
    return pd.read_json(sample_path)


def test_json_resource_read(json_file_resource, json_df):
    df = json_file_resource.read()
    pd.testing.assert_frame_equal(df, json_df)


def test_json_resource_read_with_schema(json_file_resource, json_df):
    df = json_file_resource.read(pa_schema=SampleSchema)
    pd.testing.assert_frame_equal(df, json_df)


def test_json_resource_write(json_df, tmpdir):
    tmp_location = tmpdir / "json_sample.json"
    resource = JsonFileResource(
        path=tmp_location,
        allow_no_schema=True,
    )
    resource.write(json_df)
    df = pd.read_json(tmp_location)
    pd.testing.assert_frame_equal(df, json_df)
