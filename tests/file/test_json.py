import pandas as pd
import pytest

from dynamicio.handlers.file import JsonFileResource
from tests import constants
from tests.resources.schemas import SampleSchema


@pytest.fixture()
def json_file_resource() -> JsonFileResource:
    return JsonFileResource(
        path=f"{constants.TEST_RESOURCES}/data/input/json_sample.json",
        allow_no_schema=True,
    )


@pytest.fixture()
def json_df(json_file_resource) -> pd.DataFrame:
    return json_file_resource.read()


@pytest.fixture()
def json_write_resource() -> JsonFileResource:
    return JsonFileResource(
        path=f"{constants.TEST_RESOURCES}/data/processed/json_sample.json",
        allow_no_schema=True,
    )


def test__resource_read(json_file_resource, json_df):
    df = json_file_resource.read()
    pd.testing.assert_frame_equal(df, json_df)


def test__resource_read_with_schema(json_file_resource, json_df):
    df = json_file_resource.read(pa_schema=SampleSchema)
    pd.testing.assert_frame_equal(df, json_df)


def test__resource_write(json_df, tmpdir):
    target_location = tmpdir / "json_sample.json"
    resource = JsonFileResource(
        path=target_location,
        allow_no_schema=True,
    )
    resource.write(json_df)
    df = pd.read_json(resource.path)
    pd.testing.assert_frame_equal(df, json_df)
