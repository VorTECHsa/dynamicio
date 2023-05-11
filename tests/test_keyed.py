from unittest.mock import MagicMock

import pandas as pd
import pytest

from dynamicio import KeyedResource
from tests.resources.schemas import SampleSchema

sample_df = pd.DataFrame([{"id": 1, "value": "foo"}])


@pytest.fixture
def mock_resources():
    return {
        "default": MagicMock(),
        "foo": MagicMock(),
        "bar": MagicMock(),
    }


def test_inject(mock_resources):
    keyed_resource = KeyedResource(mock_resources)
    keyed_resource = keyed_resource.inject(foo="bar")
    for key in mock_resources.keys():
        assert keyed_resource.keyed_build_configs[key].injected


def test_resource_read(mock_resources):
    keyed_resource = KeyedResource(mock_resources)
    keyed_resource.read()

    resource = keyed_resource.keyed_build_configs[keyed_resource.key]
    resource.read.assert_called_once_with()


def test_resource_write(mock_resources):
    keyed_resource = KeyedResource(mock_resources)
    sample_df = pd.DataFrame([{"id": 4, "value": "qux"}])
    keyed_resource.write(sample_df)

    resource = keyed_resource.keyed_build_configs[keyed_resource.key]
    resource.write.assert_called_once_with(sample_df)


def test_resource_read_with_schema(mock_resources):
    keyed_resource = KeyedResource(mock_resources, SampleSchema)
    keyed_resource.read()

    resource = keyed_resource.keyed_build_configs[keyed_resource.key]
    resource.read.assert_called_once_with()


def test_resource_write_with_schema(mock_resources):
    keyed_resource = KeyedResource(mock_resources, SampleSchema)
    sample_df = pd.DataFrame([{"id": 4, "value": "qux"}])
    keyed_resource.write(sample_df)

    resource = keyed_resource.keyed_build_configs[keyed_resource.key]
    resource.write.assert_called_once_with(sample_df)
