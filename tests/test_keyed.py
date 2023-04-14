from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest

from dynamicio import KeyedResource
from tests.resources.schemas import SampleSchema

sample_df = pd.DataFrame([{"id": 1, "value": "foo"}])


@pytest.fixture
def mock_resource_class():
    resource = MagicMock()
    resource.read.return_value = sample_df
    return resource


class MockConfig(Mock):
    """MockConfig class for testing."""

    def __init__(self):
        """Initialize the MockConfig."""
        super().__init__()
        self.injected = False

    def inject(self, **kwargs) -> "MockConfig":
        """Inject variables. Immutable."""
        self.injected = True
        return self

    def check_injections(self) -> None:
        """Check that all injections have been completed. Raise InjectionError if not."""
        if not self.injected:
            raise ValueError("Not injected")


sample_df = pd.DataFrame([{"id": 1, "value": "foo"}])


@pytest.fixture
def create_mock_resource():
    def _create_mock_resource():
        mock_resource = MagicMock()
        mock_resource.return_value = mock_resource
        return mock_resource

    return _create_mock_resource


@pytest.fixture
def mock_resources(create_mock_resource):
    return {
        "default": (create_mock_resource(), MockConfig()),
        "foo": (create_mock_resource(), MockConfig()),
        "bar": (create_mock_resource(), MockConfig()),
    }


def test_inject(mock_resources):
    keyed_resource = KeyedResource(mock_resources)
    keyed_resource = keyed_resource.inject(foo="bar")
    for key in mock_resources.keys():
        assert keyed_resource.keyed_build_configs[key][1].injected


def test_resource_read(mock_resources):
    keyed_resource = KeyedResource(mock_resources)
    keyed_resource.read()

    resource = keyed_resource.keyed_build_configs[keyed_resource.key][0]
    config = keyed_resource.keyed_build_configs[keyed_resource.key][1]
    resource.assert_called_once_with(config, None)
    resource.read.assert_called_once_with()


def test_resource_write(mock_resources):
    keyed_resource = KeyedResource(mock_resources)
    sample_df = pd.DataFrame([{"id": 4, "value": "qux"}])
    keyed_resource.write(sample_df)

    resource = keyed_resource.keyed_build_configs[keyed_resource.key][0]
    config = keyed_resource.keyed_build_configs[keyed_resource.key][1]
    resource.assert_called_once_with(config, None)
    resource.write.assert_called_once_with(sample_df)


def test_resource_read_with_schema(mock_resources):
    keyed_resource = KeyedResource(mock_resources, SampleSchema)
    keyed_resource.read()

    resource = keyed_resource.keyed_build_configs[keyed_resource.key][0]
    config = keyed_resource.keyed_build_configs[keyed_resource.key][1]
    resource.assert_called_once_with(config, SampleSchema)
    resource.read.assert_called_once_with()


def test_resource_write_with_schema(mock_resources):
    keyed_resource = KeyedResource(mock_resources, SampleSchema)
    sample_df = pd.DataFrame([{"id": 4, "value": "qux"}])
    keyed_resource.write(sample_df)

    resource = keyed_resource.keyed_build_configs[keyed_resource.key][0]
    config = keyed_resource.keyed_build_configs[keyed_resource.key][1]
    resource.assert_called_once_with(config, SampleSchema)
    resource.write.assert_called_once_with(sample_df)
