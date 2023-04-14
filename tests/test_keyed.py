from unittest.mock import MagicMock, Mock

import pandas as pd
import pytest

from dynamicio import KeyedHandler
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
def create_mock_handler():
    def _create_mock_handler():
        mock_handler = MagicMock()
        mock_handler.return_value = mock_handler
        return mock_handler

    return _create_mock_handler


@pytest.fixture
def mock_resources(create_mock_handler):
    return {
        "default": (create_mock_handler(), MockConfig()),
        "foo": (create_mock_handler(), MockConfig()),
        "bar": (create_mock_handler(), MockConfig()),
    }


def test_inject(mock_resources):
    keyed_resource = KeyedHandler(mock_resources)
    keyed_resource = keyed_resource.inject(foo="bar")
    for key in mock_resources.keys():
        assert keyed_resource.keyed_build_configs[key][1].injected


def test_resource_read(mock_resources):
    keyed_resource = KeyedHandler(mock_resources)
    keyed_resource.read()

    handler = keyed_resource.keyed_build_configs[keyed_resource.key][0]
    config = keyed_resource.keyed_build_configs[keyed_resource.key][1]
    handler.assert_called_once_with(config, None)
    handler.read.assert_called_once_with()


def test_resource_write(mock_resources):
    keyed_resource = KeyedHandler(mock_resources)
    sample_df = pd.DataFrame([{"id": 4, "value": "qux"}])
    keyed_resource.write(sample_df)

    handler = keyed_resource.keyed_build_configs[keyed_resource.key][0]
    config = keyed_resource.keyed_build_configs[keyed_resource.key][1]
    handler.assert_called_once_with(config, None)
    handler.write.assert_called_once_with(sample_df)


def test_resource_read_with_schema(mock_resources):
    keyed_resource = KeyedHandler(mock_resources, SampleSchema)
    keyed_resource.read()

    handler = keyed_resource.keyed_build_configs[keyed_resource.key][0]
    config = keyed_resource.keyed_build_configs[keyed_resource.key][1]
    handler.assert_called_once_with(config, SampleSchema)
    handler.read.assert_called_once_with()


def test_resource_write_with_schema(mock_resources):
    keyed_resource = KeyedHandler(mock_resources, SampleSchema)
    sample_df = pd.DataFrame([{"id": 4, "value": "qux"}])
    keyed_resource.write(sample_df)

    handler = keyed_resource.keyed_build_configs[keyed_resource.key][0]
    config = keyed_resource.keyed_build_configs[keyed_resource.key][1]
    handler.assert_called_once_with(config, SampleSchema)
    handler.write.assert_called_once_with(sample_df)
