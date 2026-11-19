# pylint: disable=missing-module-docstring
import pytest

from dynamicio.engines import EngineSetting


@pytest.fixture(autouse=True)
def default_engine():
    """Start and end each test on the pandas engine, not frozen: the engine is one setting for the process."""
    EngineSetting.reset()
    yield
    EngineSetting.reset()
