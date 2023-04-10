from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest

from dynamicio.handlers.file import BaseFileResource, ParquetFileResource
from dynamicio.inject import InjectionError


class MockBaseFileResource(BaseFileResource):
    """Mock base file resource."""

    mock_path_makedir_method = Mock()

    def _resource_write(self, df: pd.DataFrame) -> None:
        self.mock_path_makedir_method(self.path.parent)
        self._file_write_method(df, self.path, **self.kwargs)  # type: ignore

    class Config:
        arbitrary_types_allowed = True


class MockImplementedFileResource(MockBaseFileResource):
    """Mock parquet resource."""

    _file_read_method = Mock(return_value=pd.DataFrame())  # type: ignore
    _file_write_method = Mock(return_value=None)  # type: ignore


def test_file_resource_inject_read():
    resource = MockImplementedFileResource(path="foo/{bar}/baz.file", kwargs={"foo": "bar"}, allow_no_schema=True)
    resource = resource.inject(bar="boo")
    resource.read()

    resource._file_read_method.assert_called_once_with(Path("foo/boo/baz.file"), foo="bar")
    resource._file_read_method.reset_mock()

    resource = MockImplementedFileResource(path="foo/[[bar]]/baz.file", kwargs={"foo": "bar"}, allow_no_schema=True)
    resource = resource.inject(bar="car")
    resource.read()

    resource._file_read_method.assert_called_once_with(Path("foo/car/baz.file"), foo="bar")


def test_file_resource_inject_read_raises_on_incomplete_injection():
    resource = ParquetFileResource(path="foo/{bar}/{baz}", kwargs={"foo": "bar"}, allow_no_schema=True)
    resource = resource.inject(bar="baz")

    with pytest.raises(InjectionError):
        resource.read()

    resource = ParquetFileResource(path="foo/[[bar]]/[[baz]]", kwargs={"foo": "bar"}, allow_no_schema=True)
    resource = resource.inject(bar="baz")

    with pytest.raises(InjectionError):
        resource.read()


def test_file_resource_inject_write():
    df = pd.DataFrame()
    resource = MockImplementedFileResource(path="foo/{bar}/baz.file", kwargs={"foo": "bar"}, allow_no_schema=True)
    resource = resource.inject(bar="boo")
    resource.write(df)

    resource.mock_path_makedir_method.assert_called_once_with(Path("foo/boo"))
    resource._file_write_method.assert_called_once_with(df, Path("foo/boo/baz.file"), foo="bar")
    resource._file_write_method.reset_mock()

    resource = MockImplementedFileResource(path="foo/[[bar]]/baz.file", kwargs={"foo": "bar"}, allow_no_schema=True)
    resource = resource.inject(bar="car")
    resource.write(df)

    resource.mock_path_makedir_method.assert_called_once_with(Path("foo/car"))
    resource._file_write_method.assert_called_once_with(df, Path("foo/car/baz.file"), foo="bar")


def test_file_resource_inject_write_raises_on_incomplete_injection():
    resource = ParquetFileResource(path="foo/{bar}/{baz}", kwargs={"foo": "bar"}, allow_no_schema=True)
    resource = resource.inject(bar="baz")
    with pytest.raises(InjectionError):
        resource.write(pd.DataFrame())

    resource = ParquetFileResource(path="foo/[[bar]]/[[baz]]", kwargs={"foo": "bar"}, allow_no_schema=True)
    resource = resource.inject(bar="boo")
    with pytest.raises(InjectionError):
        resource.write(pd.DataFrame())
