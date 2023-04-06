# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, R0801, W0621, W0212
from pathlib import Path

import pandas as pd
import pytest
from mock import call

from dynamicio.handlers.file import ParquetFileResource
from dynamicio.inject import InjectionError


def test_file_resource_inject_read(mocker):
    mock_function = mocker.patch(
        "dynamicio.handlers.file.ParquetFileResource._file_read_method", return_value=pd.DataFrame()
    )

    resource = ParquetFileResource(path="foo/{bar}/baz", kwargs={"foo": "bar"}, allow_no_schema=True)
    resource = resource.inject(bar="baz")
    resource.read()

    resource = ParquetFileResource(path="foo/[[bar]]/baz", kwargs={"foo": "bar"}, allow_no_schema=True)
    resource = resource.inject(bar="boo")
    resource.read()

    mock_function.assert_has_calls([call(Path("foo/baz/baz"), foo="bar"), call(Path("foo/boo/baz"), foo="bar")])


def test_file_resource_inject_read_raises_on_incomplete_injection():
    resource = ParquetFileResource(path="foo/{bar}/{baz}", kwargs={"foo": "bar"}, allow_no_schema=True)
    resource = resource.inject(bar="baz")

    with pytest.raises(InjectionError):
        resource.read()

    resource = ParquetFileResource(path="foo/[[bar]]/[[baz]]", kwargs={"foo": "bar"}, allow_no_schema=True)
    resource = resource.inject(bar="baz")

    with pytest.raises(InjectionError):
        resource.read()


def test_file_resource_inject_write(mocker):
    mock_function = mocker.patch(
        "dynamicio.handlers.file.ParquetFileResource._file_write_method", return_value=pd.DataFrame()
    )

    df = pd.DataFrame()
    resource = ParquetFileResource(path="foo/{bar}/baz", kwargs={"foo": "bar"}, allow_no_schema=True)
    resource = resource.inject(bar="baz")
    resource.write(df)

    resource = ParquetFileResource(path="foo/[[bar]]/baz", kwargs={"foo": "bar"}, allow_no_schema=True)
    resource = resource.inject(bar="boo")
    resource.write(df)

    mock_function.assert_has_calls([call(df, Path("foo/baz/baz"), foo="bar"), call(df, Path("foo/boo/baz"), foo="bar")])


def test_file_resource_inject_write_raises_on_incomplete_injection():
    resource = ParquetFileResource(path="foo/{bar}/{baz}", kwargs={"foo": "bar"}, allow_no_schema=True)
    resource = resource.inject(bar="baz")
    with pytest.raises(InjectionError):
        resource.write(pd.DataFrame())

    resource = ParquetFileResource(path="foo/[[bar]]/[[baz]]", kwargs={"foo": "bar"}, allow_no_schema=True)
    resource = resource.inject(bar="boo")
    with pytest.raises(InjectionError):
        resource.write(pd.DataFrame())
