from unittest.mock import ANY, Mock

import pandas as pd
import pytest

from dynamicio.handlers.s3.file import BaseS3Resource
from dynamicio.inject import InjectionError


class MockBaseS3Resource(BaseS3Resource):
    """Mock base file resource."""

    mock_path_makedir_method = Mock()

    class Config:
        arbitrary_types_allowed = True


class MockImplementedS3Resource(MockBaseS3Resource):
    """Mock parquet resource."""

    _file_read_method = Mock(return_value=pd.DataFrame())  # type: ignore
    _file_write_method = Mock(return_value=None)  # type: ignore


def test_s3_resource_inject_read(s3_stubber, s3_named_file_reader, mock_reader):
    resource = MockImplementedS3Resource(
        bucket="[[ bucket_var ]]", path="foo/{bar}/baz.file", kwargs={"foo": "bar"}, allow_no_schema=True
    )
    resource = resource.inject(bar="boo", bucket_var="my_bucket")
    resource.read()

    mock_reader.assert_called_once_with(ANY, "my_bucket", "foo/boo/baz.file")
    resource._file_read_method.assert_called_once_with("mock_name", foo="bar")
    mock_reader.reset_mock()
    resource._file_read_method.reset_mock()


def test_s3_resource_inject_read_no_disk_space():
    resource = MockImplementedS3Resource(
        no_disk_space=True,
        bucket="my_bucket",
        path="foo/{bar}/[[baz]].file",
        kwargs={"foo": "bar"},
        allow_no_schema=True,
    )
    resource = resource.inject(bar="boo", baz="baz")
    resource.read()

    resource._file_read_method.assert_called_once_with("s3://my_bucket/foo/boo/baz.file", foo="bar")
    resource._file_read_method.reset_mock()


def test_s3_resource_inject_read_raises_on_incomplete_injection():
    resource = MockImplementedS3Resource(
        bucket="my_bucket", path="foo/[[ bar ]]/{baz}", kwargs={"foo": "bar"}, allow_no_schema=True
    )
    resource = resource.inject(bar="baz")

    with pytest.raises(InjectionError):
        resource.read()

    resource = MockImplementedS3Resource(
        bucket="[[ bucket_var ]]", path="foo/{}/baz.aoeu", kwargs={"foo": "bar"}, allow_no_schema=True
    )
    resource = resource.inject(bar="baz")

    with pytest.raises(InjectionError):
        resource.read()


def test_s3_resource_inject_write():
    df = pd.DataFrame()
    resource = MockImplementedS3Resource(
        bucket="my_bucket", path="foo/{bar}/baz.file", kwargs={"foo": "bar"}, allow_no_schema=True
    )
    resource = resource.inject(bar="boo")
    resource.write(df)

    resource._file_write_method.assert_called_once_with(df, "s3://my_bucket/foo/boo/baz.file", foo="bar")
    resource._file_write_method.reset_mock()

    resource = MockImplementedS3Resource(
        bucket="my_bucket", path="foo/[[bar]]/baz.file", kwargs={"foo": "bar"}, allow_no_schema=True
    )
    resource = resource.inject(bar="car")
    resource.write(df)

    resource._file_write_method.assert_called_once_with(df, "s3://my_bucket/foo/car/baz.file", foo="bar")


def test_s3_resource_inject_write_raises_on_incomplete_injection():
    resource = MockImplementedS3Resource(
        bucket="my_bucket", path="foo/{bar}/{baz}", kwargs={"foo": "bar"}, allow_no_schema=True
    )
    resource = resource.inject(bar="baz")
    with pytest.raises(InjectionError):
        resource.write(pd.DataFrame())

    resource = MockImplementedS3Resource(
        bucket="my_bucket", path="foo/[[bar]]/[[baz]]", kwargs={"foo": "bar"}, allow_no_schema=True
    )
    resource = resource.inject(bar="boo")
    with pytest.raises(InjectionError):
        resource.write(pd.DataFrame())
