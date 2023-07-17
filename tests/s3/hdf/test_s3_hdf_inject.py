import pandas as pd
import pytest

from dynamicio import S3HdfResource
from dynamicio.inject import InjectionError
from tests.resources.schemas import SampleSchema


def test_resource_read_inject_success(s3_stubber, hdf_s3_resource, hdf_df, mock_s3_named_file_reader):
    hdf_s3_resource.bucket = "{var1}"
    hdf_s3_resource.path = "{var2}"
    hdf_s3_resource = hdf_s3_resource.inject(var1="my_bucket", var2="some_file.hdf")
    hdf_s3_resource.read()


def test_resource_read_inject_fail(s3_stubber, hdf_s3_resource, hdf_df, mock_s3_named_file_reader):
    hdf_s3_resource.path = "{var2}"
    with pytest.raises(InjectionError):
        hdf_s3_resource.read()

    hdf_s3_resource.bucket = "{var1}"
    hdf_s3_resource.path = "some_path"
    with pytest.raises(InjectionError):
        hdf_s3_resource.read()


def test_resource_write_inject_success(s3_stubber, hdf_df, mock_s3_writer, tmp_path):
    tmp_location = tmp_path / "sample_file.hdf"
    resource = S3HdfResource(
        bucket="{var1}",
        path=tmp_path / "{var2}",
    )
    resource = resource.inject(var1="my_bucket", var2="sample_file.hdf")
    resource.write(hdf_df)
    assert tmp_location == resource.path


def test_resource_write_inject_fail(s3_stubber, hdf_df, mock_s3_writer, tmp_path):
    tmp_location = tmp_path / "sample_file.hdf"
    resource = S3HdfResource(
        bucket="{var1}",
        path=tmp_path / "{var2}",
    )
    with pytest.raises(InjectionError):
        resource.write(hdf_df)
