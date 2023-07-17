import pandas as pd

from dynamicio import S3HdfResource
from tests.resources.schemas import SampleSchema


def test_resource_read(s3_stubber, hdf_s3_resource, hdf_df, mock_s3_named_file_reader):
    df = hdf_s3_resource.read()
    pd.testing.assert_frame_equal(df, hdf_df)


def test_resource_read_with_schema(s3_stubber, hdf_s3_config, hdf_df, mock_s3_named_file_reader):
    df = S3HdfResource(**hdf_s3_config, pa_schema=SampleSchema).read()
    pd.testing.assert_frame_equal(df, hdf_df)


def test_resource_read_no_disk_space(s3_stubber, hdf_s3_config, hdf_df, mock_s3_reader):
    hdf_s3_config["force_read_to_memory"] = True
    df = S3HdfResource(**hdf_s3_config).read()
    pd.testing.assert_frame_equal(df, hdf_df)


def test_resource_write(s3_stubber, hdf_df, mock_s3_writer, tmp_path):
    tmp_location = tmp_path / "sample_file.hdf"
    resource = S3HdfResource(
        bucket="my_bucket",
        path=tmp_location,
    )
    resource.write(hdf_df)
    df = pd.read_hdf(tmp_location)
    pd.testing.assert_frame_equal(df, hdf_df)
