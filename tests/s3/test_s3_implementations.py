from pathlib import Path

import pandas as pd

from tests.resources.schemas import SampleSchema

s3_bucket = "my_bucket"


# --- Tests ---


def test_s3_config_read(file_name, read_func, resource_class, input_path, test_df):
    resource = resource_class(
        bucket=s3_bucket,
        path=f"some/{file_name}",
    )
    expected_df = read_func(input_path / file_name)
    df = resource.read()
    pd.testing.assert_frame_equal(df, expected_df)


def test_s3_config_read_with_schema(file_name, read_func, resource_class, input_path, test_df):
    expected_df = read_func(input_path / file_name)

    df = resource_class(bucket=s3_bucket, path=f"some/{file_name}", pa_schema=SampleSchema).read()

    pd.testing.assert_frame_equal(df, expected_df)


def test_s3_config_write(read_func, write_func, resource_class, tmpdir, input_path, test_df):
    resource = resource_class(
        bucket=str(tmpdir / "bucket"),
        path=f"file.extension",
        allow_no_schema=True,
    )
    Path(resource.full_path).parent.mkdir(parents=True, exist_ok=True)
    resource.write(test_df)
    written_df = read_func(tmpdir / "bucket" / "file.extension")

    pd.testing.assert_frame_equal(test_df, written_df)
