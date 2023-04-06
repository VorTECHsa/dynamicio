import pandas as pd

from dynamicio.handlers import ParquetFileResource
from tests.constants import TEST_RESOURCES


def test_parquet_resource_read():
    test_path = TEST_RESOURCES / "data/input/parquet_sample.parquet"
    resource = ParquetFileResource(path=test_path, allow_no_schema=True)
    df = resource.read()
    target_df = pd.read_parquet(test_path)
    pd.testing.assert_frame_equal(df, target_df)


def test_parquet_resource_write(output_dir_path):
    test_path = output_dir_path / "test_parquet_resource_write.parquet"
    resource = ParquetFileResource(path=test_path, allow_no_schema=True)
    df = pd.DataFrame({"A": [1, 2, 3], "B": ["a", "b", "c"]})
    resource.write(df)
    target_df = pd.read_parquet(test_path)
    pd.testing.assert_frame_equal(df, target_df)
