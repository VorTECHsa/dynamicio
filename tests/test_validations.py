import pandas as pd
import pytest
from pandera import Field, SchemaModel
from pandera.errors import SchemaError
from pandera.typing import Series

from dynamicio import ParquetConfig, ParquetHandler
from tests.constants import TEST_RESOURCES


class ParquetSampleSchema(SchemaModel):
    """Schema for sample parquet file."""

    id: Series[int]
    foo_name: Series[str]
    bar: Series[int]


def test_parquet_resource_read_with_schema():
    test_path = TEST_RESOURCES / "data/input/parquet_sample.parquet"

    resource = ParquetConfig(path=test_path)
    handler = ParquetHandler(resource, ParquetSampleSchema)
    df = handler.read()

    target_df = pd.read_parquet(test_path)
    pd.testing.assert_frame_equal(df, target_df)


def test_parquet_resource_write_with_schema(output_dir_path):
    input_path = TEST_RESOURCES / "data/input/parquet_sample.parquet"
    output_path = output_dir_path / "test_parquet_resource_write.parquet"
    in_memory_df = pd.read_parquet(input_path)

    config = ParquetConfig(path=output_path)
    handler = ParquetHandler(config, ParquetSampleSchema)
    handler.write(in_memory_df)

    target_df = pd.read_parquet(output_path)
    pd.testing.assert_frame_equal(in_memory_df, target_df)


def test_parquet_resource_read_with_schema_fails_validation():
    class ParquetSampleSchema(SchemaModel):
        """Schema for sample parquet file."""

        id: Series[int] = Field(gt=3)
        foo_name: Series[str]
        bar: Series[int]

    test_path = TEST_RESOURCES / "data/input/parquet_sample.parquet"
    config = ParquetConfig(path=test_path)
    handler = ParquetHandler(config, ParquetSampleSchema)
    with pytest.raises(SchemaError):
        handler.read()


def test_parquet_resource_read_with_schema_pandera_config_is_applied():
    class ParquetSampleSchema(SchemaModel):
        """Schema for sample parquet file."""

        id: Series[int]
        foo_name: Series[str]

        class Config:
            strict = True

    test_path = TEST_RESOURCES / "data/input/parquet_sample.parquet"
    config = ParquetConfig(path=test_path)
    handler = ParquetHandler(config, ParquetSampleSchema)
    with pytest.raises(SchemaError):
        handler.read()
