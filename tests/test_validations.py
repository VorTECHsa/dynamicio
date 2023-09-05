import pandas as pd
import pytest
from pandera import SchemaModel
from pandera.errors import SchemaError
from pandera.typing import Series

from dynamicio import LocalFileResource
import tests.constants as constants
from tests.fixtures.schemas import SampleSchema

file_path = constants.TEST_FIXTURES / "sample.parquet"


def test_parquet_resource_read_with_schema():
    resource = LocalFileResource(path=file_path, pa_schema=SampleSchema)
    df = resource.read()

    target_df = pd.read_parquet(file_path)
    pd.testing.assert_frame_equal(df, target_df)


def test_parquet_resource_write_with_schema(tmpdir):
    output_path = tmpdir / "test_parquet_resource_write.parquet"
    in_memory_df = pd.read_parquet(file_path)

    resource = LocalFileResource(path=output_path, pa_schema=SampleSchema)
    resource.write(in_memory_df)

    target_df = pd.read_parquet(output_path)
    pd.testing.assert_frame_equal(in_memory_df, target_df)


def test_parquet_resource_read_with_schema_fails_validation():
    class FailingSchema(SchemaModel):
        z: Series[int]

    resource = LocalFileResource(path=file_path, pa_schema=FailingSchema)
    with pytest.raises(SchemaError):
        resource.read()


def test_parquet_resource_read_with_schema_pandera_config_is_applied():
    class FailingSchema(SchemaModel):
        a: Series[int]
        b: Series[str]

        class Config:
            strict = True

    resource = LocalFileResource(path=file_path, pa_schema=FailingSchema)
    with pytest.raises(SchemaError):
        resource.read()
