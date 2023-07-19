from pandera import SchemaModel
from pandera.typing import Series

from dynamicio import KeyedResource, ParquetResource, S3ParquetResource


class MySchema(SchemaModel):
    column_a: Series[str]
    column_b: Series[str]


# <<< Currently - what previously was called 'environments'
thatfile_resource = KeyedResource(
    {
        "local": ParquetResource(path="that/long/filepath/that/long/filepath.parquet"),
        "cloud": S3ParquetResource(path="s3://bucket/that/long/filepath/thatfile.parquet"),
    },
    pa_schema=MySchema,
)

thatfile_resource.key = "cloud"

df = thatfile_resource.read()
# >>>

# <<< With uhura
with_uhura_resource = S3ParquetResource(path="s3://bucket/that/long/filepath/thatfile.parquet", pa_schema=MySchema)

df = with_uhura_resource.read()
# >>>


# Alternative, Uhura with explicit cache key
with_uhura_resource = S3ParquetResource(
    path="s3://bucket/that/long/filepath/thatfile.parquet",
    pa_schema=MySchema,
    cache_key="tests/uhura_data/thatfile.parquet",
)

df = with_uhura_resource.read()

# Alternative, Uhura with explicit cache key
with_uhura_resource = S3ParquetResource(
    path="s3://bucket/that/long/filepath/thatfile.parquet",
    pa_schema=MySchema,
    test_path="asdf/those_tests/thatfile.parquet",
)
