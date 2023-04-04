"""Example of reading and writing data using dynamicio."""
from pathlib import Path

from pandera import Field, Float, SchemaModel, String
from pandera.typing import Series

from dynamicio.handlers.file import ParquetFileResource


### Example 2 ###
class BarSchema(SchemaModel):
    column_a: Series[String] = Field(unique=True)
    column_b: Series[String] = Field(nullable=False)
    column_c: Series[Float] = Field(gt=1000)
    # column_d: Series[Float] = Field(lt=1000)

    class Config:
        strict = "filter"


TEST_RESOURCES = Path(__file__).parent / "data"
resource = ParquetFileResource(path=TEST_RESOURCES / "input/bar.parquet").read(pa_schema=BarSchema)
df = resource.read()
print(df)  # noqa: T201
