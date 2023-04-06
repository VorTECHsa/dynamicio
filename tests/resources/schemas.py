# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, R0801

from pandera import SchemaModel
from pandera.typing import Series


class SomeParquetToRead(SchemaModel):
    id: Series[int]
    bar: Series[int]
    foo_name: Series[str]
