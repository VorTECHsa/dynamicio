# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, R0801

from pandera import SchemaModel
from pandera.typing import Series


class SampleSchema(SchemaModel):
    id: Series[int]
    bar: Series[int]
    foo_name: Series[str]


class PgSampleSchema(SchemaModel):
    id: Series[str]
    foo: Series[str]
    bar: Series[int]
    baz: Series[str]
