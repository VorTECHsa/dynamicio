# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, R0801

from pandera import SchemaModel
from pandera.typing import Series


class SampleSchema(SchemaModel):
    a: Series[int]
    b: Series[str]
    c: Series[bool]
    # d: Series[float]
