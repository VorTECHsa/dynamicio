"""Models of the datasets the staging runner writes and the transform runner reads (`definitions/raw.yaml`). Declaration only: dynamicio runs the validations and metrics."""

import pandera.pandas as pa
from pandera.typing import Series


class StagedFooModel(pa.DataFrameModel):
    """Staged foos: dtypes only, no validations or metrics."""

    column_a: Series[object] = pa.Field(nullable=True)
    column_b: Series[object] = pa.Field(nullable=True)
    column_c: Series[int] = pa.Field(nullable=True)
    column_d: Series[int] = pa.Field(nullable=True)

    class Config:
        name = "staged_foo"


class StagedBarModel(pa.DataFrameModel):
    """Staged bars: dtypes only, no validations or metrics."""

    column_a: Series[object] = pa.Field(nullable=True)
    column_b: Series[object] = pa.Field(nullable=True)
    column_c: Series[int] = pa.Field(nullable=True)
    column_d: Series[int] = pa.Field(nullable=True)

    class Config:
        name = "staged_bar"
