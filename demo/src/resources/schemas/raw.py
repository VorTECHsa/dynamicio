"""Models of the datasets the staging runner writes and the transform runner reads (`definitions/raw.yaml`). Declaration only: dynamicio runs the validations and metrics."""

from pandera.typing import Series

from dynamicio.schema import DataModel, Field


class StagedFooModel(DataModel, dataset="staged_foo"):
    """Staged foos: dtypes only, no validations or metrics."""

    column_a: Series[object] = Field(nullable=True)
    column_b: Series[object] = Field(nullable=True)
    column_c: Series[int] = Field(nullable=True)
    column_d: Series[int] = Field(nullable=True)


class StagedBarModel(DataModel, dataset="staged_bar"):
    """Staged bars: dtypes only, no validations or metrics."""

    column_a: Series[object] = Field(nullable=True)
    column_b: Series[object] = Field(nullable=True)
    column_c: Series[int] = Field(nullable=True)
    column_d: Series[int] = Field(nullable=True)
