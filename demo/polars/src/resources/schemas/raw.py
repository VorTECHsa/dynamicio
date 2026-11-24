import polars as pl

from dynamicio.schema import Field
from dynamicio.schema.polars import DataModel


class StagedFooModel(DataModel, dataset='staged_foo'):
    """Migrated from the pandas model demo.pandas.src.resources.schemas.raw.StagedFooModel."""

    column_a: pl.String = Field(nullable=True)
    column_b: pl.String = Field(nullable=True)
    column_c: pl.Int64 = Field(nullable=True)
    column_d: pl.Int64 = Field(nullable=True)


class StagedBarModel(DataModel, dataset='staged_bar'):
    """Migrated from the pandas model demo.pandas.src.resources.schemas.raw.StagedBarModel."""

    column_a: pl.String = Field(nullable=True)
    column_b: pl.String = Field(nullable=True)
    column_c: pl.Int64 = Field(nullable=True)
    column_d: pl.Int64 = Field(nullable=True)
