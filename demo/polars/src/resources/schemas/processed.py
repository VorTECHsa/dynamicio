import polars as pl

from dynamicio.schema import Field, Metric
from dynamicio.schema.polars import DataModel


class FinalBar(DataModel, dataset='final_bar'):
    """Migrated from the pandas model demo.pandas.src.resources.schemas.processed.FinalBar."""

    column_a: pl.String = Field(nullable=True, unique=True, metrics=[Metric.COUNTS])
    column_b: pl.String = Field(nullable=False, metrics=[Metric.COUNTS_PER_LABEL])
    column_c: pl.Float64 = Field(nullable=True, gt=1000)
    column_d: pl.Float64 = Field(nullable=True, lt=1000, metrics=[Metric.MIN, Metric.MAX, Metric.MEAN, Metric.STD, Metric.VARIANCE])


class FinalFoo(DataModel, dataset='final_foo'):
    """Migrated from the pandas model demo.pandas.src.resources.schemas.processed.FinalFoo."""

    column_a: pl.String = Field(nullable=True, unique=True, metrics=[Metric.COUNTS])
    column_b: pl.String = Field(nullable=False, metrics=[Metric.COUNTS_PER_LABEL])
    column_c: pl.Float64 = Field(nullable=True, gt=1000)
    column_d: pl.Float64 = Field(nullable=True, lt=1000, metrics=[Metric.MIN, Metric.MAX, Metric.MEAN, Metric.STD, Metric.VARIANCE])
