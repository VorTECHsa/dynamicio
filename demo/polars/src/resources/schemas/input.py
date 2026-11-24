import polars as pl

from dynamicio.schema import Field, Metric
from dynamicio.schema.polars import DataModel


class Bar(DataModel, dataset='bar', table='bar'):
    """Migrated from the pandas model demo.pandas.src.resources.schemas.input.Bar."""

    column_a: pl.String = Field(nullable=True, unique=True, metrics=[Metric.COUNTS])
    column_b: pl.String = Field(nullable=False, metrics=[Metric.COUNTS_PER_LABEL])
    column_c: pl.Float64 = Field(nullable=True, gt=1000)
    column_d: pl.Float64 = Field(nullable=True, lt=1000, metrics=[Metric.MIN, Metric.MAX, Metric.MEAN, Metric.STD, Metric.VARIANCE])


class Orders(DataModel, dataset='orders'):
    """Migrated from the pandas model demo.pandas.src.resources.schemas.input.Orders."""

    order_id: pl.Int64 = Field(nullable=True, unique=True, metrics=[Metric.COUNTS])
    customer: pl.String = Field(nullable=False)
    status: pl.String = Field(nullable=True)


class Foo(DataModel, dataset='foo'):
    """Migrated from the pandas model demo.pandas.src.resources.schemas.input.Foo."""

    column_a: pl.String = Field(nullable=True, unique=True, metrics=[Metric.COUNTS])
    column_b: pl.String = Field(nullable=False, metrics=[Metric.COUNTS_PER_LABEL])
    column_c: pl.Float64 = Field(nullable=True, gt=1000)
    column_d: pl.Float64 = Field(nullable=True, lt=1000, metrics=[Metric.MIN, Metric.MAX, Metric.MEAN, Metric.STD, Metric.VARIANCE])
