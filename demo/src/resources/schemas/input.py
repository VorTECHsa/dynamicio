"""Models of the datasets the staging runner reads (`definitions/input.yaml`). Declaration only: dynamicio runs the validations and metrics."""

import pandera.pandas as pa
from pandera.typing import Series

from dynamicio.schema import Metric, metrics


class Bar(pa.DataFrameModel):
    """The `bar` dataset."""

    column_a: Series[str] = pa.Field(nullable=True, unique=True, metadata=metrics(Metric.COUNTS))
    column_b: Series[str] = pa.Field(nullable=False, metadata=metrics(Metric.COUNTS_PER_LABEL))
    column_c: Series[float] = pa.Field(nullable=True, gt=1000)
    column_d: Series[float] = pa.Field(nullable=True, lt=1000, metadata=metrics(Metric.MIN, Metric.MAX, Metric.MEAN, Metric.STD, Metric.VARIANCE))


class Orders(pa.DataFrameModel):
    """The `orders` dataset."""

    order_id: Series[int] = pa.Field(nullable=True, unique=True, metadata=metrics(Metric.COUNTS))
    customer: Series[str] = pa.Field(nullable=False)
    status: Series[str] = pa.Field(nullable=True)


class Foo(pa.DataFrameModel):
    """The `foo` dataset."""

    column_a: Series[str] = pa.Field(nullable=True, unique=True, metadata=metrics(Metric.COUNTS))
    column_b: Series[str] = pa.Field(nullable=False, metadata=metrics(Metric.COUNTS_PER_LABEL))
    column_c: Series[float] = pa.Field(nullable=True, gt=1000)
    column_d: Series[float] = pa.Field(nullable=True, lt=1000, metadata=metrics(Metric.MIN, Metric.MAX, Metric.MEAN, Metric.STD, Metric.VARIANCE))
