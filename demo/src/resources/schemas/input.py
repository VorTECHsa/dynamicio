"""Models of the datasets the staging runner reads (`definitions/input.yaml`). Declaration only: dynamicio runs the validations and metrics."""

from pandera.typing import Series

from dynamicio.schema import DataModel, Field, Metric


class Bar(DataModel, dataset="bar", validate=True, log_metrics=True):
    """The `bar` dataset."""

    column_a: Series[str] = Field(nullable=True, unique=True, metrics=[Metric.COUNTS])
    column_b: Series[str] = Field(nullable=False, metrics=[Metric.COUNTS_PER_LABEL])
    column_c: Series[float] = Field(nullable=True, gt=1000)
    column_d: Series[float] = Field(nullable=True, lt=1000, metrics=[Metric.MIN, Metric.MAX, Metric.MEAN, Metric.STD, Metric.VARIANCE])


class Orders(DataModel, dataset="orders"):
    """The `orders` dataset."""

    order_id: Series[int] = Field(nullable=True, unique=True, metrics=[Metric.COUNTS])
    customer: Series[str] = Field(nullable=False)
    status: Series[str] = Field(nullable=True)


class Foo(DataModel, dataset="foo", validate=True, log_metrics=True):
    """The `foo` dataset."""

    column_a: Series[str] = Field(nullable=True, unique=True, metrics=[Metric.COUNTS])
    column_b: Series[str] = Field(nullable=False, metrics=[Metric.COUNTS_PER_LABEL])
    column_c: Series[float] = Field(nullable=True, gt=1000)
    column_d: Series[float] = Field(nullable=True, lt=1000, metrics=[Metric.MIN, Metric.MAX, Metric.MEAN, Metric.STD, Metric.VARIANCE])
