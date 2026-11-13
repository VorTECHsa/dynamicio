"""Models of the datasets the transform runner writes (`definitions/processed.yaml`). Declaration only: dynamicio runs the validations and metrics."""

from pandera.typing import Series

from demo.src.config.environment import LOWER_THAN_LIMIT
from dynamicio.schema import DataModel, Field, Metric


class FinalBar(DataModel, dataset="final_bar", validate=True, log_metrics=True):
    """The `final_bar` dataset."""

    column_a: Series[str] = Field(nullable=True, unique=True, metrics=[Metric.COUNTS])
    column_b: Series[str] = Field(nullable=False, metrics=[Metric.COUNTS_PER_LABEL])
    column_c: Series[float] = Field(nullable=True, gt=1000)
    column_d: Series[float] = Field(nullable=True, lt=LOWER_THAN_LIMIT, metrics=[Metric.MIN, Metric.MAX, Metric.MEAN, Metric.STD, Metric.VARIANCE])


class FinalFoo(DataModel, dataset="final_foo", validate=True, log_metrics=True):
    """The `final_foo` dataset."""

    column_a: Series[str] = Field(nullable=True, unique=True, metrics=[Metric.COUNTS])
    column_b: Series[str] = Field(nullable=False, metrics=[Metric.COUNTS_PER_LABEL])
    column_c: Series[float] = Field(nullable=True, gt=1000)
    column_d: Series[float] = Field(nullable=True, lt=1000, metrics=[Metric.MIN, Metric.MAX, Metric.MEAN, Metric.STD, Metric.VARIANCE])
