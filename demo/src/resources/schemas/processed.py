"""Models of the datasets the transform runner writes (`definitions/processed.yaml`). Declaration only: dynamicio runs the validations and metrics."""

import pandera.pandas as pa
from pandera.typing import Series

from demo.src.config.environment import LOWER_THAN_LIMIT
from dynamicio.schema import Metric, metrics


class FinalBar(pa.DataFrameModel):
    """The `final_bar` dataset."""

    column_a: Series[str] = pa.Field(nullable=True, unique=True, metadata=metrics(Metric.COUNTS))
    column_b: Series[str] = pa.Field(nullable=False, metadata=metrics(Metric.COUNTS_PER_LABEL))
    column_c: Series[float] = pa.Field(nullable=True, gt=1000)
    column_d: Series[float] = pa.Field(nullable=True, lt=LOWER_THAN_LIMIT, metadata=metrics(Metric.MIN, Metric.MAX, Metric.MEAN, Metric.STD, Metric.VARIANCE))


class FinalFoo(pa.DataFrameModel):
    """The `final_foo` dataset."""

    column_a: Series[str] = pa.Field(nullable=True, unique=True, metadata=metrics(Metric.COUNTS))
    column_b: Series[str] = pa.Field(nullable=False, metadata=metrics(Metric.COUNTS_PER_LABEL))
    column_c: Series[float] = pa.Field(nullable=True, gt=1000)
    column_d: Series[float] = pa.Field(nullable=True, lt=1000, metadata=metrics(Metric.MIN, Metric.MAX, Metric.MEAN, Metric.STD, Metric.VARIANCE))
