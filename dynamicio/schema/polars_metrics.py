"""The metrics of the polars engine: the same names, values and ``METRIC`` log lines as ``dynamicio.schema.metrics``.

A polars metric is an expression that gives one value for a column. The engine computes the metrics of all columns in one ``select``, and logs each value
with ``dynamicio.schema.metrics.log_metric``, so a patch of ``dynamicio.metrics.log_metric`` applies to both engines. The module needs polars, so only the
polars engine imports it.
"""

import math
from typing import Any, Callable, Dict, Mapping, Optional

import polars as pl

from dynamicio.schema import metrics


class PolarsMetrics:
    """The polars metrics, by the names that a schema uses. A consumer adds one with ``@PolarsMetrics.metric("Name")``."""

    ALL: Dict[str, Callable[[str], pl.Expr]] = {}

    @classmethod
    def metric(cls, name: str) -> Callable[[Callable[[str], pl.Expr]], Callable[[str], pl.Expr]]:
        """Register a polars metric: a function of the column name that gives an expression with one value, or a list of ``{label, count}`` structs."""

        def register(func: Callable[[str], pl.Expr]) -> Callable[[str], pl.Expr]:
            if name in cls.ALL:
                raise ValueError(f"a polars metric named {name} exists")
            cls.ALL[name] = func
            return func

        return register

    @staticmethod
    def value_of(value: Any) -> Any:
        """The value of a metric as pandas gives it: polars gives None where pandas gives NaN, such as the ``Min`` of an empty or all-null column."""
        return math.nan if value is None else value

    @classmethod
    def log(cls, dataset: str, column: str, name: str, value: Any, dtype: Optional[pl.DataType] = None) -> Any:
        """Log a metric value as ``Metric.__call__`` does in pandas: a mapping logs one line for each label, in sorted order.

        A list is the ``value_counts`` of the column, and gives the ``CountsPerLabel`` mapping. ``dtype`` is the polars dtype of the column. The metric
        value is returned.
        """
        if isinstance(value, list):
            value = cls.counts_per_label(column, value, dtype)
        if isinstance(value, Mapping):
            for entity in sorted(value.keys()):
                metrics.log_metric(dataset, entity, name, value[entity])
        else:
            value = cls.value_of(value)
            metrics.log_metric(dataset=dataset, column=column, metric=name, value=value)
        return value

    @staticmethod
    def counts_per_label(column: str, counts: Any, dtype: Optional[pl.DataType] = None) -> Dict[str, int]:
        """The ``CountsPerLabel`` mapping of the collected label counts: ``column-label`` to the count of the label. Nulls are not labels.

        Each entry is a ``{label, count}`` struct, read by position: the count field has the name of ``count_field``. The categories of an ``Enum`` that
        are not in the column have a count of 0, as the categories of a pandas ``category`` in ``value_counts``.
        """
        found = dict.fromkeys(dtype.categories if isinstance(dtype, pl.Enum) else [], 0)
        found.update(tuple(entry.values()) for entry in counts)
        return {column + "-" + label: count for label, count in found.items()}

    @staticmethod
    def count_field(column: str) -> str:
        """The name of the count field of the label counts of a column. It is not the name of the column, so a column named ``count`` has its counts."""
        return column + "-count"


PolarsMetrics.ALL.update(
    {
        "Min": lambda column: pl.col(column).min(),
        "Max": lambda column: pl.col(column).max(),
        "Mean": lambda column: pl.col(column).mean(),
        "Std": lambda column: pl.col(column).std(ddof=1),
        "Variance": lambda column: pl.col(column).var(ddof=1),
        "Counts": lambda column: pl.col(column).len(),
        "UniqueCounts": lambda column: pl.col(column).n_unique(),
        "CountsPerLabel": lambda column: pl.col(column).drop_nulls().value_counts(name=PolarsMetrics.count_field(column)).implode(),
    }
)
