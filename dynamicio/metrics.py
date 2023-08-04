import json
import logging
from enum import Enum
from typing import Mapping

import pandas as pd
from pandera import extensions

logger = logging.getLogger(__name__)


def log_metric(column: str, metric: str, value: float):
    """Logs a metric in a structured way for a given dataset column.

    Args:
        column: Column for which the metric is logged
        metric: name fo the metric, e.g. "unique_vals"
        value: The metric's value, e.g. "10000"
    """
    logger.info(
        json.dumps({"message": "METRIC", "column": column, "metric": metric, "value": float(value)})
    )


class Metric(str, Enum):
    MIN = "Min"
    MAX = "Max"
    MEAN = "Mean"
    STD = "Std"
    VARIANCE = "Variance"
    COUNTS = "Counts"
    UNIQUE_COUNTS = "UniqueCounts"
    COUNTS_PER_LABEL = "CountsPerLabel"


@extensions.register_check_method(statistics=["metrics"])
def log_statistics(pandas_obj, *, metrics):
    """
    Implements column-level data metrics as a workaround through custom metrics
    """

    col_name = str(pandas_obj.name)

    for metric in metrics:
        computed_metric = None

        if metric == Metric.MIN:
            computed_metric = calculate_min(pandas_obj)
        elif metric == Metric.MAX:
            computed_metric = calculate_max(pandas_obj)
        elif metric == Metric.MEAN:
            computed_metric = calculate_mean(pandas_obj)
        elif metric == Metric.STD:
            computed_metric = calculate_std(pandas_obj)
        elif metric == Metric.VARIANCE:
            computed_metric = calculate_variance(pandas_obj)
        elif metric == Metric.COUNTS:
            computed_metric = calculate_counts(pandas_obj)
        elif metric == Metric.UNIQUE_COUNTS:
            computed_metric = calculate_unique_counts(pandas_obj)
        elif metric == Metric.COUNTS_PER_LABEL:
            computed_metric = calculate_counts_per_label(pandas_obj)

        if isinstance(computed_metric, Mapping):
            for entity in sorted(computed_metric.keys()):  # pylint: disable=no-member
                value = computed_metric[entity]  # pylint: disable=unsubscriptable-object
                log_metric(column=col_name, metric=metric, value=value)
        else:
            log_metric(column=col_name, metric=metric, value=computed_metric)

    return True


def calculate_min(series: pd.Series) -> float:
    """Generate and return the minimum value of a column.

    Returns:
         The minimum value of a column.
    """
    return series.min()


def calculate_max(series: pd.Series) -> float:
    """Generate and return the maximum value of a column.

    Returns:
        The maximum value of a column.
    """
    return series.max()


def calculate_mean(series: pd.Series) -> float:
    """Generate and return the mean value of a column.

    Returns:
        The mean value of a column.
    """
    return series.mean()


def calculate_std(series: pd.Series) -> float:
    """Generate and return the standard deviation of a column.

    Returns:
        The standard deviation of a column.
    """
    return series.std()


def calculate_variance(series: pd.Series) -> float:
    """Generate and return the variance of a column.

    Returns:
        The variance of a column.
    """
    return series.var()


def calculate_counts(series: pd.Series) -> int:
    """Generate and return the length of a column.

    Returns:
        The length of a column.
    """
    return len(series)


def calculate_unique_counts(series: pd.Series) -> int:
    """Generate and return the unique values of a column.

    Returns:
        The unique values of a column.
    """
    return len(series.unique())


def calculate_counts_per_label(series: pd.Series) -> dict:
    """Generate and return the counts per label in a categorical column.

    Returns:
        The counts per label in a categorical column
    """
    column_vs_metric_value = series.value_counts().to_dict()
    label_vs_metric_value_with_column_prefix = {}
    for key in column_vs_metric_value.keys():
        new_key = str(series.name) + "-" + key
        label_vs_metric_value_with_column_prefix[new_key] = column_vs_metric_value[key]
    return label_vs_metric_value_with_column_prefix
