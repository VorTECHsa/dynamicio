from enum import Enum

import pandas as pd
import pandera as pa
from pandera import SchemaModel, extensions
from pandera.typing import Series


# TODO: use legacy names only for migration? Does it even matter?
class Metric(str, Enum):
    MIN = "Min"
    MAX = "Max"
    MEAN = "Mean"
    STD = "Std"
    VARIANCE = "Variance"
    COUNTS = "Counts"
    UNIQUE_COUNTS = "UniqueCounts"
    COUNTS_PER_LABEL = "CountsPerLabel"


# TODO: figure out how to import this into consumer code
@extensions.register_check_method(statistics=["metrics"])
def log_statistics(pandas_obj, *, metrics):
    """
    Implements column-level data metrics as a workaround through custom metrics
    """

    for metric in metrics:
        # TODO: replace with logging
        print(f"Computing {metric} for {pandas_obj.name}")

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

        print(f"{metric} for {pandas_obj.name} is {computed_metric}")

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


# TODO: doesn't actually work
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


class Schema(SchemaModel):
    col1: Series[str] = pa.Field()
    col2: Series[int] = pa.Field(
        log_statistics={"metrics": [Metric.MIN, Metric.MEAN, Metric.STD, Metric.UNIQUE_COUNTS]}
    )


if __name__ == "__main__":
    data = pd.DataFrame({"col1": ["value"] * 5, "col2": range(5)})

    Schema.validate(data)
