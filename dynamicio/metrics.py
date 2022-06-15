"""A module responsible for metrics generation and logging."""
# pylint: disable=missing-function-docstring,missing-class-docstring
import json
import logging
import sys
from numbers import Number
from typing import Any, Dict, Mapping, Type

import pandas as pd  # type: ignore
from magic_logger import logger
from pythonjsonlogger import jsonlogger  # type: ignore

logHandler = logging.StreamHandler(sys.stdout)
formatter = jsonlogger.JsonFormatter()
logHandler.setFormatter(formatter)
logger.addHandler(logHandler)


__metrics__: Dict[str, Type["Metric"]] = {}


def get_metric(name: str) -> Type["Metric"]:
    return __metrics__[name]


def log_metric(dataset: str, column: str, metric: str, value: float):
    """Logs a metric in a structured way for a given dataset column.

    Args:
        dataset: The dataset for which the metric is logged
        column: Column for which the metric is logged
        metric: name fo the metric, e.g. "unique_vals"
        value: The metric's value, e.g. "10000"
    """
    logger.info(json.dumps({"message": "METRIC", "dataset": dataset, "column": column, "metric": metric, "value": float(value)}))


class Metric:
    """A base class for implementing metrics classes."""

    def __init__(self, dataset_name: str, df: pd.DataFrame, column: str):  # noqa
        self.dataset_name = dataset_name
        self.df = df
        self.column = column

    def __init_subclass__(cls):  # noqa
        __metrics__[cls.__name__] = cls
        assert "calculate_metric" in cls.__dict__

    def __call__(self) -> Any:  # noqa
        metric_value = self.calculate_metric()

        if isinstance(metric_value, Mapping):
            for entity in sorted(metric_value.keys()):  # pylint: disable=no-member
                column = metric_value[entity]  # pylint: disable=unsubscriptable-object
                log_metric(self.dataset_name, entity, self.metric_name, column)
        else:
            log_metric(dataset=self.dataset_name, column=self.column, metric=self.metric_name, value=metric_value)
        return metric_value

    @property
    def metric_name(self) -> str:
        """Retrieves the name of the metric from the class name.

        Returns:
            The name of the metric, e.g. "Min or Mean".
        """
        return self.__class__.__name__

    def calculate_metric(self) -> Any:
        """Dictates that subclasses need to implement this method.

        Returns:
            NotImplemented is returned if the method is not implemented, by the subclass
            inevitably pointing to the parent implementation.
        """
        return NotImplemented


class Min(Metric):
    """A metric instance that enables generating and returning the minimum value of a column."""

    def calculate_metric(self) -> Number:
        """Generate and return the minimum value of a column.

        Returns:
             The minimum value of a column.
        """
        return self.df[self.column].min()


class Max(Metric):
    """A metric instance that enables generating and returning the maximum value of a column."""

    def calculate_metric(self) -> Number:
        """Generate and return the maximum value of a column.

        Returns:
            The maximum value of a column.
        """
        return self.df[self.column].max()


class Mean(Metric):
    """A metric instance that enables generating and returning the mean value of a column."""

    def calculate_metric(self) -> Number:
        """Generate and return the mean value of a column.

        Returns:
            The mean value of a column.
        """
        return self.df[self.column].mean()


class Std(Metric):
    """A metric instance that enables generating and returning the standard deviation of a column."""

    def calculate_metric(self) -> Number:
        """Generate and return the standard deviation of a column.

        Returns:
            The standard deviation of a column.
        """
        return self.df[self.column].std()


class Variance(Metric):
    """A metric instance that generated and returns the variance of a column."""

    def calculate_metric(self) -> Number:
        """Generate and return the variance of a column.

        Returns:
            The variance of a column.
        """
        return self.df[self.column].var()


class Counts(Metric):
    """A metric instance that enables generating and returning the length of a column."""

    def calculate_metric(self) -> int:
        """Generate and return the length of a column.

        Returns:
            The length of a column.
        """
        return len(self.df[self.column])


class UniqueCounts(Metric):
    """A metric instance that enables generating and returning the unique values of a column."""

    def calculate_metric(self) -> int:
        """Generate and return the unique values of a column.

        Returns:
            The unique values of a column.
        """
        return len(self.df[self.column].unique())


class CountsPerLabel(Metric):
    """A metric instance that enables generating and returning the counts per label in a categorical column."""

    def calculate_metric(self) -> Mapping:
        """Generate and return the counts per label in a categorical column.

        Returns:
            The counts per label in a categorical column
        """
        column_vs_metric_value = self.df[self.column].value_counts().to_dict()
        label_vs_metric_value_with_column_prefix = {}
        for key in column_vs_metric_value.keys():
            new_key = self.column + "-" + key
            label_vs_metric_value_with_column_prefix[new_key] = column_vs_metric_value[key]
        return label_vs_metric_value_with_column_prefix
