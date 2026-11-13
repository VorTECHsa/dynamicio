"""Typed ``pa.Field(metadata=...)`` for dynamicio: the metrics a column logs and the validations Pandera has no ``Field`` argument for.

Pandera takes one free-form ``metadata`` dict per field. Writing dynamicio's part of it as a nested dict of strings gives an editor nothing to complete
and a typo nothing to fail on, so it is declared with these helpers instead::

    column_d: Series[float] = pa.Field(nullable=True, lt=1000, metadata=metrics(Metric.MIN, Metric.MAX) | null_fraction_below(0.2))

Every other validation is already a ``pa.Field`` argument (``gt``, ``lt``, ``isin``, ``in_range``, ``unique``, ``nullable``).
"""

import enum
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Tuple

METADATA_KEY = "dynamicio"


@enum.unique
class Metric(enum.Enum):
    """The metrics dynamicio can log for a column, as ``METRIC`` lines; the value is the name the log line reports."""

    MIN = "Min"
    MAX = "Max"
    MEAN = "Mean"
    STD = "Std"
    VARIANCE = "Variance"
    COUNTS = "Counts"
    UNIQUE_COUNTS = "UniqueCounts"
    COUNTS_PER_LABEL = "CountsPerLabel"


@dataclass(frozen=True)
class ValidationSpec:
    """A dynamicio validation (by the name of its validator) and the options it runs with."""

    name: str
    options: Mapping[str, Any]


@dataclass(frozen=True)
class DynamicioMetadata:
    """What a column declares for dynamicio: metrics to log and validations to run."""

    metrics: Tuple[Metric, ...] = ()
    validations: Tuple[ValidationSpec, ...] = ()


class ColumnMetadata(dict):
    """The ``metadata`` of a ``pa.Field``: a dict, as Pandera requires, that holds one `DynamicioMetadata` and combines with ``|``."""

    def __init__(self, payload: DynamicioMetadata):
        super().__init__({METADATA_KEY: payload})

    @property
    def payload(self) -> DynamicioMetadata:
        """The dynamicio part of the metadata."""
        return self[METADATA_KEY]

    def __or__(self, other: "ColumnMetadata") -> "ColumnMetadata":  # type: ignore[override]
        if not isinstance(other, ColumnMetadata):
            return NotImplemented
        return ColumnMetadata(
            DynamicioMetadata(
                metrics=self.payload.metrics + tuple(metric for metric in other.payload.metrics if metric not in self.payload.metrics),
                validations=self.payload.validations + other.payload.validations,
            )
        )


def metrics(*names: Metric) -> ColumnMetadata:
    """Metadata asking dynamicio to log these metrics for the column on every read and write that sets `log_schema_metrics`."""
    for name in names:
        if not isinstance(name, Metric):
            raise TypeError(f"metrics() takes members of dynamicio.schema.Metric (for example Metric.MIN), got {name!r}")
    return ColumnMetadata(DynamicioMetadata(metrics=tuple(dict.fromkeys(names))))


def null_fraction_below(threshold: float) -> ColumnMetadata:
    """Metadata validating that the fraction of nulls in the column is below ``threshold`` (0.2 allows up to, not including, 20% nulls).

    This is the validator named ``has_acceptable_percentage_of_nulls`` in error payloads and logs.
    """
    if not 0 < threshold < 1:
        raise ValueError(f"null_fraction_below() takes a fraction between 0 and 1, exclusive, got {threshold!r}")
    return ColumnMetadata(DynamicioMetadata(validations=(ValidationSpec("has_acceptable_percentage_of_nulls", {"threshold": threshold}),)))


def exactly_values(expected: Iterable[Any]) -> ColumnMetadata:
    """Metadata validating that the column holds exactly these values: none other, and none missing (``pa.Field(isin=...)`` only forbids others)."""
    return ColumnMetadata(DynamicioMetadata(validations=(ValidationSpec("is_in", {"categorical_values": set(expected), "match_all": False}),)))


def strictly_between(lower: Any, upper: Any, *, include_lower: bool = False, include_upper: bool = False) -> ColumnMetadata:
    """Metadata validating that the column lies between two bounds, which are not compared on definition (``pa.Field(in_range=...)`` does).

    Use it for bounds that are only known at import time, such as values read from settings. The bounds are excluded unless included explicitly.
    """
    options = {"lower": lower, "upper": upper, "include_left": include_lower, "include_right": include_upper}
    return ColumnMetadata(DynamicioMetadata(validations=(ValidationSpec("is_between", options),)))
