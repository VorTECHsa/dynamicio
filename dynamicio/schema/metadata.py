"""What a column declares for dynamicio beyond Pandera: the metrics it logs and the validations Pandera has no ``Field`` argument for.

Pandera takes one free-form ``metadata`` dict per field. ``dynamicio.schema.Field`` writes dynamicio's part of it as a typed `DynamicioMetadata` under
the ``"dynamicio"`` key, from its named arguments (``Field(nullable=True, metrics=[Metric.MIN], max_null_fraction=0.2)``), and the model compiler reads it
back. These types are the contract between the two; models declare columns with ``Field`` and never build them by hand.
"""

import enum
from dataclasses import dataclass
from typing import Any, Mapping, Tuple

METADATA_KEY = "dynamicio"
# The key of the pandas dtype of a column of a polars model, in the field metadata (`Field(pandas_dtype="Int64")`)
PANDAS_DTYPE_KEY = "pandas_dtype"


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
