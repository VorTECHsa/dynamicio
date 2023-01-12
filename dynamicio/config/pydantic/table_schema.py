# pylint: disable=no-member, no-self-argument, unused-argument

"""This module defines Config schema for data source (pandas dataframe)"""

import enum
from typing import Mapping, Sequence

import pydantic


@enum.unique
class MetricsName(str, enum.Enum):
    """The list of valid metrics names."""

    # pylint: disable=invalid-name
    min = "Min"
    max = "Max"
    mean = "Mean"
    stddev = "Std"
    variance = "Variance"
    counts = "Counts"
    counts_per_label = "CountsPerLabel"
    unique_counts = "UniqueCounts"


@enum.unique
class ColumnType(str, enum.Enum):
    """The list of valid column types."""

    # pylint: disable=invalid-name
    object = "object"
    string = "string"
    array = "array"
    number = "number"

    float = "float"
    float32 = "float32"
    float64 = "float64"
    double = "double"

    int = "int"
    integer = "integer"

    int8 = "int8"
    int32 = "int32"
    int64 = "int64"

    Int8 = "Int8"
    Int32 = "Int32"
    Int64 = "Int64"

    uint8 = "uint8"
    uint32 = "uint32"
    uint64 = "uint64"

    bool = "bool"
    boolean = "boolean"

    datetime64_ns = "datetime64[ns]"
    datetime64_ns_utc = "datetime64[ns,UTC]"
    datetime64_ms = "datetime64[ms]"

    timedelta64_ns = "timedelta64[ns]"


class ColumnValidationBase(pydantic.BaseModel):
    """A single column validator."""

    name: str
    apply: bool
    options: Mapping[str, object]


class SchemaColumn(pydantic.BaseModel):
    """Definition os a single data source column."""

    name: str
    data_type: ColumnType = pydantic.Field(alias="type")
    validations: Sequence[ColumnValidationBase] = pydantic.Field(default_factory=list)
    metrics: Sequence[MetricsName] = ()

    @pydantic.validator("validations", pre=True)
    def remap_validations(cls, field):
        """Remap the yaml structure of {validation_type: <params>} to a list with validation_type as a key"""
        if not isinstance(field, dict):
            raise ValueError(f"{field!r} should be a dict")
        out = []
        for (key, params) in field.items():
            new_el = params.copy()
            new_el.update({"name": key})
            out.append(new_el)
        return out

    @pydantic.validator("metrics", pre=True, always=True)
    def validate_metrics(cls, field):
        """Remap any false-ish `metrics` value to an empty list."""
        if field:
            out = field
        else:
            out = []
        return out


class DataframeSchema(pydantic.BaseModel):
    """Pydantic model describing the tabular data provided by the data source."""

    name: str
    columns: Mapping[str, SchemaColumn]

    @pydantic.validator("columns", pre=True)
    def supply_column_names(cls, field):
        """Tell each column its name (the key it is listed under)"""
        if not isinstance(field, Mapping):
            raise ValueError(f"{field!r} shoudl be a dict.")

        return {col_name: {**{"name": col_name}, **col_data} for (col_name, col_data) in field.items()}

    @property
    def validations(self) -> Mapping[str, Sequence[ColumnValidationBase]]:
        """A short-hand property to access the validators for each column."""
        return {col_name: col.validations for (col_name, col) in self.columns.items()}

    @property
    def metrics(self) -> Mapping[str, Sequence[MetricsName]]:
        """A short-hand property to access the metrics for each column."""
        return {col_name: col.metrics for (col_name, col) in self.columns.items()}

    @property
    def column_names(self) -> Sequence[str]:
        """Property providing the list of all column names."""
        return tuple(self.columns.keys())
