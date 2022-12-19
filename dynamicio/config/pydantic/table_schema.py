"""This module defines Config schema for data source (pandas dataframe)"""

import enum
import typing

import pydantic


@enum.unique
class MetricsName(str, enum.Enum):
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
    object = "object"
    float64 = "float64"
    int64 = "int64"
    bool = "bool"
    datetime64_ns = "datetime64[ns]"


class ColumnValidationBase(pydantic.BaseModel):
    name: str
    apply: bool
    options: typing.Mapping[str, object]


ColumnValidationType = typing.Union[ColumnValidationBase, ColumnValidationBase]


class SchemaColumn(pydantic.BaseModel):
    name: str
    data_type: ColumnType = pydantic.Field(alias="type")
    validations: typing.Sequence[ColumnValidationType] = pydantic.Field(default_factory=list)
    metrics: typing.Sequence[MetricsName] = ()

    @pydantic.validator("validations", pre=True)
    def remap_validations(cls, field):
        # Remap the yaml structure of {validation_type: <params>} to a list with validation_type as a key
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
        if field:
            out = field
        else:
            # Remap None as empty sequence
            out = []
        return out


class DataframeSchema(pydantic.BaseModel):
    name: str
    columns: typing.Mapping[str, SchemaColumn]

    @pydantic.validator("columns", pre=True)
    def supply_column_names(cls, field):
        if not isinstance(field, typing.Mapping):
            raise ValueError(f"{field!r} shoudl be a dict.")

        return {col_name: {**{"name": col_name}, **col_data} for (col_name, col_data) in field.items()}

    @property
    def validations(self) -> typing.Mapping[str, ColumnValidationType]:
        return {col_name: col.validations for (col_name, col) in self.columns.items()}

    @property
    def metrics(self) -> typing.Mapping[str, typing.Sequence[MetricsName]]:
        return {col_name: col.metrics for (col_name, col) in self.columns.items()}

    @property
    def column_names(self) -> typing.Sequence[str]:
        return tuple(self.columns.keys())


class DataframeSchemaRef(pydantic.BaseModel):
    """A reference to a schema file stored somewhere else on the system"""

    file_path: pydantic.FilePath
