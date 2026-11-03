"""Spike: compile a Pandera ``DataFrameModel`` into dynamicio's existing ``DataframeSchema``.

Pandera is the declaration (what developers click through); dynamicio's validators and metrics stay the execution engine, so
messages, payloads and log lines are unchanged. Anything Pandera cannot express natively lives in
``Field(metadata={"dynamicio": {"validations": {...}, "metrics": [...]}})``.

Nothing in a model is dropped silently. Column checks that dynamicio has no validator for (``str_matches``, a custom ``@pa.check``) run as Pandera
checks, and the dataframe-level parts (``@pa.dataframe_check``, ``Config.unique``, ``Config.ordered``, an index) run as a residual Pandera schema.
Constructs that cannot be honoured raise ``ValueError`` naming them. The compiled ``DataframeSchema`` is the backend-neutral contract; this module only
reads pandas models, so a polars model is refused and gets its own compiler when the polars backend lands.
"""

from typing import Any, Dict, List

import pandas as pd
import pandera.pandas as pa

from dynamicio.config.pydantic.table_schema import DataframeSchema

METADATA_KEY = "dynamicio"

# pandera check name -> (dynamicio validator, mapping of pandera statistics -> validator options)
_CHECKS = {
    "greater_than": ("is_greater_than", lambda s: {"threshold": s["min_value"]}),
    "greater_than_or_equal_to": ("is_greater_than_or_equal", lambda s: {"threshold": s["min_value"]}),
    "less_than": ("is_lower_than", lambda s: {"threshold": s["max_value"]}),
    "less_than_or_equal_to": ("is_lower_than_or_equal", lambda s: {"threshold": s["max_value"]}),
    "in_range": ("is_between", lambda s: {"lower": s["min_value"], "upper": s["max_value"], "include_left": s["include_min"], "include_right": s["include_max"]}),
    "isin": ("is_in", lambda s: {"categorical_values": list(s["allowed_values"])}),
}

# Schema- and column-level options with no dynamicio behaviour: (attribute, value that means "not used")
_SCHEMA_UNSUPPORTED = (("parsers", None), ("dtype", None), ("add_missing_columns", False), ("on_missing_columns", None), ("drop_invalid_rows", False), ("unique_column_names", False))
_COLUMN_UNSUPPORTED = (("parsers", None), ("regex", False), ("required", True), ("default", None), ("drop_invalid_rows", False), ("on_missing", None))


def _dtype(column: Any) -> str:
    name = str(column.dtype)
    # Python `str` is whatever pandas makes of it: `object` before pandas 3, its `str` dtype after. Declare `string[pyarrow]` for a smaller column.
    return str(pd.Series([""]).dtype) if name == "str" else name


def _is_used(value: Any, unused: Any) -> bool:
    return value is not unused and value != unused and not (isinstance(value, (list, tuple, dict)) and not value)


def _unsupported(model: type, schema: Any) -> None:
    """Fail on anything dynamicio would otherwise ignore, so a migrated rule is never silently lost."""
    if not isinstance(schema, pa.DataFrameSchema):
        raise ValueError(f"{model.__name__}: only pandas Pandera models are supported, got {type(schema).__module__}")
    problems: List[str] = [f"Config.{name}" for name, unused in _SCHEMA_UNSUPPORTED if _is_used(getattr(schema, name, unused), unused)]
    if schema.strict is True:
        problems.append('Config.strict = True (dynamicio drops columns that are not in the schema before validating: use strict = "filter")')
    for name, column in schema.columns.items():
        problems += [f"{name}: {attribute}" for attribute, unused in _COLUMN_UNSUPPORTED if _is_used(getattr(column, attribute, unused), unused)]
    if problems:
        raise ValueError(f"{model.__name__}: dynamicio cannot honour {', '.join(problems)}")


def dataframe_validator(model: type) -> Any:
    """The dataframe-level part of a model (checks, `unique`, `ordered`, the index) as a Pandera schema, or None when it has none."""
    schema = model.to_schema()
    if not (schema.checks or schema.unique or schema.ordered or schema.index is not None):
        return None
    columns = {name: pa.Column(nullable=True) for name in schema.columns} if schema.ordered else {}
    return pa.DataFrameSchema(
        columns, checks=schema.checks, index=schema.index, unique=schema.unique, ordered=schema.ordered, report_duplicates=schema.report_duplicates, name=schema.name
    )


def to_dataframe_schema(model: type) -> DataframeSchema:
    """Translate a ``pa.DataFrameModel`` subclass to the equivalent ``DataframeSchema``."""
    schema = model.to_schema()
    _unsupported(model, schema)
    columns: Dict[str, Dict[str, Any]] = {}
    for name, column in schema.columns.items():
        # TODO: optionally accept `pa.Field(log_statistics={"metrics": [...]})` (a Pandera check method that logs the metrics while validating, as the
        #  abandoned v5 branch did) next to this metadata. It would log as a side effect of validation, so the order relative to other checks and
        #  logging for data that fails validation could differ from today's METRIC lines: add it only as an opt-in feature with its own tests.
        extra = (column.metadata or {}).get(METADATA_KEY, {})
        validations: Dict[str, Dict[str, Any]] = {}
        if not column.nullable:
            validations["has_no_null_values"] = {"apply": True, "options": {}}
        if column.unique:
            validations["has_unique_values"] = {"apply": True, "options": {}}
        for check in column.checks:
            if check.name in _CHECKS:
                validator, options = _CHECKS[check.name]
                validations[validator] = {"apply": True, "options": options(check.statistics)}
            else:  # `str_matches`, a custom `@pa.check`, ...: Pandera runs it
                validations[check.name or "pandera_check"] = {"apply": True, "options": {"pandera_check": check}}
        for validator, options in extra.get("validations", {}).items():
            validations[validator] = {"apply": True, "options": options}
        columns[name] = {"type": _dtype(column), "validations": validations, "metrics": list(extra.get("metrics", []))}
    return DataframeSchema(name=schema.name or model.__name__, columns=columns)
