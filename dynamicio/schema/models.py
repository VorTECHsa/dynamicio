"""Spike: compile a Pandera ``DataFrameModel`` into dynamicio's existing ``DataframeSchema``.

Pandera is the declaration (what developers click through); dynamicio's validators and metrics stay the execution engine, so
messages, payloads and log lines are unchanged. Anything Pandera cannot express natively lives in
``Field(metadata={"dynamicio": {"validations": {...}, "metrics": [...]}})``.
"""

from typing import Any, Dict

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


def _dtype(column: Any) -> str:
    name = str(column.dtype)
    return "object" if name in ("str", "string") else name


def to_dataframe_schema(model: type) -> DataframeSchema:
    """Translate a ``pa.DataFrameModel`` subclass to the equivalent ``DataframeSchema``."""
    schema = model.to_schema()
    columns: Dict[str, Dict[str, Any]] = {}
    for name, column in schema.columns.items():
        extra = (column.metadata or {}).get(METADATA_KEY, {})
        validations: Dict[str, Dict[str, Any]] = {}
        if not column.nullable:
            validations["has_no_null_values"] = {"apply": True, "options": {}}
        if column.unique:
            validations["has_unique_values"] = {"apply": True, "options": {}}
        for check in column.checks:
            validator, options = _CHECKS[check.name]
            validations[validator] = {"apply": True, "options": options(check.statistics)}
        for validator, options in extra.get("validations", {}).items():
            validations[validator] = {"apply": True, "options": options}
        columns[name] = {"type": _dtype(column), "validations": validations, "metrics": list(extra.get("metrics", []))}
    return DataframeSchema(name=schema.name or model.__name__, columns=columns)
