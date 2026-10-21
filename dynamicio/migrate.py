"""Spike: render a YAML-defined ``DataframeSchema`` as the source of an equivalent Pandera ``DataFrameModel``."""

import keyword
import re
from typing import Any, Dict, List, Mapping

from dynamicio.config.pydantic.table_schema import DataframeSchema, SchemaColumn

_DTYPES = {"int64": "int", "float64": "float", "object": "str", "bool": "bool", "datetime64[ns]": "pd.Timestamp"}


def _class_name(name: str) -> str:
    return "".join(part.capitalize() for part in re.split(r"[^0-9a-zA-Z]+", name) if part) or "Schema"


def _render_column(column: SchemaColumn) -> str:
    kwargs: List[str] = []
    extra: Dict[str, Any] = {}
    applied = [v for v in column.validations if v.apply]
    by_name = {v.name: dict(v.options) for v in applied}

    # Pandera's default is nullable=False; dynamicio's is "nulls allowed unless has_no_null_values". Always be explicit.
    kwargs.append(f"nullable={'has_no_null_values' not in by_name}")
    if "has_unique_values" in by_name:
        kwargs.append("unique=True")
    simple = {"is_greater_than": "gt", "is_greater_than_or_equal": "ge", "is_lower_than": "lt", "is_lower_than_or_equal": "le"}
    for name, options in by_name.items():
        if name in simple:
            kwargs.append(f"{simple[name]}={options['threshold']!r}")
        elif name == "is_between":
            kwargs.append(
                f"in_range={{'min_value': {options['lower']!r}, 'max_value': {options['upper']!r}, "
                f"'include_min': {options.get('include_left', False)!r}, 'include_max': {options.get('include_right', False)!r}}}"
            )
        elif name == "is_in" and options.get("match_all", True):
            kwargs.append(f"isin={sorted(options['categorical_values'])!r}")
        elif name not in ("has_no_null_values", "has_unique_values"):
            extra.setdefault("validations", {})[name] = options
    if column.metrics:
        extra["metrics"] = [m.value if hasattr(m, "value") else m for m in column.metrics]
    if extra:
        kwargs.append(f"metadata={{'dynamicio': {extra!r}}}")
    return f"pa.Field({', '.join(kwargs)})"


def render_model(schema: DataframeSchema) -> str:
    """Return Python source for a ``pa.DataFrameModel`` equivalent to ``schema``."""
    lines = [f"class {_class_name(schema.name)}(pa.DataFrameModel):", f'    """Migrated from the dynamicio schema {schema.name!r}."""', ""]
    for name, column in schema.columns.items():
        attribute = name if name.isidentifier() and not keyword.iskeyword(name) else f"column_{re.sub(r'[^0-9a-zA-Z_]', '_', name)}"
        field = _render_column(column)
        if attribute != name:
            field = field[:-1] + f", alias={name!r})"
        dtype = _DTYPES.get(column.data_type, f"pd.api.types.pandas_dtype({column.data_type!r})")
        lines.append(f"    {attribute}: Series[{dtype}] = {field}")
    lines += ["", "    class Config:", f"        name = {schema.name!r}", ""]
    return "\n".join(lines)


def render_module(schemas: Mapping[str, DataframeSchema]) -> str:
    """Return a module defining one model per schema."""
    header = ["import pandas as pd", "import pandera.pandas as pa", "from pandera.typing import Series", "", ""]
    return "\n".join(header) + "\n\n".join(render_model(s) for s in schemas.values())
