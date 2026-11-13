"""Spike: compile a Pandera ``DataFrameModel`` into dynamicio's existing ``DataframeSchema``.

Pandera is the declaration (what developers click through); dynamicio's validators and metrics stay the execution engine, so
messages, payloads and log lines are unchanged. Anything Pandera cannot express natively is declared with the typed helpers of
``dynamicio.schema.metadata`` (``Field(metrics=[Metric.MIN], max_null_fraction=0.2)``).

A model names its dataset (the label of logs and ``METRIC`` lines) with ``__dataset_name__``, which is mandatory, and the Postgres table it maps to
with ``__table_name__``. ``DataModel`` sets them from class keywords (see ``dynamicio.schema.model_settings``).

Nothing in a model is dropped silently. Column checks that dynamicio has no validator for (``str_matches``, a custom ``@pa.check``) run as Pandera
checks, and the dataframe-level parts (``@pa.dataframe_check``, ``Config.unique``, ``Config.ordered``, an index) run as a residual Pandera schema.
Constructs that cannot be honoured raise ``ValueError`` naming them. The compiled ``DataframeSchema`` is the backend-neutral contract; this module only
reads pandas models, so a polars model is refused and gets its own compiler when the polars backend lands.
"""

from typing import Any, Dict, List

import pandas as pd
import pandera.pandas as pa

from dynamicio.config.pydantic.table_schema import DataframeSchema
from dynamicio.schema.metadata import METADATA_KEY, DynamicioMetadata
from dynamicio.schema.model_settings import ModelSettings


class ModelCompiler:
    """Compiles a Pandera ``DataFrameModel`` into the ``DataframeSchema`` contract and the residual Pandera validator that runs what the contract cannot."""

    # pandera check name -> (dynamicio validator, mapping of pandera statistics -> validator options)
    CHECKS = {
        "greater_than": ("is_greater_than", lambda s: {"threshold": s["min_value"]}),
        "greater_than_or_equal_to": ("is_greater_than_or_equal", lambda s: {"threshold": s["min_value"]}),
        "less_than": ("is_lower_than", lambda s: {"threshold": s["max_value"]}),
        "less_than_or_equal_to": ("is_lower_than_or_equal", lambda s: {"threshold": s["max_value"]}),
        "in_range": ("is_between", lambda s: {"lower": s["min_value"], "upper": s["max_value"], "include_left": s["include_min"], "include_right": s["include_max"]}),
        "isin": ("is_in", lambda s: {"categorical_values": list(s["allowed_values"])}),
    }

    # Schema- and column-level options with no dynamicio behaviour: (attribute, value that means "not used")
    SCHEMA_UNSUPPORTED = (("parsers", None), ("dtype", None), ("add_missing_columns", False), ("on_missing_columns", None), ("drop_invalid_rows", False), ("unique_column_names", False))
    COLUMN_UNSUPPORTED = (("parsers", None), ("regex", False), ("required", True), ("default", None), ("drop_invalid_rows", False), ("on_missing", None))

    @staticmethod
    def dtype(column: Any) -> str:
        """The dtype a Pandera column declares, spelled as dynamicio casts to it."""
        name = str(column.dtype)
        # Python `str` is whatever pandas makes of it: `object` before pandas 3, its `str` dtype after. Declare `string[pyarrow]` for a smaller column.
        return str(pd.Series([""]).dtype) if name == "str" else name

    @staticmethod
    def is_used(value: Any, unused: Any) -> bool:
        """Whether a Pandera option was set to something other than its "not used" value."""
        return value is not unused and value != unused and not (isinstance(value, (list, tuple, dict)) and not value)

    @classmethod
    def check_supported(cls, model: type, schema: Any) -> None:
        """Fail on anything dynamicio would otherwise ignore, so a migrated rule is never silently lost."""
        if not isinstance(schema, pa.DataFrameSchema):
            raise ValueError(f"{model.__name__}: only pandas Pandera models are supported, got {type(schema).__module__}")
        problems: List[str] = [f"Config.{name}" for name, unused in cls.SCHEMA_UNSUPPORTED if cls.is_used(getattr(schema, name, unused), unused)]
        if schema.strict is True:
            problems.append('Config.strict = True (dynamicio drops columns that are not in the schema before validating: use strict = "filter")')
        for name, column in schema.columns.items():
            problems += [f"{name}: {attribute}" for attribute, unused in cls.COLUMN_UNSUPPORTED if cls.is_used(getattr(column, attribute, unused), unused)]
        if problems:
            raise ValueError(f"{model.__name__}: dynamicio cannot honour {', '.join(problems)}")

    @staticmethod
    def dataframe_validator(model: type) -> Any:
        """The dataframe-level part of a model (checks, `unique`, `ordered`, the index) as a Pandera schema, or None when it has none."""
        schema = model.to_schema()
        if not (schema.checks or schema.unique or schema.ordered or schema.index is not None):
            return None
        columns = {name: pa.Column(nullable=True) for name in schema.columns} if schema.ordered else {}
        return pa.DataFrameSchema(
            columns, checks=schema.checks, index=schema.index, unique=schema.unique, ordered=schema.ordered, report_duplicates=schema.report_duplicates, name=schema.name
        )

    @classmethod
    def validations_of(cls, column: Any) -> Dict[str, Dict[str, Any]]:
        """The validations a Pandera column declares, plus those in its ``dynamicio`` metadata."""
        validations: Dict[str, Dict[str, Any]] = {}
        if not column.nullable:
            validations["has_no_null_values"] = {"apply": True, "options": {}}
        if column.unique:
            validations["has_unique_values"] = {"apply": True, "options": {}}
        for check in column.checks:
            if check.name in cls.CHECKS:
                validator, options = cls.CHECKS[check.name]
                validations[validator] = {"apply": True, "options": options(check.statistics)}
            else:  # `str_matches`, a custom `@pa.check`, ...: Pandera runs it
                validations[check.name or "pandera_check"] = {"apply": True, "options": {"pandera_check": check}}
        for spec in cls.declared(column).validations:
            validations[spec.name] = {"apply": True, "options": dict(spec.options)}
        return validations

    @staticmethod
    def declared(column: Any) -> DynamicioMetadata:
        """What a Pandera column declares for dynamicio through ``metadata``; anything but the typed helpers of ``dynamicio.schema`` is refused."""
        declared = (column.metadata or {}).get(METADATA_KEY)
        if declared is None:
            return DynamicioMetadata()
        if not isinstance(declared, DynamicioMetadata):
            raise ValueError(
                f"{column.name}: metadata={{'{METADATA_KEY}': ...}} is not supported any more. "
                "Declare metrics and validations with dynamicio.schema.Field: Field(nullable=True, metrics=[Metric.MIN], max_null_fraction=0.2)"
            )
        return declared

    @classmethod
    def to_dataframe_schema(cls, model: type) -> DataframeSchema:
        """Translate a ``pa.DataFrameModel`` subclass to the equivalent ``DataframeSchema``."""
        schema = model.to_schema()
        cls.check_supported(model, schema)
        # TODO: optionally accept `pa.Field(log_statistics={"metrics": [...]})` (a Pandera check method that logs the metrics while validating, as the
        #  abandoned v5 branch did) next to this metadata. It would log as a side effect of validation, so the order relative to other checks and
        #  logging for data that fails validation could differ from today's METRIC lines: add it only as an opt-in feature with its own tests.
        columns = {
            name: {
                "type": cls.dtype(column),
                "validations": cls.validations_of(column),
                "metrics": [metric.value for metric in cls.declared(column).metrics],
            }
            for name, column in schema.columns.items()
        }
        return DataframeSchema(name=ModelSettings.dataset_name(model), table_name=ModelSettings.table_name(model), columns=columns)


# The names the rest of dynamicio imports.
dataframe_validator = ModelCompiler.dataframe_validator
to_dataframe_schema = ModelCompiler.to_dataframe_schema
