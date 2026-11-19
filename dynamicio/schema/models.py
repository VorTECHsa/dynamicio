"""Spike: compile a Pandera ``DataFrameModel`` into dynamicio's existing ``DataframeSchema``.

Pandera is the declaration (what developers click through); dynamicio's validators and metrics stay the execution engine, so
messages, payloads and log lines are unchanged. Anything Pandera cannot express natively is declared with the typed helpers of
``dynamicio.schema.metadata`` (``Field(metrics=[Metric.MIN], max_null_fraction=0.2)``).

A model names its dataset (the label of logs and ``METRIC`` lines) with ``__dataset_name__``, which is mandatory, and the Postgres table it maps to
with ``__table_name__``; the dunders, because Pandera reserves public attributes for columns. ``DataModel`` sets them from its class keywords.

Nothing in a model is dropped silently. Column checks that dynamicio has no validator for (``str_matches``, a custom ``@pa.check``) run as Pandera
checks, and the dataframe-level parts (``@pa.dataframe_check``, ``Config.unique``, ``Config.ordered``, an index) run as a residual Pandera schema.
Constructs that cannot be honoured raise ``ValueError`` naming them. The compiled ``DataframeSchema`` is the backend-neutral contract. A ``pandera.pandas``
and a ``pandera.polars`` model compile the same way, because ``Field`` gives both the same checks and metadata; the engine of the process decides which of
the two an IO object takes.
"""

import importlib
from typing import Any, Dict, List, Optional

import pandas as pd

from dynamicio.config.pydantic.table_schema import DataframeSchema
from dynamicio.errors import DatasetNameNotFoundError
from dynamicio.schema.metadata import METADATA_KEY, PANDAS_DTYPE_KEY, DynamicioMetadata


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

    DATASET_NAME = "__dataset_name__"
    TABLE_NAME = "__table_name__"

    # The module of the Pandera model class of each backend -> the name of the backend, which is also the name of its dynamicio engine
    BACKENDS = {"pandera.api.pandas.model": "pandas", "pandera.api.polars.model": "polars"}

    @classmethod
    def backend_of(cls, model: type) -> str:
        """The backend of a Pandera model, ``"pandas"`` or ``"polars"``, from its base class, without importing polars."""
        for base in model.__mro__:
            backend = cls.BACKENDS.get(base.__module__)
            if backend is not None and base.__name__ == "DataFrameModel":
                return backend
        raise ValueError(f"{model.__name__}: only pandas and polars Pandera models are supported, got {', '.join(sorted({b.__module__ for b in model.__mro__}))}")

    @classmethod
    def pandera_of(cls, model: type) -> Any:
        """The Pandera module of the backend of a model, ``pandera.pandas`` or ``pandera.polars``."""
        return importlib.import_module(f"pandera.{cls.backend_of(model)}")

    @classmethod
    def name_model(cls, model: type, dataset: Optional[str], table: Optional[str]) -> None:
        """Set the dataset and table names of a model from the class keywords of a ``DataModel`` of either backend."""
        for attribute, value in ((cls.DATASET_NAME, dataset), (cls.TABLE_NAME, table)):
            if value is not None:
                cls.check_name(model, attribute, value)
                setattr(model, attribute, value)

    @staticmethod
    def check_name(model: type, attribute: str, value: Any) -> None:
        """Refuse a dataset or table name that is not a non-empty string."""
        if not (isinstance(value, str) and value.strip()):
            raise ValueError(f"{model.__name__}.{attribute} must be a non-empty string, got {value!r}")

    @classmethod
    def dataset_name(cls, model: type) -> str:
        """The label of the dataset in logs and ``METRIC`` lines, as the model declares it. It is not inherited and never guessed from the class name."""
        declared = vars(model).get(cls.DATASET_NAME)
        if declared is None:
            raise DatasetNameNotFoundError(model.__name__)
        cls.check_name(model, cls.DATASET_NAME, declared)
        return declared

    @classmethod
    def table_name(cls, model: type) -> Optional[str]:
        """The Postgres table of the model, exactly as the database spells it (``schema.table`` for one outside the default schema); None for a model without one."""
        declared = vars(model).get(cls.TABLE_NAME)
        if declared is not None:
            cls.check_name(model, cls.TABLE_NAME, declared)
        return declared

    @classmethod
    def dtype(cls, model: type, column: Any) -> str:
        """The pandas dtype of a Pandera column: the declared dtype of a pandas model, the pair of the dtype map (decision 14 of ADR 0010) of a polars model."""
        if cls.backend_of(model) == "polars":
            # pylint: disable=import-outside-toplevel
            from dynamicio.schema.polars_dtypes import DtypeMap

            return DtypeMap.pandas_dtype_of_column(model, column)
        if PANDAS_DTYPE_KEY in (column.metadata or {}):
            raise ValueError(f"{model.__name__}.{column.name}: pandas_dtype is for a polars model only. A pandas model declares the pandas dtype as the type of the column")
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
        cls.backend_of(model)
        problems: List[str] = [f"Config.{name}" for name, unused in cls.SCHEMA_UNSUPPORTED if cls.is_used(getattr(schema, name, unused), unused)]
        if schema.strict is True:
            problems.append('Config.strict = True (dynamicio drops columns that are not in the schema before validating: use strict = "filter")')
        for name, column in schema.columns.items():
            problems += [f"{name}: {attribute}" for attribute, unused in cls.COLUMN_UNSUPPORTED if cls.is_used(getattr(column, attribute, unused), unused)]
        if problems:
            raise ValueError(f"{model.__name__}: dynamicio cannot honour {', '.join(problems)}")

    @classmethod
    def dataframe_validator(cls, model: type) -> Any:
        """The dataframe-level part of a model (checks, `unique`, `ordered`, the index) as a Pandera schema of its backend, or None when it has none."""
        schema = model.to_schema()
        if not (schema.checks or schema.unique or schema.ordered or getattr(schema, "index", None) is not None):
            return None
        pandera = cls.pandera_of(model)
        columns = {name: pandera.Column(nullable=True) for name in schema.columns} if schema.ordered else {}
        if cls.backend_of(model) == "polars":  # a polars frame has no index
            return pandera.DataFrameSchema(columns, checks=schema.checks, unique=schema.unique, ordered=schema.ordered, report_duplicates=schema.report_duplicates, name=schema.name)
        return pandera.DataFrameSchema(
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
                "type": cls.dtype(model, column),
                "validations": cls.validations_of(column),
                "metrics": [metric.value for metric in cls.declared(column).metrics],
            }
            for name, column in schema.columns.items()
        }
        return DataframeSchema(name=cls.dataset_name(model), table_name=cls.table_name(model), columns=columns)


# The names the rest of dynamicio imports.
dataframe_validator = ModelCompiler.dataframe_validator
to_dataframe_schema = ModelCompiler.to_dataframe_schema
