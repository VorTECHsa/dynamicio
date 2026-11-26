"""The source of a polars model for the ``DataframeSchema`` of a pandas model."""

import ast
from typing import Any, Mapping, Optional, Sequence

from dynamicio.config.pydantic.table_schema import DataframeSchema
from dynamicio.schema.migrate import ModelRenderer, SchemaComparison
from dynamicio.schema.polars_dtypes import DtypeMap


class PolarsModelRenderer(ModelRenderer):
    """Renders the ``DataframeSchema`` of a pandas model as the source of a polars ``DataModel`` with the same columns, validations and metrics."""

    MODULE_NAMES = ModelRenderer.MODULE_NAMES | {"Annotated", "pl"}
    IMPORTED_NAMES = frozenset({"Annotated", "DataModel", "Field", "Metric", "pl"})
    MIGRATED = "Migrated from the pandas model {name}."

    @staticmethod
    def polars_dtype_source(pandas_dtype: str) -> str:
        """The annotation of a column of a pandas dtype: a polars dtype, with the time unit (and the time zone of a ``Datetime``, None without one).

        Raises:
            ValueError: The pandas dtype has no polars pair, for example ``datetime64[s]`` or ``period[D]``.
        """
        dtype = DtypeMap.polars_of(pandas_dtype)
        name = type(dtype).__name__
        if name in ("Datetime", "Duration"):
            arguments = [dtype.time_unit] + ([dtype.time_zone] if name == "Datetime" else [])  # Pandera needs both arguments of a Datetime
            return f"Annotated[pl.{name}, {', '.join(repr(argument) for argument in arguments)}]"
        return f"pl.{name}"

    @staticmethod
    def pandas_dtype_argument(pandas_dtype: str) -> Optional[str]:
        """The ``pandas_dtype`` of the field, or None when the reverse map of the polars dtype already gives the pandas dtype."""
        if DtypeMap.reverse_name(DtypeMap.polars_of(pandas_dtype)) == SchemaComparison.canonical_dtype(pandas_dtype):
            return None
        return pandas_dtype

    @staticmethod
    def check_literal_arguments(field: str, column: str) -> None:
        """Check that the arguments of a rendered ``Field`` are literals, so that the generated module needs no import for them.

        Raises:
            ValueError: A bound or a member of ``isin`` has no literal source, for example a ``datetime`` or a ``Timestamp``.
        """
        call = ast.parse(field, mode="eval").body
        for argument in call.keywords:
            if argument.arg != "metrics":
                try:
                    ast.literal_eval(argument.value)
                except ValueError:
                    raise ValueError(f"Column {column!r}: the value of {argument.arg} has no literal source ({ast.unparse(argument.value)}): port this check by hand") from None

    @classmethod
    def polars_class_source(cls, schema: DataframeSchema, class_name: str, config: Mapping[str, Any], docstring: str) -> str:
        """The source of one polars ``DataModel`` class for ``schema``, with the ``Config`` options of the pandas model in ``config``.

        Raises:
            ValueError: The class name is a name that the generated module imports, or a bound has no literal source.
        """
        if class_name in cls.IMPORTED_NAMES:
            raise ValueError(f"the model name {class_name} shadows a name that the generated module imports: rename the model")
        keywords = f"dataset={schema.name!r}" + (f", table={schema.table_name!r}" if schema.table_name else "")
        lines = [f"class {class_name}(DataModel, {keywords}):", f'    """{docstring}"""', ""]
        for name, column in schema.columns.items():
            attribute = cls.attribute_name(name, schema.columns, cls.MODULE_NAMES)
            arguments = []
            pandas_dtype = cls.pandas_dtype_argument(column.data_type)
            if pandas_dtype is not None:
                arguments.append(f"pandas_dtype={pandas_dtype!r}")
            if attribute != name:
                arguments.append(f"alias={name!r}")
            field = cls.render_field(column)
            cls.check_literal_arguments(field, name)
            if arguments:
                field = field[:-1] + ", " + ", ".join(arguments) + ")"
            lines.append(f"    {attribute}: {cls.polars_dtype_source(column.data_type)} = {field}")
        if config:
            lines += ["", "    class Config:"] + [f"        {option} = {value!r}" for option, value in config.items()]
        return "\n".join(lines) + "\n"

    @staticmethod
    def polars_module(classes: Sequence[str]) -> str:
        """A module of polars models, with the imports that the class bodies use."""
        body = "\n\n".join(classes)
        header = []
        if "Annotated[" in body:
            header += ["from typing import Annotated", ""]
        header += ["import polars as pl", ""]
        helpers = ["Field"] + (["Metric"] if "Metric." in body else [])
        header += [f"from dynamicio.schema import {', '.join(helpers)}", "from dynamicio.schema.polars import DataModel", "", "", ""]
        return "\n".join(header) + body
