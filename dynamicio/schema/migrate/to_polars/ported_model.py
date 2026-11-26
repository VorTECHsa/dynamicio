"""One pandas model and the parts of it that the polars model cannot take over."""

from typing import Any, Dict, List


from dynamicio.config.pydantic.table_schema import DataframeSchema
from dynamicio.schema.migrate import SchemaComparison
from dynamicio.schema.migrate.to_polars.renderer import PolarsModelRenderer
from dynamicio.schema.models import to_dataframe_schema
from dynamicio.schema.polars_dtypes import DtypeMap


class PortedModel:
    """One pandas model and the parts of it that the polars model cannot take over."""

    def __init__(self, model: type):
        self.model = model
        self.source = to_dataframe_schema(model)
        self.todos: List[str] = []
        # `Series[str]` compiles to `object` too: only the Pandera dtype tells a column of `object` values, which can hold lists, from a column of strings
        self.objects = [f"{model.__name__}.{name}" for name, column in model.to_schema().columns.items() if str(column.dtype) == DtypeMap.LIST]
        self.schema = self._without_checks()
        self.config = self._config()

    @property
    def label(self) -> str:
        """The dotted name of the pandas model."""
        return f"{self.model.__module__}.{self.model.__name__}"

    def _without_checks(self) -> DataframeSchema:
        """The schema without the Pandera checks of the pandas model (``str_matches``, a custom ``@pa.check``): they are pandas code to port by hand."""
        columns: Dict[str, Dict[str, Any]] = {}
        for name, column in self.source.columns.items():
            validations = {}
            for validation in column.validations:
                if "pandera_check" in validation.options:
                    self.todos.append(f"{self.model.__name__}.{name}: port the check {validation.name!r} to polars")
                else:
                    validations[validation.name] = {"apply": validation.apply, "options": dict(validation.options)}
            columns[name] = {"type": column.data_type, "validations": validations, "metrics": [getattr(metric, "value", metric) for metric in column.metrics]}
        return DataframeSchema(name=self.source.name, table_name=self.source.table_name, columns=columns)

    def _config(self) -> Dict[str, Any]:
        """The ``Config`` options of the pandas model that a polars model has too. The dataframe checks and the index are listed to port by hand."""
        schema = self.model.to_schema()
        config: Dict[str, Any] = {}
        for option in ("strict", "coerce", "unique", "ordered"):
            value = getattr(schema, option)
            if value not in (False, None):
                config[option] = list(value) if isinstance(value, (list, tuple)) else value
        if schema.checks:
            self.todos.append(f"{self.model.__name__}: port the dataframe checks {[check.name for check in schema.checks]} to polars")
        if schema.index is not None:
            self.todos.append(f"{self.model.__name__}: a polars frame has no index. Write the index as a column (reset_index()) and declare it in the model")
        return config

    def class_source(self) -> str:
        """The source of the polars model.

        Raises:
            ValueError: A dtype has no polars pair, or a validation has no ``Field`` argument.
        """
        try:
            return PolarsModelRenderer.polars_class_source(self.schema, self.model.__name__, self.config, PolarsModelRenderer.MIGRATED.format(name=self.label))
        except ValueError as error:
            raise ValueError(f"{self.label}: {error}") from None

    def differences(self, polars_model: type) -> List[str]:
        """How the polars model differs from the pandas model, without the checks to port by hand (empty when equivalent)."""
        compiled = to_dataframe_schema(polars_model)
        found = SchemaComparison.differences(compiled, self.schema)
        return found + ([f"table: {compiled.table_name!r} != {self.schema.table_name!r}"] if compiled.table_name != self.schema.table_name else [])
