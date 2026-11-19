"""The pandas engine: the behaviour of dynamicio 11."""

from typing import TYPE_CHECKING, Any, NoReturn

import pandas as pd

from dynamicio.engines.engine import Engine
from dynamicio.engines.registry import Registry
from dynamicio.errors import ColumnsDataTypeError, SchemaValidationError
from dynamicio.schema import metrics, validations

if TYPE_CHECKING:  # pragma: no cover
    from dynamicio.core import DynamicDataIO


class PandasEngine(Engine):
    """The engine of pandas frames, the default. It does what ``DynamicDataIO`` did before engines, in the same order."""

    name = "pandas"
    FOREIGN_FRAME_MODULES = ("polars",)

    def __init__(self):
        """Hold the live registries of ``dynamicio.schema``."""
        self._registry = Registry(validations.ALL_VALIDATORS, metrics.__metrics__)

    @property
    def registry(self) -> Registry:
        """The validators and metrics of ``dynamicio.schema``."""
        return self._registry

    def lazy_unsupported(self, method: str) -> NoReturn:
        """Refuse a lazy read or write, which only the polars engine has."""
        raise NotImplementedError(f'{method}() needs the polars engine: call dynamicio.configure(engine="polars") before the first read or write')

    def scan(self, io: "DynamicDataIO") -> Any:
        """Refuse: pandas has no lazy frame."""
        self.lazy_unsupported("scan")

    def sink(self, io: "DynamicDataIO", frame: Any) -> None:
        """Refuse: pandas has no lazy frame."""
        self.lazy_unsupported("sink")

    def select_columns(self, io: "DynamicDataIO", frame: pd.DataFrame) -> pd.DataFrame:
        """The schema columns of a frame. A frame with only the schema columns is a shallow copy, because the cast changes columns in place."""
        if set(frame.columns) == set(io.schema.column_names):
            return frame.copy(deep=False)
        return frame[[column for column in frame.columns.to_list() if column in io.schema.column_names]]

    def cast(self, io: "DynamicDataIO", frame: pd.DataFrame) -> pd.DataFrame:
        """Cast with ``DynamicDataIO._has_valid_dtypes``, and raise ``ColumnsDataTypeError`` for a column that does not cast."""
        if not io._has_valid_dtypes(frame):  # pylint: disable=protected-access
            raise ColumnsDataTypeError()
        return frame

    def validate(self, io: "DynamicDataIO", frame: pd.DataFrame) -> None:
        """Run the column validations, then the dataframe-level Pandera checks, and raise one ``SchemaValidationError`` for all failures."""
        failed_validations: dict = {}
        for column, col_validations in io.schema_validations.items():
            for validation in col_validations:
                if validation.apply:
                    validator = validations.pandera_check if "pandera_check" in validation.options else self.registry.validator(validation.name)
                    validation_result = validator(io.name, frame, column, **validation.options)
                    if not validation_result.valid:
                        failed_validations.setdefault(validation.name, []).append(validation_result.message)

        dataframe_validator = getattr(io, "_dataframe_validator", None)
        if dataframe_validator is not None:
            for name, message in validations.dataframe_failures(io.name, frame, dataframe_validator).items():
                failed_validations.setdefault(name, []).append(message)

        if failed_validations:
            # One entry per validation name; when several columns fail the same validation their messages are joined, none are lost.
            raise SchemaValidationError({name: "; ".join(messages) for name, messages in failed_validations.items()})

    def log_metrics(self, io: "DynamicDataIO", frame: pd.DataFrame) -> None:
        """Log each metric of each column."""
        for column, column_metrics in io.schema_metrics.items():
            for metric in column_metrics:
                self.registry.metric(metric)(io.name, frame, column)()  # type: ignore
