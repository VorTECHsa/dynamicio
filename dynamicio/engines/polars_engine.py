"""The polars engine. It checks frames, models and resources, reads, scans and writes files, casts, validates and logs metrics."""

from typing import TYPE_CHECKING, Any, Dict, List, NoReturn, Tuple

from dynamicio.engines.engine import Engine
from dynamicio.engines.registry import Registry
from dynamicio.errors import ColumnsDataTypeError, SchemaValidationError

if TYPE_CHECKING:  # pragma: no cover
    from dynamicio.core import DynamicDataIO


class PolarsEngine(Engine):
    """The engine of polars frames. This module does not import polars, so dynamicio imports without it: the engine imports polars when it starts."""

    name = "polars"
    FOREIGN_FRAME_MODULES = ("pandas",)

    def __init__(self):
        """Hold the registries of the polars validators and metrics."""
        # pylint: disable=import-outside-toplevel
        from dynamicio.schema.polars_metrics import PolarsMetrics
        from dynamicio.schema.polars_validations import PolarsValidations

        self._registry = Registry(PolarsValidations.ALL, PolarsMetrics.ALL)

    @property
    def registry(self) -> Registry:
        """The polars validators and metrics, by the names of the pandas ones."""
        return self._registry

    @staticmethod
    def logger() -> Any:
        """The ``dynamicio.core.logger`` of now: a consumer that patches it gets the messages of the polars cast, as of the pandas cast."""
        from dynamicio import core  # pylint: disable=import-outside-toplevel

        return core.logger

    def not_yet(self, operation: str) -> NoReturn:
        """Refuse an operation that the polars engine does not have yet."""
        raise NotImplementedError(f"the polars engine does not {operation} yet")

    def check_resource(self, io: "DynamicDataIO") -> None:
        """Refuse a file resource with a file type or an option that the polars engine cannot read or write (decision 6 of ADR 0010).

        Raises:
            EngineResourceError: The file type or an option has no polars reader and writer.
        """
        from dynamicio.engines.polars_files import PolarsFiles  # pylint: disable=import-outside-toplevel

        PolarsFiles.check(io)

    def read(self, io: "DynamicDataIO") -> Any:
        """Read a ``local``, ``local_batch``, ``s3_file`` or ``s3_path_prefix`` resource into a polars frame. Other resources are not read yet.

        A prefix read collects the scan of the prefix, so that the eager and the lazy reads of a prefix give the same frame.
        """
        # pylint: disable=import-outside-toplevel
        from dynamicio.engines.polars_files import PolarsFiles
        from dynamicio.engines.polars_scans import PolarsScans

        source = self.source_of(io)
        if source not in PolarsFiles.READ_SOURCES:
            self.not_yet(f"read {source} resources")
        if source == "s3_path_prefix":
            return PolarsScans.scan(io).collect()
        return PolarsFiles.read(io)

    def write(self, io: "DynamicDataIO", frame: Any) -> None:
        """Write a polars frame to a ``local`` or ``s3_file`` resource. Other resources are not written yet."""
        from dynamicio.engines.polars_files import PolarsFiles  # pylint: disable=import-outside-toplevel

        source = self.source_of(io)
        if source not in PolarsFiles.SOURCES:
            self.not_yet(f"write {source} resources")
        PolarsFiles.write(io, frame.collect() if hasattr(frame, "collect") else frame)

    def scan(self, io: "DynamicDataIO") -> Any:
        """The ``LazyFrame`` of a ``local``, ``local_batch``, ``s3_file`` or ``s3_path_prefix`` resource. Other resources are not scanned yet."""
        from dynamicio.engines.polars_scans import PolarsScans  # pylint: disable=import-outside-toplevel

        source = self.source_of(io)
        if source not in PolarsScans.SOURCES:
            self.not_yet(f"scan {source} resources")
        return PolarsScans.scan(io)

    def sink(self, io: "DynamicDataIO", frame: Any) -> None:
        """Refuse: no polars sinks yet."""
        self.not_yet("sink")

    def select_columns(self, io: "DynamicDataIO", frame: Any) -> Any:
        """The schema columns of a frame, in the order of the frame, with float NaN changed to null as the cast changes it.

        The write validations and metrics then see a NaN as a null, as the pandas engine sees it. A polars frame does not change in place, so the caller's
        frame does not change.
        """
        import polars.selectors as cs  # pylint: disable=import-outside-toplevel

        selected = frame.select([column for column in frame.collect_schema().names() if column in io.schema.column_names])
        return selected.with_columns(cs.float().fill_nan(None))

    def cast(self, io: "DynamicDataIO", frame: Any) -> Any:
        """A frame with the polars dtypes of the model, and float NaN changed to null (decision 3 of ADR 0010).

        A ``DataFrame`` casts column by column, so a failed cast names its column. A ``LazyFrame`` gets the casts in its plan, and a failed cast raises
        when the frame is collected. A string column casts to a ``Datetime`` with ``str.to_datetime``, as ``astype`` parses strings in pandas.

        Raises:
            ColumnsDataTypeError: A column is not in the schema, a schema column is not in the frame, or a column does not cast.
        """
        import polars as pl  # pylint: disable=import-outside-toplevel

        found = frame.collect_schema()
        casts = self.casts(io, found)
        if isinstance(frame, pl.LazyFrame):
            return frame.with_columns([expression for _, _, expression in casts])
        for column, expected, expression in casts:
            try:
                frame = frame.with_columns(expression)
            except (pl.exceptions.InvalidOperationError, pl.exceptions.ComputeError):
                self.logger().exception(f"Tried casting column {io.name}['{column}'] to '{expected}' from '{found[column]}', but failed")
                raise ColumnsDataTypeError() from None
        return frame

    def casts(self, io: "DynamicDataIO", found: Any) -> List[Tuple[str, Any, Any]]:
        """The cast expression of each column to validate: ``(column, polars dtype, expression)``. A column with the dtype of the model only gets the null rule."""
        import polars as pl  # pylint: disable=import-outside-toplevel

        from dynamicio.schema.polars_dtypes import DtypeMap  # pylint: disable=import-outside-toplevel

        columns, not_allowed = io._split_columns_to_validate_between_allowed_and_not(columns_to_validate=io._columns_to_validate())  # pylint: disable=protected-access
        if not_allowed:
            self.logger().error(f"The columns passed: {not_allowed} do not belong to the schema")
            raise ColumnsDataTypeError()
        missing = [column.name for column in columns if column.name not in found]
        if missing:
            self.logger().error(f"The columns {missing} of the schema of {io.name} are not in the frame")
            raise ColumnsDataTypeError()

        dtypes: Dict[str, Any] = DtypeMap.of_model(io._model)  # pylint: disable=protected-access
        casts = []
        for column in columns:
            expected, current = dtypes[column.name], found[column.name]
            expression = pl.col(column.name)
            if current != expected or getattr(current, "time_zone", None) != getattr(expected, "time_zone", None):
                if io.show_casting_warnings:
                    self.logger().info(f"Expected: '{expected}' dtype for {io.name}['{column.name}'], found '{current}'")
                expression = self.cast_expression(expression, current, expected)
            if expected.is_float():
                expression = expression.fill_nan(None)
            casts.append((column.name, expected, expression))
        return casts

    @staticmethod
    def cast_expression(expression: Any, current: Any, expected: Any) -> Any:
        """The strict cast of an expression from a polars dtype to another. polars 2 does not cast a string to a datetime: it parses it."""
        import polars as pl  # pylint: disable=import-outside-toplevel

        if current == pl.String:
            if isinstance(expected, pl.Datetime):
                return expression.str.to_datetime(time_unit=expected.time_unit, time_zone=expected.time_zone, strict=True)
            if isinstance(expected, pl.Duration):
                # The text of a pandas timedelta, for example "0 days 00:00:01", as a CSV file of the pandas engine has it
                return expression.map_batches(lambda series: PolarsEngine.parse_durations(series, expected), return_dtype=expected)
            if expected == pl.Boolean:
                return expression.map_batches(PolarsEngine.parse_booleans, return_dtype=pl.Boolean)
        if current.is_float():
            # NaN is the null of a float column: pandas casts it to <NA> of a nullable dtype, a strict polars cast refuses it
            expression = expression.fill_nan(None)
        return expression.cast(expected, strict=True)

    # The strings of a Boolean column in a CSV file: pandas writes True and False, and `read_csv` in pandas also reads true and false
    BOOLEANS = {"True": True, "False": False, "true": True, "false": False, "TRUE": True, "FALSE": False}

    @classmethod
    def parse_booleans(cls, series: Any) -> Any:
        """The Boolean series of a string series. A string that is not a Boolean fails, as a strict cast does.

        Raises:
            InvalidOperationError: A string is not a Boolean.
        """
        import polars as pl  # pylint: disable=import-outside-toplevel

        unknown = series.drop_nulls().filter(~series.drop_nulls().is_in(list(cls.BOOLEANS)))
        if unknown.len():
            raise pl.exceptions.InvalidOperationError(f"conversion from `str` to `bool` failed for the values {unknown.head(5).to_list()}")
        return series.replace_strict(cls.BOOLEANS, default=None, return_dtype=pl.Boolean)

    @staticmethod
    def parse_durations(series: Any, expected: Any) -> Any:
        """The ``Duration`` series of a string series, with the parser of ``pd.to_timedelta``. A string that is not a duration fails, as a strict cast does.

        Raises:
            InvalidOperationError: A string is not a duration.
        """
        import pandas as pd  # pylint: disable=import-outside-toplevel
        import polars as pl  # pylint: disable=import-outside-toplevel

        try:
            parsed = pd.to_timedelta(series.to_pandas())
        except ValueError as error:
            raise pl.exceptions.InvalidOperationError(f"conversion from `str` to `{expected}` failed: {error}") from error
        return pl.from_pandas(parsed).cast(expected, strict=True)

    def validate(self, io: "DynamicDataIO", frame: Any) -> None:
        """Run the column validations and the dataframe-level Pandera checks in one ``collect_all``, and raise one ``SchemaValidationError`` for all failures.

        Raises:
            SchemaValidationError: One or more validations failed. The messages of each validation name are joined with ``"; "``, as on the pandas engine.
            ColumnsDataTypeError: A cast in the plan of a ``LazyFrame`` failed when the validations collected it.
        """
        import polars as pl  # pylint: disable=import-outside-toplevel

        try:
            # A check can collect a part of the plan when it is made, for example the schema of a column or a Pandera dataframe check
            pending = self.pending_validations(io, frame.lazy())
            queries = [query for _, check in pending for query in check.queries]
            collected = pl.collect_all(queries) if queries else []
        except (pl.exceptions.InvalidOperationError, pl.exceptions.ComputeError):
            self.logger().exception(f"The validations of {io.name} could not collect the frame: a cast in its plan failed")
            raise ColumnsDataTypeError() from None

        failed_validations: Dict[str, List[str]] = {}
        start = 0
        for name, check in pending:
            result = check.finish(collected[start : start + len(check.queries)])
            start += len(check.queries)
            if not result.valid:
                failed_validations.setdefault(name, []).append(result.message)
        if failed_validations:
            raise SchemaValidationError({name: "; ".join(messages) for name, messages in failed_validations.items()})

    def pending_validations(self, io: "DynamicDataIO", lazy: Any) -> List[Tuple[str, Any]]:
        """The ``(validation name, PolarsValidations.Pending)`` of each column validation to apply, then of each dataframe-level Pandera check."""
        from dynamicio.schema.polars_validations import PolarsValidations  # pylint: disable=import-outside-toplevel

        pending: List[Tuple[str, Any]] = []
        for column, column_validations in io.schema_validations.items():
            for validation in column_validations:
                if validation.apply:
                    validator = PolarsValidations.pandera_check if "pandera_check" in validation.options else self.registry.validator(validation.name)
                    pending.append((validation.name, validator(io.name, lazy, column, **validation.options)))
        dataframe_validator = getattr(io, "_dataframe_validator", None)
        if dataframe_validator is not None:
            pending += PolarsValidations.dataframe_checks(io.name, lazy, lazy.collect_schema().names(), dataframe_validator)
        return pending

    def log_metrics(self, io: "DynamicDataIO", frame: Any) -> None:
        """Compute the metrics of all columns in one ``select``, then log each, in the order of the schema, as the pandas engine does."""
        import polars as pl  # pylint: disable=import-outside-toplevel

        from dynamicio.schema.polars_metrics import PolarsMetrics  # pylint: disable=import-outside-toplevel

        wanted = [(column, getattr(metric, "value", metric)) for column, column_metrics in io.schema_metrics.items() for metric in column_metrics]
        if not wanted:
            return
        expressions = [self.registry.metric(metric)(column).alias(f"_dynamicio_metric_{index}") for index, (column, metric) in enumerate(wanted)]
        lazy = frame.lazy()
        dtypes = lazy.collect_schema()
        try:
            values = lazy.select(expressions).collect().row(0)
        except (pl.exceptions.InvalidOperationError, pl.exceptions.ComputeError):
            self.logger().exception(f"The metrics of {io.name} could not collect the frame: a cast in its plan failed")
            raise ColumnsDataTypeError() from None
        for (column, metric), value in zip(wanted, values):
            PolarsMetrics.log(io.name, column, metric, value, dtypes.get(column))
