"""Implements the DynamicDataIO class which provides functionality for data: loading; sinking, and; schema validation."""

# pylint: disable=no-member
__all__ = ["DynamicDataIO", "SCHEMA_FROM_ARGUMENT", "CASTING_WARNING_MSG"]

import asyncio
import inspect
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Any, List, Mapping, MutableMapping, Optional, Tuple, Type

import pandas as pd
from pandera.api.dataframe.model import DataFrameModel  # the base of the pandas and the polars models

# Application Imports
from dynamicio.config.pydantic import DataframeSchema, IOEnvironment, SchemaColumn
from dynamicio.engines.engine_setting import EngineSetting
from dynamicio.errors import CASTING_WARNING_MSG, NOTICE_MSG, MissingMixinError, SchemaNotDeclaredError, SchemaNotFoundError, TableNameNotFoundError
from dynamicio.parquet_footer import ParquetFooter, ParquetFooters
from dynamicio.schema.models import dataframe_validator, to_dataframe_schema

logger = logging.getLogger(__name__)


class _SchemaFromArgument:
    """The type of `SCHEMA_FROM_ARGUMENT`."""

    def __repr__(self) -> str:
        return "SCHEMA_FROM_ARGUMENT"


SCHEMA_FROM_ARGUMENT = _SchemaFromArgument()  # `schema = SCHEMA_FROM_ARGUMENT` on an IO class: each instance gets its model from the `schema=` argument

pool = ThreadPoolExecutor()


def __getattr__(name: str) -> Any:
    """Name the replacement of what dynamicio 11 removed, for the `from dynamicio.core import SCHEMA_FROM_FILE` that every older IO module starts with."""
    if name == "SCHEMA_FROM_FILE":
        raise ImportError(f"dynamicio.core.SCHEMA_FROM_FILE: {DynamicDataIO.REMOVED_DECLARATIONS}")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


class DynamicDataIO:  # pylint: disable=too-many-instance-attributes
    """Given a `src.utils.dynamicio.config.IOConfig` object, it generates an object with access to a series of methods for cloud I/O operations and data validations.

    Example:
       >>> input_sources_config = IOConfig(
       >>>     "path_to/input.yaml",
       >>>     os.getenv("ENVIRONMENT",default="LOCAL")
       >>> )
       >>>
       >>> class IO(WithS3File, WithLocal, DynamicDataIO):
       >>>     schema = S
       >>>
       >>> my_dataset_local_mapping = input_config.get(source_key="MY_DATASET")
       >>> my_dataset_io = IO(my_dataset_local_mapping)
       >>> my_dataset_df = my_dataset_io.read()
    """

    schema: DataframeSchema
    sources_config: IOEnvironment

    def __init__(
        self,
        source_config: IOEnvironment,
        apply_schema_validations: bool = False,
        log_schema_metrics: bool = False,
        show_casting_warnings: bool = False,
        strict_dtype_check: bool = False,
        *,
        schema: Optional[Type[DataFrameModel]] = None,
        missing_ok: bool = False,
        **options: MutableMapping[str, Any],
    ):
        """Class constructor.

        Args:
            source_config: Configuration to use when reading/writing data from/to a source
            apply_schema_validations: Applies schema validations on either read() or write()
            log_schema_metrics: Logs schema metrics on either read() or write()
            show_casting_warnings: Logs casting warnings on either read() or write() if set to True
            strict_dtype_check: Applies strict dtype check on either read() or write(), confirming the correct type for all rows per column, if set to True
            schema: A Pandera `DataFrameModel` for this instance. Takes precedence over the class attribute; required when the class sets `schema = SCHEMA_FROM_ARGUMENT`
            missing_ok: If True, `read()` returns None when the source does not exist, instead of raising `FileNotFoundError`
            options: Any additional kwargs that may be used throughout the lifecycle of the object
        """
        if type(self) is DynamicDataIO:  # pylint: disable=unidiomatic-typecheck
            raise TypeError("Abstract class DynamicDataIO cannot be used to instantiate an object...")

        self.sources_config = source_config
        self.name = self._transform_class_name_to_dataset_name(self.__class__.__name__)
        self.missing_ok = missing_ok
        self.options = self._get_options(options, source_config.options)
        source_name = self.sources_config.data_backend_type.value
        self.apply_schema_validations = apply_schema_validations
        self.log_schema_metrics = log_schema_metrics
        self.show_casting_warnings = show_casting_warnings
        self.strict_dtype_check = strict_dtype_check
        self._dataframe_validator = None  # the dataframe-level part of a Pandera model, if any
        self._model = None  # the Pandera model of the instance; `self.schema` becomes its compiled `DataframeSchema`
        if schema is not None:
            if not (inspect.isclass(schema) and issubclass(schema, DataFrameModel)):
                raise TypeError(f"schema must be a pandera DataFrameModel subclass, got {schema!r}")
            self._model = schema
        elif self.schema is not SCHEMA_FROM_ARGUMENT:  # `__init_subclass__` has checked that the class attribute is a model
            self._model = self.schema
        if self._model is not None:
            EngineSetting.current().check_model(self._model)
            active_schema = to_dataframe_schema(self._model)
            self._dataframe_validator = dataframe_validator(self._model)
        else:
            active_schema = None

        if not active_schema:
            raise SchemaNotFoundError()

        assert isinstance(active_schema, DataframeSchema)
        if source_name == "postgres" and not active_schema.table_name and not self.options.get("sql_query"):  # a `sql_query` read names its own tables
            raise TableNameNotFoundError(active_schema.name)
        self.schema = active_schema
        self.name = self.schema.name.upper()
        self.schema_validations = self.schema.validations
        self.schema_metrics = self.schema.metrics

        if not (hasattr(self, f"_read_from_{source_name}") or hasattr(self, f"_write_to_{source_name}")):
            raise MissingMixinError(f"No method '_read_from_{source_name}' or '_write_to_{source_name}'. Have you registered a mixin for {source_name}?")
        EngineSetting.current().check_resource(self)

    REMOVED_DECLARATIONS = (
        "Dict schemas (`schema = {...}`, `schema_validations`, `schema_metrics`) and YAML schemas (`SCHEMA_FROM_FILE`) were removed in dynamicio 11: "
        "declare the columns as a model, see https://dynamicio.readthedocs.io/en/latest/guides/schemas-and-validation/ (`dynamicio migrate` converts YAML schemas)."
    )

    def __init_subclass__(cls):
        """Ensure that every IO class outside dynamicio declares its schema as a Pandera model.

        Raises:
            SchemaNotDeclaredError: If the class does not set `schema` (a `TypeError` and an `AssertionError`)
            TypeError: If `schema` is not a model (or SCHEMA_FROM_ARGUMENT), or the class still sets `schema_validations` or `schema_metrics`
        """
        module = inspect.getmodule(cls).__name__
        if module == "dynamicio" or module.startswith("dynamicio."):
            return
        if "schema" not in cls.__dict__:
            raise SchemaNotDeclaredError(f"{cls.__name__} must define `schema`: a pandera DataFrameModel, or SCHEMA_FROM_ARGUMENT")
        leftovers = [name for name in ("schema_validations", "schema_metrics") if name in cls.__dict__]
        if leftovers:  # the model's own validations and metrics would replace them without a word
            raise TypeError(f"{cls.__name__} sets {' and '.join(leftovers)}, which the model replaces. {cls.REMOVED_DECLARATIONS}")
        if cls.schema is SCHEMA_FROM_ARGUMENT or (inspect.isclass(cls.schema) and issubclass(cls.schema, DataFrameModel)):
            return
        raise TypeError(f"{cls.__name__}.schema must be a pandera DataFrameModel (or SCHEMA_FROM_ARGUMENT), got {cls.schema!r}. {cls.REMOVED_DECLARATIONS}")

    async def async_read(self):
        """Allows the use of asyncio to concurrently read files in memory.

        Returns:
            pd.DataFrame | Iterable: A pandas dataframe or an iterable.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(pool, self.read)

    def read(self) -> Optional[pd.DataFrame]:
        """Reads data source and returns a schema validated dataframe (by means of _apply_schema).

        Returns:
            A pandas dataframe or an iterable. None if the source does not exist and `missing_ok` is True.
        """
        engine = EngineSetting.use(self._model, resource=self)  # checks the model again: `configure()` may have changed the engine since `__init__`
        try:
            df = engine.read(self)
        except FileNotFoundError as err:
            if not self.missing_ok:
                raise
            logger.warning(f"Source {self.name!r} does not exist and missing_ok is set: returning None. ({err})")
            return None

        df = self._apply_schema(df)
        if self.apply_schema_validations:
            self.validate_from_schema(df)
        if self.log_schema_metrics:
            self.log_metrics_from_schema(df)

        return df

    async def async_write(self, df: pd.DataFrame):
        """Allows the use of asyncio to concurrently write files out.

        Args:
            df: The data to be written
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(pool, self.write, df)

    def write(self, df: pd.DataFrame):
        """Sink data to a given source based on the sources_config.

        Args:
            df: The data to be written
        """
        engine = EngineSetting.use(self._model, (df,), resource=self)
        # Decision 12 of ADR 0010: the validations and the metrics see the cast frame, as on a read. 11.x validated the frame of the caller.
        df = self._apply_schema(engine.select_columns(self, df))

        if self.apply_schema_validations:
            self.validate_from_schema(df)
        if self.log_schema_metrics:
            self.log_metrics_from_schema(df)

        engine.write(self, df)

    def scan(self) -> Any:
        """Read the source lazily: a polars ``LazyFrame`` on the polars engine, with the casts of the model in its plan.

        A failed cast raises when the frame is collected. A lazy frame has no rows before it is collected, so ``scan()`` does not validate the frame
        or log its metrics. Call ``validate_from_schema`` on the collected frame, or use ``read()``.

        Returns:
            A polars ``LazyFrame``. None if the source does not exist and `missing_ok` is True.

        Raises:
            NotImplementedError: On the pandas engine, which has no lazy frame.
        """
        engine = EngineSetting.use(self._model, resource=self, operation="scan")
        try:
            frame = engine.scan(self)
        except FileNotFoundError as err:
            if not self.missing_ok:
                raise
            logger.warning(f"Source {self.name!r} does not exist and missing_ok is set: returning None. ({err})")
            return None
        if self.apply_schema_validations or self.log_schema_metrics:
            logger.warning(f"scan() of {self.name!r} does not validate or log metrics: a lazy frame has no rows before it is collected. Use read(), or validate the collected frame.")
        return self._apply_schema(frame)

    def sink(self, df: Any) -> None:
        """Write a lazy frame to the source: a polars ``LazyFrame`` on the polars engine.

        The engine runs the write, the validations and the metrics of the schema in one ``collect_all``. The write goes to a temporary file, and the
        destination changes only after the validations pass. The metrics are logged after the validations pass. ``sink()`` does not call
        ``validate_from_schema`` or ``log_metrics_from_schema``.

        Args:
            df: The lazy frame to write. A polars ``DataFrame`` is written as its lazy frame. A list or a tuple of frames is the parts of one frame, in
                their order: each part gets the casts of the schema, and the sink concatenates them.

        Raises:
            NotImplementedError: On the pandas engine, which has no lazy frame, and for a resource that the polars engine does not sink yet.
            ColumnsDataTypeError: A schema column is not in the frame, or a cast failed.
            SchemaValidationError: A validation failed. The destination does not change.
        """
        parts = tuple(df) if isinstance(df, (list, tuple)) else (df,)
        EngineSetting.use(self._model, parts, resource=self, operation="sink").sink(self, df)

    def footer(self) -> Optional[ParquetFooter]:
        """Read the footer of a parquet file without its rows: the arrow schema, the key-value metadata and the row count.

        The read is the same on the two engines, and does not fix the engine. A ``local`` file is read with pyarrow. An ``s3_file`` object is read with
        ranged ``GetObject`` calls on its end.

        Returns:
            The footer. None if the file does not exist and `missing_ok` is True.

        Raises:
            EngineResourceError: The resource is not a ``local`` or ``s3_file`` parquet file.
            FileNotFoundError: The file does not exist, and `missing_ok` is False. An S3 key raises ``S3KeyNotFoundError``.
        """
        try:
            return ParquetFooters.read(self)
        except FileNotFoundError as err:
            if not self.missing_ok:
                raise
            logger.warning(f"Source {self.name!r} does not exist and missing_ok is set: returning None. ({err})")
            return None

    def validate_from_schema(self, df: pd.DataFrame) -> "DynamicDataIO":
        """Validates a dataframe based on the validations present in its schema definition.

        All validations are checked and if any of them fails, a `SchemaValidationError` is raised.

        Args:
            df: A dataframe to be validated.

        Returns:
             self (to allow for method chaining).

        Raises:
            SchemaValidationError: if any of the validations failed. The `message` attribute of
                the exception object is a `List[str]`, where each element is the name of a
                validation that failed.
        """
        EngineSetting.current().validate(self, df)
        return self

    def log_metrics_from_schema(self, df: pd.DataFrame) -> "DynamicDataIO":
        """Calculates and logs metrics based on the metrics present in its schema definition.

        Args:
            df: A dataframe for which metrics are generated and logged

        Returns:
             self (to allow for method chaining).
        """
        EngineSetting.current().log_metrics(self, df)
        return self

    def _apply_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Called by the `self.read()` and the `self._write_to_local()` methods.

        Contrasts a dataframe's read from a given source against the class's schema dictionary,
        checking that columns are the same (by means of _has_columns and _has_valid_dtypes). Then,
        check if the columns are fine, it further validates if the types of columns conform to the
        expected schema. Finally, if schema types are different, then it attempts to apply schema;
        if possible then the schema validation is successful.

        Args:
            df: A pandas dataframe.

        Returns:
            A schema validated dataframe.
        """
        return EngineSetting.current().cast(self, df)

    @staticmethod
    def _transform_class_name_to_dataset_name(string_to_transform: str) -> str:
        """Called by the init function to fetch dataset names from class name.

        Used to create dataset name from class name, turns camel case into upper snake case.
        For example: 'ThisNameABC' -> 'THIS_NAME_ABC'.
        """
        words = re.findall(r"\d[A-Z]+|[A-Z]?[a-z\d]+|[A-Z]{2,}(?=[A-Z][a-z]|\d|\W|$)|\d+|[A-Z]{2,}|[A-Z]", string_to_transform)
        return "_".join(map(str.lower, words)).upper()

    @staticmethod
    def _is_declared_dtype(declared: str, found: Any) -> bool:
        """Whether a column's dtype is the declared one.

        Compares dtypes, not their names: the name of `string[pyarrow]` is `string`, which never equals the declared string and recast the column on every read.
        A bare `category` accepts any categorical dtype, and a bare `string` any string storage, as their names did: only `string[python]` or `string[pyarrow]`
        ask for one storage. A declared type pandas does not know is not declared by anything: the cast reports it.
        """
        try:
            expected = pd.api.types.pandas_dtype(declared)
        except TypeError:
            return False
        if isinstance(expected, pd.CategoricalDtype) and expected.categories is None:
            return isinstance(found, pd.CategoricalDtype)
        if isinstance(expected, pd.StringDtype) and declared.strip() == "string":
            return isinstance(found, pd.StringDtype)
        return bool(expected == found)

    def _has_valid_dtypes(self, df: pd.DataFrame) -> bool:
        """Checks if `df` has the expected dtypes defined in `schema`.

        If `strict_dtype_check=True`, performs row-wise type inspection and logs warnings if
        multiple Python-level types are detected in any column.

        Args:
            df: A pandas dataframe to be validated.

        Returns:
            bool: True if column types match schema expectations or can be casted, False otherwise.
        """
        dtypes = df.dtypes

        cols_allowed, cols_not_allowed = self._split_columns_to_validate_between_allowed_and_not(columns_to_validate=self._columns_to_validate())
        if cols_not_allowed:
            logger.exception(f"The columns passed: {cols_not_allowed} do not belong to the schema")
            return False

        for col_info in cols_allowed:
            column_name = col_info.name
            expected_dtype = col_info.data_type
            found_dtype = dtypes[column_name].name

            if not self._is_declared_dtype(expected_dtype, dtypes[column_name]):
                if self.show_casting_warnings:
                    logger.info(f"Expected: '{expected_dtype}' dtype for {self.name}['{column_name}'], " f"found '{found_dtype}'")
                try:
                    if self.strict_dtype_check:
                        logger.warning(f"[strict_dtype_check=True] Performing expensive type-checking for {self.name}['{column_name}']. This may impact performance on large datasets.")
                        num_types = df[column_name].map(type).nunique()
                        if num_types > 1:
                            logger.warning(CASTING_WARNING_MSG.format(column_name, expected_dtype, found_dtype))
                            logger.info(NOTICE_MSG.format(column_name))

                    df[column_name] = df[column_name].astype(expected_dtype)
                except (ValueError, TypeError):
                    logger.exception(f"ValueError: Tried casting column {self.name}['{column_name}'] to " f"'{expected_dtype}' from '{found_dtype}', but failed")
                    return False

        return True

    def _columns_to_validate(self) -> List[str]:
        # A string `columns` is a path template field (for example `data/{columns}.parquet`), not a column selection
        columns = self.options.get("columns")
        return self.schema.column_names if columns is None or isinstance(columns, str) else columns

    def _split_columns_to_validate_between_allowed_and_not(self, columns_to_validate: List[str]) -> Tuple[List[SchemaColumn], List[str]]:
        not_allowed_cols, allowed_cols = [], []
        for col in columns_to_validate:
            col_from_schema = self.schema.columns.get(col)
            if col_from_schema:
                allowed_cols.append(col_from_schema)
            else:
                not_allowed_cols.append(col)
        return allowed_cols, not_allowed_cols

    @staticmethod
    def _get_options(options_from_code: MutableMapping[str, Any], options_from_resource_definition: Optional[Mapping[str, Any]]) -> MutableMapping[str, Any]:
        """Retrieves options either from code or from a resource-definition.

        Options are merged if they are provided by both sources, while in the case of conflicts, the options from the code
        take precedence.

        Args:
            options_from_code (Optional[Mapping])
            options_from_resource_definition (Optional[Mapping])

        Returns:
            [Optional[Mapping]]: options that are going to be used
        """
        if options_from_resource_definition:
            return {**options_from_resource_definition, **options_from_code}
        return options_from_code
