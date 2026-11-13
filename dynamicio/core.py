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
from pandera.pandas import DataFrameModel

# Application Imports
from dynamicio.config.pydantic import DataframeSchema, IOEnvironment, SchemaColumn
from dynamicio.errors import CASTING_WARNING_MSG, NOTICE_MSG, ColumnsDataTypeError, SchemaNotFoundError, SchemaValidationError, TableNameNotFoundError
from dynamicio.schema import validations
from dynamicio.schema.behaviour import Behaviour
from dynamicio.schema.metrics import get_metric
from dynamicio.schema.models import dataframe_validator, to_dataframe_schema

logger = logging.getLogger(__name__)


class _SchemaFromArgument:
    """The type of `SCHEMA_FROM_ARGUMENT`."""

    def __repr__(self) -> str:
        return "SCHEMA_FROM_ARGUMENT"


SCHEMA_FROM_ARGUMENT = _SchemaFromArgument()  # `schema = SCHEMA_FROM_ARGUMENT` on an IO class: each instance gets its model from the `schema=` argument

pool = ThreadPoolExecutor()


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
        apply_schema_validations: Optional[bool] = None,
        log_schema_metrics: Optional[bool] = None,
        show_casting_warnings: Optional[bool] = None,
        strict_dtype_check: Optional[bool] = None,
        *,
        schema: Optional[Type[DataFrameModel]] = None,
        missing_ok: bool = False,
        **options: MutableMapping[str, Any],
    ):
        """Class constructor.

        Args:
            source_config: Configuration to use when reading/writing data from/to a source
            apply_schema_validations: Applies schema validations on either read() or write(). Default: what the model's `__io__` declares, else False
            log_schema_metrics: Logs schema metrics on either read() or write(). Default: what the model's `__io__` declares, else False
            show_casting_warnings: Logs casting warnings on either read() or write() if set to True. Default: what the model's `__io__` declares, else False
            strict_dtype_check: Applies strict dtype check on either read() or write(), confirming the correct type for all rows per column, if set to True. Default: what the model's `__io__` declares, else False
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
        self._dataframe_validator = None  # the dataframe-level part of a Pandera model, if any
        model = None
        if schema is not None:
            if not (inspect.isclass(schema) and issubclass(schema, DataFrameModel)):
                raise TypeError(f"schema must be a pandera DataFrameModel subclass, got {schema!r}")
            model = schema
            active_schema = to_dataframe_schema(schema)
            self._dataframe_validator = dataframe_validator(schema)
        elif self.schema is SCHEMA_FROM_ARGUMENT:
            active_schema = None
        else:  # `__init_subclass__` has checked that the class attribute is a model
            model = self.schema
            active_schema = to_dataframe_schema(self.schema)
            self._dataframe_validator = dataframe_validator(self.schema)

        declared = Behaviour.of(model) if model is not None else Behaviour()
        self.apply_schema_validations = Behaviour.resolve(apply_schema_validations, declared.validate)
        self.log_schema_metrics = Behaviour.resolve(log_schema_metrics, declared.log_metrics)
        self.show_casting_warnings = Behaviour.resolve(show_casting_warnings, declared.show_casting_warnings)
        self.strict_dtype_check = Behaviour.resolve(strict_dtype_check, declared.strict_dtype_check)

        if not active_schema:
            raise SchemaNotFoundError()

        assert isinstance(active_schema, DataframeSchema)
        if source_name == "postgres" and not active_schema.table_name:
            raise TableNameNotFoundError(active_schema.name)
        self.schema = active_schema
        self.name = self.schema.name.upper()
        self.schema_validations = self.schema.validations
        self.schema_metrics = self.schema.metrics

        assert hasattr(self, f"_read_from_{source_name}") or hasattr(
            self, f"_write_to_{source_name}"
        ), f"No method '_read_from_{source_name}' or '_write_to_{source_name}'. Have you registered a mixin for {source_name}?"

    def __init_subclass__(cls):
        """Ensure that all subclasses have a `schema` attribute and a `validate` method.

        Raises:
            AssertionError: If either of the attributes is not implemented
        """
        if not inspect.getmodule(cls).__name__.startswith("dynamicio"):
            assert "schema" in cls.__dict__, f"{cls.__name__} must define `schema`: a pandera DataFrameModel, or SCHEMA_FROM_ARGUMENT"

            if cls.schema is SCHEMA_FROM_ARGUMENT or (inspect.isclass(cls.schema) and issubclass(cls.schema, DataFrameModel)):
                return
            raise TypeError(
                f"{cls.__name__}.schema must be a pandera DataFrameModel (or SCHEMA_FROM_ARGUMENT), got {cls.schema!r}. "
                "Dict schemas (`schema = {...}`, `schema_validations`, `schema_metrics`) and YAML schemas (`SCHEMA_FROM_FILE`) were removed in dynamicio 11: "
                "declare the columns as a model, see https://dynamicio.readthedocs.io/en/latest/guides/schemas-and-validation/ (`dynamicio migrate` converts YAML schemas)."
            )

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
        source_name = self.sources_config.data_backend_type.value
        try:
            df = getattr(self, f"_read_from_{source_name}")()
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
        source_name = self.sources_config.data_backend_type.value
        if set(df.columns) != self.schema.column_names:  # pylint: disable=E1101
            columns = [column for column in df.columns.to_list() if column in self.schema.column_names]
            df = df[columns]

        if self.apply_schema_validations:
            self.validate_from_schema(df)
        if self.log_schema_metrics:
            self.log_metrics_from_schema(df)

        getattr(self, f"_write_to_{source_name}")(self._apply_schema(df))

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
        failed_validations: dict = {}
        for column in self.schema_validations.keys():
            col_validations = self.schema_validations[column]
            for validation in col_validations:
                if validation.apply:
                    validator = validations.pandera_check if "pandera_check" in validation.options else validations.ALL_VALIDATORS[validation.name]
                    validation_result = validator(self.name, df, column, **validation.options)
                    if not validation_result.valid:
                        failed_validations.setdefault(validation.name, []).append(validation_result.message)

        if getattr(self, "_dataframe_validator", None) is not None:
            for name, message in validations.dataframe_failures(self.name, df, self._dataframe_validator).items():
                failed_validations.setdefault(name, []).append(message)

        if len(failed_validations) > 0:
            # One entry per validation name; when several columns fail the same validation their messages are joined, none are lost.
            raise SchemaValidationError({name: "; ".join(messages) for name, messages in failed_validations.items()})

        return self

    def log_metrics_from_schema(self, df: pd.DataFrame) -> "DynamicDataIO":
        """Calculates and logs metrics based on the metrics present in its schema definition.

        Args:
            df: A dataframe for which metrics are generated and logged

        Returns:
             self (to allow for method chaining).
        """
        for column in self.schema_metrics.keys():
            for metric in self.schema_metrics[column]:
                get_metric(metric)(self.name, df, column)()  # type: ignore

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
        if not self._has_valid_dtypes(df):
            raise ColumnsDataTypeError()
        return df

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
