"""Implements the DynamicDataIO class which provides functionality for data: loading; sinking, and; schema validation."""
# pylint: disable=no-member
__all__ = ["DynamicDataIO", "SCHEMA_FROM_FILE"]

import asyncio
import inspect
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Mapping, MutableMapping, Optional

import pandas as pd  # type: ignore
from magic_logger import logger

from dynamicio import validations
from dynamicio.errors import CASTING_WARNING_MSG, NOTICE_MSG, ColumnsDataTypeError, MissingSchemaDefinition, SchemaNotFoundError, SchemaValidationError
from dynamicio.metrics import get_metric

SCHEMA_FROM_FILE = {"schema": object()}

pool = ThreadPoolExecutor()


class DynamicDataIO:
    """Given a `src.utils.dynamicio.config.IOConfig` object, it generates an object with access to a series of methods for cloud I/O operations and data validations.

    Example:
       >>> input_sources_config = IOConfig(
       >>>     "path_to/input.yaml",
       >>>     os.getenv("ENVIRONMENT",default="LOCAL")
       >>> )
       >>>
       >>> class CmVolumesIO(DynamicDataIO):
       >>>     schema = {
       >>>         "id": "object",
       >>>         "product_id": "object",
       >>>         "tonnes": "float64",
       >>>         "cubic_metres": "float64",
       >>>     }
       >>>
       >>>     @staticmethod
       >>>     def validate(df: pd.DataFrame):
       >>>         pass
       >>>
       >>> cm_volumes_local_mapping = input_config.get(source_key="CM_VOLUMES")
       >>> cm_volumes_io = CmVolumesIO(cm_volumes_local_mapping, model=CmVolume)
       >>> cm_volumes_df = cm_volumes_io.read()
    """

    schema: Mapping

    def __init__(
        self,
        source_config: Mapping,
        apply_schema_validations: bool = False,
        log_schema_metrics: bool = False,
        show_casting_warnings: bool = False,
        **options: MutableMapping[str, Any],
    ):
        """Class constructor.

        Args:
            source_config: Configuration to use when reading/writing data from/to a source
            apply_schema_validations: Applies schema validations on either read() or write()
            log_schema_metrics: Logs schema metrics on either read() or write()
            show_casting_warnings: Logs casting warnings on either read() or write() if set to True
            options: Any additional kwargs that may be used throughout the lifecycle of the object
        """
        if type(self) is DynamicDataIO:  # pylint: disable=unidiomatic-typecheck
            raise TypeError("Abstract class DynamicDataIO cannot be used to instantiate an object...")

        self.sources_config = source_config
        self.name = self._transform_class_name_to_dataset_name(self.__class__.__name__)
        self.apply_schema_validations = apply_schema_validations
        self.log_schema_metrics = log_schema_metrics
        self.show_casting_warnings = show_casting_warnings
        self.options = self._get_options(options, source_config.get("options"))
        source_name = self.sources_config.get("type")
        if self.schema is SCHEMA_FROM_FILE:
            try:
                self.schema = self.sources_config["schema"]
                self.name = self.sources_config["name"].upper()
                self.schema_validations = self.sources_config["validations"]
                self.schema_metrics = self.sources_config["metrics"]
            except KeyError as _error:
                raise SchemaNotFoundError() from _error

        assert hasattr(self, f"_read_from_{source_name}") or hasattr(
            self, f"_write_to_{source_name}"
        ), f"No method '_read_from_{source_name}' or '_write_to_{source_name}'. Have you registered a mixin for {source_name}?"

    def __init_subclass__(cls):
        """Ensure that all subclasses have a `schema` attribute and a `validate` method.

        Raises:
            AssertionError: If either of the attributes is not implemented
        """
        if not inspect.getmodule(cls).__name__.startswith("dynamicio"):
            assert "schema" in cls.__dict__

            if cls.schema is None or (cls.schema is not SCHEMA_FROM_FILE and len(cls.schema) == 0):
                raise ValueError(f"schema for class {cls} cannot be None or empty...")

    async def async_read(self):
        """Allows the use of asyncio to concurrently read files in memory.

        Returns:
            A pandas dataframe or an iterable.
        """
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(pool, self.read)

    def read(self) -> pd.DataFrame:
        """Reads data source and returns a schema validated dataframe (by means of _apply_schema).

        Returns:
            A pandas dataframe or an iterable.
        """
        source_name = self.sources_config.get("type")
        df = getattr(self, f"_read_from_{source_name}")()

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
        source_name = self.sources_config.get("type")
        if set(list(df.columns)) != set(self.schema.keys()):  # pylint: disable=E1101
            columns = [column for column in df.columns.to_list() if column in self.schema.keys()]
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
            df:

        Returns:
             self (to allow for method chaining).

        Raises:
            SchemaValidationError: if any of the validations failed. The `message` attribute of
                the exception object is a `List[str]`, where each element is the name of a
                validation that failed.
        """
        if not hasattr(self, "schema_validations"):
            raise MissingSchemaDefinition(self.__class__)

        failed_validations = {}
        for column in self.schema_validations.keys():
            for validation in self.schema_validations[column].keys():
                if self.schema_validations[column][validation]["apply"] is True:
                    validation_result = getattr(validations, validation)(self.name, df, column, **self.schema_validations[column][validation]["options"])
                    if not validation_result.valid:
                        failed_validations[validation] = validation_result.message

        if len(failed_validations) > 0:
            raise SchemaValidationError(failed_validations)

        return self

    def log_metrics_from_schema(self, df: pd.DataFrame) -> "DynamicDataIO":
        """Calculates and logs metrics based on the metrics present in its schema definition.

        Args:
            df: A dataframe for which metrics are generated and logged

        Returns:
             self (to allow for method chaining).
        """
        if not hasattr(self, "schema_metrics"):
            raise MissingSchemaDefinition(self.__class__)

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

    def _has_valid_dtypes(self, df: pd.DataFrame) -> bool:
        """Checks if `df` has the expected dtypes defined in `schema`.

        Schema is a dictionary object where keys are column names and values are dtypes in string format as returned by e.g.
        `df[column].dtype.name`.

        This function issues `error` level logs describing the first column that caused the check to fail.

        It is assumed that `df` only has the columns defined in `schema`.

        Args:
            df:

        Returns:
            bool - `True` if `df` has the given dtypes, `False` otherwise
        """
        dtypes = df.dtypes

        for column_name, expected_dtype in self.schema.items():
            found_dtype = dtypes[column_name].name
            if found_dtype != expected_dtype:
                if self.show_casting_warnings:
                    logger.info(f"Expected: '{expected_dtype}' dtype for {self.name}['{column_name}]', found '{found_dtype}'")
                try:
                    if len(set([type(v) for v in df[column_name].values])) > 1:  # pylint: disable=consider-using-set-comprehension
                        logger.warning(CASTING_WARNING_MSG.format(column_name, expected_dtype, found_dtype))  # pylint: disable=logging-format-interpolation
                        logger.info(NOTICE_MSG.format(column_name))  # pylint: disable=logging-format-interpolation
                    df[column_name] = df[column_name].astype(self.schema[column_name])
                except (ValueError, TypeError):
                    logger.error(f"ValueError: Tried casting column {self.name}['{column_name}]' to '{expected_dtype}' " f"from '{found_dtype}', but failed")
                    return False
        return True

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
