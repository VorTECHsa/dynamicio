"""Hosts exception implementations for different errors."""
# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, super-init-not-called
__all__ = [
    "DynamicIOError",
    "DataSourceError",
    "ColumnsDataTypeError",
    "NonUniqueIdColumnError",
    "NullValueInColumnError",
    "NotExpectedCategoricalValue",
    "MissingSchemaDefinition",
    "SchemaNotFoundError",
    "SchemaValidationError",
    "InvalidDatasetTypeError",
    "CASTING_WARNING_MSG",
    "NOTICE_MSG",
]

from typing import Any, Optional


class DynamicIOError(Exception):
    """Base class for DynamicIO errors."""

    ERROR_STR: str = ""
    ERROR_STR_DETAILED: str = "{0}"

    @property
    def message(self) -> Optional[Any]:
        """Easy access for optional message argument.

        Returns:
            Message or `None` if not set
        """
        try:
            return self.args[0]
        except IndexError:
            return None

    def __str__(self):
        """Enrich and return error message."""
        message = self.message

        if message is None:
            return self.ERROR_STR

        return self.ERROR_STR_DETAILED.format(message)


class SchemaNotFoundError(DynamicIOError):
    """Error raised when schema is not specified in the provided source."""

    ERROR_STR = "Schema not specified in the provided source"
    ERROR_STR_DETAILED = "Schema not specified in the provided source: {0} "


class SchemaValidationError(DynamicIOError):
    """Error raised when schema validation fails."""


class MissingSchemaDefinition(DynamicIOError):
    """Error raised when schema is not specified in the provided source."""

    ERROR_STR = "The resource definition for this class is missing a schema definition"
    ERROR_STR_DETAILED = "The resource definition for this class is missing a schema definition: {0}"


class DataSourceError(DynamicIOError):
    """Error raised when the data source fails to load."""


class ColumnsDataTypeError(DynamicIOError):
    """Error raised when the validated data does not have the expected data types."""


class NonUniqueIdColumnError(DynamicIOError):
    """Error raised when the data source fails to load."""


class NullValueInColumnError(DynamicIOError):
    """Error raised when the data source fails to load."""


class NotExpectedCategoricalValue(DynamicIOError):
    """Error raised when the data source fails to load."""


class InvalidDatasetTypeError(DynamicIOError):
    """Error raised when dataset type is not one of [parquet, json, csv, h5]."""

    ERROR_STR = "The dataset provided is not amongst the supported types (parquet, json, csv, h5) handled by dynamicio."
    ERROR_STR_DETAILED = "Dataset: {0} provided is not amongst the supported types (parquet, json, csv, h5) handled by dynamicio."


# Warning messages
CASTING_WARNING_MSG = "Applying casting column: '{0}' to: 'type:{1}' from 'type:{2}' though not advised, as `dtypes`>1 for {0}, which may lead to data corruption!"
NOTICE_MSG = "Keeping the {0} as is, may anyway cause I/O errors or data corruption issues especially when using `pandas.DataFrame.to_parquet` or `pandas.DataFrame.to_json`."
