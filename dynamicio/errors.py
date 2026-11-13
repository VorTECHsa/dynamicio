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
    "NoObjectsFoundError",
    "ObjectNotFoundError",
    "UnknownEnvironmentError",
    "CASTING_WARNING_MSG",
    "NOTICE_MSG",
]

import difflib
from typing import Any, Iterable, Optional

from awswrangler.exceptions import NoFilesFound


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


class NoObjectsFoundError(FileNotFoundError, ValueError):
    """Error raised when an S3 path prefix, or an S3 parquet `file_path` that ends in `/`, has no objects.

    It is a `FileNotFoundError`, so a caller can treat it as a missing source. It is also a `ValueError`: in earlier releases, a path prefix
    with no objects raised the pandas `ValueError: No objects to concatenate`, and callers that catch `ValueError` must not change.
    """


class ObjectNotFoundError(FileNotFoundError, NoFilesFound):
    """Error raised when the S3 key of a csv or json `file_path` does not exist.

    It is a `FileNotFoundError`, so a caller can treat it as a missing source. It is also an awswrangler `NoFilesFound`: earlier releases raised
    `NoFilesFound` for a missing key, and callers that catch `NoFilesFound` must not change.
    """


class UnknownEnvironmentError(KeyError):
    """Error raised when a resource definition does not define the environment it is asked for.

    It is a `KeyError`, so a caller that catches the bare `KeyError` that earlier releases raised does not change. The message names the
    source, the file, the environments the source defines and, when there is one, the closest defined name.
    """

    def __init__(self, source_key: str, environment: str, defined: Iterable[str], path: Optional[str] = None):
        self.source_key = source_key
        self.environment = environment
        self.defined = list(defined)
        self.path = path
        super().__init__(environment)

    @property
    def suggestion(self) -> Optional[str]:
        """The defined environment closest to the requested one (ignoring case), or `None`."""
        by_folded_name = {name.casefold(): name for name in self.defined}
        matches = difflib.get_close_matches(self.environment.casefold(), list(by_folded_name), n=1)
        return by_folded_name[matches[0]] if matches else None

    def __str__(self):
        """Explain which environment is missing where, instead of the repr of the key that `KeyError` prints."""
        where = f" in {self.path}" if self.path else ""
        defined = ", ".join(self.defined) if self.defined else "none"
        message = f"Source '{self.source_key}'{where} has no environment '{self.environment}'. Defined: {defined}."
        if self.suggestion is not None:
            message += f" Did you mean '{self.suggestion}'?"
        return message


# Warning messages
CASTING_WARNING_MSG = "Applying casting column: '{0}' to: 'type:{1}' from 'type:{2}' though not advised, as `dtypes`>1 for {0}, which may lead to data corruption!"
NOTICE_MSG = "Keeping the {0} as is, may anyway cause I/O errors or data corruption issues especially when using `pandas.DataFrame.to_parquet` or `pandas.DataFrame.to_json`."
