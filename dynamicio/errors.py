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
    "DatasetNameNotFoundError",
    "TableNameNotFoundError",
    "SchemaValidationError",
    "InvalidDatasetTypeError",
    "NoObjectsFoundError",
    "ObjectNotFoundError",
    "S3KeyNotFoundError",
    "SchemaNotDeclaredError",
    "MissingMixinError",
    "InvalidS3UrlError",
    "CASTING_WARNING_MSG",
    "NOTICE_MSG",
]

from typing import Any, Optional

from awswrangler.exceptions import NoFilesFound
from botocore.exceptions import ClientError


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


class DatasetNameNotFoundError(DynamicIOError):
    """Error raised when a model does not name its dataset."""

    ERROR_STR = "A model must name its dataset"
    ERROR_STR_DETAILED = 'The model {0!r} has no dataset name: declare it with `class {0}(DataModel, dataset="...")` (or `__dataset_name__ = "..."` on a plain Pandera model)'


class TableNameNotFoundError(DynamicIOError):
    """Error raised when a model used with a Postgres resource does not name its table."""

    ERROR_STR = "A model used with a Postgres resource must name its table"
    ERROR_STR_DETAILED = (
        "The model of dataset {0!r} is used with a Postgres resource but names no table: declare `table=` (or `__table_name__`) with the exact name of the table in the database"
    )


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
    """Error raised when an S3 path prefix, an S3 parquet `file_path` that ends in `/`, or a `local_batch` source has no objects.

    It is a `FileNotFoundError`, so a caller can treat it as a missing source. It is also a `ValueError`: in earlier releases, a path prefix
    or a `local_batch` source with no objects raised the pandas `ValueError: No objects to concatenate`, and callers that catch `ValueError` must not change.
    """


class ObjectNotFoundError(FileNotFoundError, NoFilesFound):
    """Error raised when the S3 key of a csv or json `file_path` does not exist.

    It is a `FileNotFoundError`, so a caller can treat it as a missing source. It is also an awswrangler `NoFilesFound`: earlier releases raised
    `NoFilesFound` for a missing key, and callers that catch `NoFilesFound` must not change.
    """


class S3KeyNotFoundError(FileNotFoundError, ClientError):
    """Error raised when the S3 key of a parquet or hdf `file_path` does not exist.

    It is a `FileNotFoundError`, so a caller can treat it as a missing source. It is also the botocore `ClientError` that S3 returned: earlier
    releases raised that `ClientError` for a missing key, and callers that catch `ClientError` or read its `response` must not change.
    """

    MISSING_KEY_CODES = ("NoSuchKey", "404")  # GetObject returns NoSuchKey; HeadObject, which `download_fileobj` sends first, returns a bare 404

    def __init__(self, s3_path: str, error: ClientError):
        """Wrap the `ClientError` of a missing key, keeping its `response` and `operation_name`."""
        ClientError.__init__(self, error.response, error.operation_name)
        self.s3_path = s3_path
        self.args = (f"No object found at {s3_path}: {error}",)

    def __reduce__(self):
        """Rebuild from the arguments of `__init__`, so the error survives `pickle` (a process pool worker) and `copy`."""
        return type(self), (self.s3_path, ClientError(self.response, self.operation_name))

    def __str__(self) -> str:
        """The message, not the `OSError` format (which expects errno and strerror)."""
        return self.args[0]

    @classmethod
    def is_missing_key(cls, error: ClientError) -> bool:
        """Whether `error` is the S3 reply for a key that does not exist."""
        return error.response.get("Error", {}).get("Code") in cls.MISSING_KEY_CODES


class SchemaNotDeclaredError(DynamicIOError, TypeError, AssertionError):
    """Error raised when an IO class does not set `schema`.

    It is a `TypeError`, as the other errors of a wrong IO class declaration are. It is also an `AssertionError`: earlier releases checked this with
    `assert`, and callers that catch `AssertionError` must not change.
    """


class MissingMixinError(DynamicIOError, TypeError, AssertionError):
    """Error raised when an IO class has no mixin that reads or writes the type of its resource.

    It is a `TypeError`, as the other errors of a wrong IO class declaration are. It is also an `AssertionError`: earlier releases checked this with
    `assert`, and callers that catch `AssertionError` must not change.
    """


class InvalidS3UrlError(DynamicIOError, ValueError, AssertionError):
    """Error raised when an S3 operation gets a URL that does not start with `s3://`.

    It is a `ValueError`. It is also an `AssertionError`: earlier releases checked this with `assert`, and callers that catch `AssertionError` must not change.
    """


# Warning messages
CASTING_WARNING_MSG = "Applying casting column: '{0}' to: 'type:{1}' from 'type:{2}' though not advised, as `dtypes`>1 for {0}, which may lead to data corruption!"
NOTICE_MSG = "Keeping the {0} as is, may anyway cause I/O errors or data corruption issues especially when using `pandas.DataFrame.to_parquet` or `pandas.DataFrame.to_json`."
