# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, too-many-public-methods, too-few-public-methods
import pytest

# Application Imports
from dynamicio.errors import (
    ColumnsDataTypeError,
    DataSourceError,
    DynamicIOError,
    InvalidDatasetTypeError,
    MissingSchemaDefinition,
    NonUniqueIdColumnError,
    NotExpectedCategoricalValue,
    NullValueInColumnError,
    SchemaNotFoundError,
    SchemaValidationError,
)


class TestErrors:

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "exception_cls, init_arg, expected_str",
        [
            # ✅ Custom __str__ formatting
            (SchemaNotFoundError, "test-source", "Schema not specified in the provided source: test-source "),
            (MissingSchemaDefinition, "test-class", "The resource definition for this class is missing a schema definition: test-class"),
            (InvalidDatasetTypeError, "my_file.avro", "Dataset: my_file.avro provided is not amongst the supported types (parquet, json, csv, h5) handled by dynamicio."),
        ],
    )
    def test_custom_exceptions_format_output(self, exception_cls, init_arg, expected_str):
        # Given
        ex = exception_cls(init_arg)

        # When
        result_str = str(ex)

        # Then
        assert result_str == expected_str
        assert isinstance(ex, DynamicIOError)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "exception_cls",
        [
            # ✅ Subclasses without custom formatting
            SchemaValidationError,
            DataSourceError,
            ColumnsDataTypeError,
            NonUniqueIdColumnError,
            NullValueInColumnError,
            NotExpectedCategoricalValue,
        ],
    )
    def test_simple_dynamicio_exceptions_with_message(self, exception_cls):
        # Given
        ex = exception_cls("Something went wrong")

        # When
        result_str = str(ex)

        # Then
        assert result_str == "Something went wrong"
        assert ex.message == "Something went wrong"
        assert isinstance(ex, DynamicIOError)

    @pytest.mark.unit
    def test_dynamicio_base_exception_without_message(self):
        # Given
        ex = DynamicIOError()

        # When
        result_str = str(ex)

        # Then
        assert ex.message is None
        assert result_str == ""
