# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, too-many-public-methods, too-few-public-methods
import pytest
from pydantic import ValidationError

# Application Imports
from dynamicio.config.pydantic.table_schema import SchemaColumn


class TestSchemaColumn:

    @pytest.mark.unit
    def test_schema_column_accepts_valid_pandas_types(self):
        column = SchemaColumn(name="test", type="int64")
        assert column.data_type == "int64"

    @pytest.mark.unit
    def test_schema_column_rejects_invalid_pandas_types(self):
        with pytest.raises(ValidationError):
            SchemaColumn(name="test", type="not a type")
