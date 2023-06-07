# pylint: disable=missing-module-docstring, missing-function-docstring
import pytest
from pydantic import ValidationError

from dynamicio.config.pydantic.table_schema import SchemaColumn


def test_schema_column_accepts_valid_pandas_types():
    column = SchemaColumn(name="test", type="int64")
    assert column.data_type == "int64"


def test_schema_column_rejects_invalid_pandas_types():
    with pytest.raises(ValidationError):
        SchemaColumn(name="test", type="not a type")
