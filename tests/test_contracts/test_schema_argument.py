"""Per-instance ``schema=`` argument: one IO class, the schema chosen per resource."""

# pylint: disable=missing-class-docstring, missing-function-docstring, redefined-outer-name

import pytest

from dynamicio import UnifiedIO
from dynamicio.core import SCHEMA_FROM_ARGUMENT
from dynamicio.errors import SchemaNotFoundError, SchemaValidationError
from tests.test_contracts.conftest import ContractModel


class ArgumentIO(UnifiedIO):
    schema = SCHEMA_FROM_ARGUMENT


@pytest.mark.unit
@pytest.mark.parametrize("make_io", ["pandera_schema"], indirect=True)
class TestSchemaArgument:
    def test_schema_argument_is_used(self, make_io, valid_df):
        io = make_io(valid_df, io_class=ArgumentIO, schema=ContractModel)
        assert io.name == "CONTRACT_DATASET"
        assert list(io.read().columns) == ["a", "b", "c"]

    def test_schema_argument_validates_like_a_class_attribute(self, make_io, invalid_df):
        io = make_io(invalid_df, io_class=ArgumentIO, schema=ContractModel, apply_schema_validations=True)
        with pytest.raises(SchemaValidationError):
            io.read()

    def test_missing_schema_raises(self, make_io, valid_df):
        with pytest.raises(SchemaNotFoundError):
            make_io(valid_df, io_class=ArgumentIO)

    def test_argument_overrides_class_attribute(self, make_io, valid_df):
        class Other(UnifiedIO):
            """Class attribute that the argument must override."""

            schema = {"zzz": "int64"}

        io = make_io(valid_df, io_class=Other, schema=ContractModel)
        assert io.name == "CONTRACT_DATASET"

    def test_non_model_is_rejected(self, make_io, valid_df):
        with pytest.raises(TypeError):
            make_io(valid_df, io_class=ArgumentIO, schema={"a": "int64"})
