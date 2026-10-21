"""Behaviour contract: how schemas are declared and how dtypes are enforced.

Covers the two declaration styles that exist today (YAML schema file, in-code ``schema`` dict plus ``schema_validations`` /
``schema_metrics``), and the dtype-casting behaviour that read() and write() apply.
"""

# pylint: disable=missing-class-docstring, missing-function-docstring
import pandas as pd
import pytest
from pydantic import ValidationError

from dynamicio import UnifiedIO
from dynamicio.config.pydantic.table_schema import DataframeSchema, SchemaColumn
from dynamicio.errors import ColumnsDataTypeError


class TestYamlSchemaShape:
    @pytest.mark.unit
    def test_column_order_is_preserved(self):
        schema = DataframeSchema(name="s", columns={"z": {"type": "int64"}, "a": {"type": "object"}, "m": {"type": "float64"}})

        assert list(schema.columns) == ["z", "a", "m"]

    @pytest.mark.unit
    def test_validations_and_metrics_default_to_empty(self):
        column = SchemaColumn(name="c", type="int64")

        assert not list(column.validations) and not list(column.metrics)

    @pytest.mark.unit
    def test_null_metrics_are_treated_as_empty(self):
        assert not list(SchemaColumn(name="c", type="int64", metrics=None).metrics)

    @pytest.mark.unit
    def test_validations_are_remapped_from_name_keyed_mapping_to_a_list_in_order(self):
        column = SchemaColumn(
            name="c", type="int64", validations={"is_greater_than": {"apply": True, "options": {"threshold": 1}}, "has_no_null_values": {"apply": False, "options": {}}}
        )

        assert [(v.name, v.apply, dict(v.options)) for v in column.validations] == [("is_greater_than", True, {"threshold": 1}), ("has_no_null_values", False, {})]

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "bad_column",
        [
            {"type": "int64", "metrics": ["Median"]},  # unknown metric
            {"type": "int64", "validations": ["has_no_null_values"]},  # validations must be a mapping
            {"type": "int64", "validations": {"has_no_null_values": {"options": {}}}},  # `apply` is required
            {"type": "int64", "validations": {"has_no_null_values": {"apply": True}}},  # `options` is required
            {"type": "not_a_dtype"},
            {},  # type is required
        ],
    )
    def test_invalid_column_definitions_are_rejected(self, bad_column):
        with pytest.raises((ValidationError, ValueError)):
            SchemaColumn(name="c", **bad_column)

    @pytest.mark.unit
    def test_unknown_validator_names_are_accepted_when_the_schema_is_loaded(self):
        # Quirk: names are not checked against the registry when the schema is loaded.
        column = SchemaColumn(name="c", type="int64", validations={"no_such_validator": {"apply": True, "options": {}}})

        assert column.validations[0].name == "no_such_validator"


class TestInCodeSchemaDeclaration:
    @pytest.mark.unit
    def test_subclass_without_schema_attribute_is_rejected(self):
        with pytest.raises(AssertionError):

            class _NoSchema(UnifiedIO):  # pylint: disable=unused-variable
                pass

    @pytest.mark.unit
    def test_empty_schema_is_rejected(self):
        with pytest.raises(ValueError):

            class _EmptySchema(UnifiedIO):  # pylint: disable=unused-variable
                schema = {}

    @pytest.mark.unit
    def test_validations_and_metrics_are_optional(self, make_io, valid_df, metric_records):
        class Bare(UnifiedIO):
            schema = {"a": "int64", "b": "object", "c": "float64"}

        out = make_io(valid_df, io_class=Bare, apply_schema_validations=True, log_schema_metrics=True).read()

        assert list(out.columns) == ["a", "b", "c"] and metric_records() == []


class TestDtypeEnforcement:
    @pytest.mark.unit
    def test_read_casts_castable_columns_to_the_schema_dtypes(self, make_io):
        df = pd.DataFrame({"a": [2.0, 3.0, 4.0], "b": ["x", "y", "x"], "c": [1, 2, 3]})

        out = make_io(df).read()

        assert out.dtypes.astype(str).to_dict() == {"a": "int64", "b": "object", "c": "float64"}

    @pytest.mark.unit
    def test_read_raises_columns_data_type_error_when_a_column_cannot_be_cast(self, make_io):
        df = pd.DataFrame({"a": ["not a number", "x", "y"], "b": ["x", "y", "x"], "c": [1.0, 2.0, 3.0]})

        with pytest.raises(ColumnsDataTypeError):
            make_io(df).read()

    @pytest.mark.unit
    def test_read_returns_only_the_schema_columns(self, make_io, valid_df):
        out = make_io(valid_df.assign(extra=1)).read()

        assert list(out.columns) == ["a", "b", "c"]

    @pytest.mark.unit
    def test_columns_option_naming_a_column_outside_the_schema_raises(self, make_io, valid_df):
        with pytest.raises(ColumnsDataTypeError):
            make_io(valid_df, columns=["a", "nope"]).read()

    @pytest.mark.unit
    def test_write_raises_columns_data_type_error_when_a_column_cannot_be_cast(self, make_io, valid_df):
        with pytest.raises(ColumnsDataTypeError):
            make_io().write(valid_df.assign(a=["x", "y", "z"]))
