"""Behaviour contract: schema validations.

These tests pin what users observe today (exception type, payload shape, messages, ordering, when validations run), so
the validation engine can be replaced without changing behaviour. Where current behaviour is a quirk, the test says so.
"""

# pylint: disable=missing-class-docstring, missing-function-docstring, redefined-outer-name
import inspect
import os

import pandas as pd
import pytest

from dynamicio import validations
from dynamicio.errors import SchemaValidationError

PUBLIC_VALIDATORS = {
    "has_unique_values": ["dataset", "df", "column"],
    "has_no_null_values": ["dataset", "df", "column"],
    "has_acceptable_percentage_of_nulls": ["dataset", "df", "column", "threshold"],
    "is_in": ["dataset", "df", "column", "categorical_values", "match_all"],
    "is_greater_than": ["dataset", "df", "column", "threshold"],
    "is_greater_than_or_equal": ["dataset", "df", "column", "threshold"],
    "is_lower_than": ["dataset", "df", "column", "threshold"],
    "is_lower_than_or_equal": ["dataset", "df", "column", "threshold"],
    "is_between": ["dataset", "df", "column", "lower", "upper", "include_left", "include_right"],
}


@pytest.mark.unit
def test_validator_names_and_options_are_the_public_yaml_vocabulary():
    # Names and option names are what users write in YAML / schema_validations, so adding is fine, renaming is breaking.
    public = {name: list(inspect.signature(func).parameters) for name, func in validations.ALL_VALIDATORS.items() if not name.startswith("_")}

    assert public == PUBLIC_VALIDATORS


@pytest.mark.unit
def test_all_violations_are_collected_not_fail_fast_and_keyed_by_validation_name(make_io, invalid_df):
    name = make_io.dataset_name

    with pytest.raises(SchemaValidationError) as exc_info:
        make_io().validate_from_schema(invalid_df)

    assert exc_info.value.message == {
        "has_unique_values": f"Values [1] for {name}[a] are duplicated!",
        "is_greater_than": f"2 cell values for {name}[a] are below 1",
        "is_between": f"1 cell values for {name}[a] are either below 0 or above 5",
        "has_no_null_values": f"{name}[b] has 1 nulls; {name}[c] has 2 nulls",
        "is_in": f"Values {{'z'}} for {name}[b] are not acceptable for 2 cells",
        "has_acceptable_percentage_of_nulls": f"Percentage of nulls of for {name}[c] is 0.6666666666666666 which exceeds threshold: 0.3",
    }
    # iteration order: columns in schema order, validations in declaration order within a column
    assert list(exc_info.value.message) == ["has_unique_values", "is_greater_than", "is_between", "has_no_null_values", "is_in", "has_acceptable_percentage_of_nulls"]


@pytest.mark.unit
def test_same_validation_failing_on_two_columns_reports_every_column_joined_in_one_entry(make_io):
    # v10 fix: this used to keep only the last column's message. Keys are unchanged; the entry now holds all messages.
    df = pd.DataFrame({"a": [2, 3, 4], "b": [None, "x", "y"], "c": [None, 1.0, 1.0]})

    with pytest.raises(SchemaValidationError) as exc_info:
        make_io().validate_from_schema(df)

    name = make_io.dataset_name
    assert exc_info.value.message["has_no_null_values"] == f"{name}[b] has 1 nulls; {name}[c] has 1 nulls"


@pytest.mark.unit
def test_validations_flagged_apply_false_are_skipped(make_io):
    # c has `is_lower_than_or_equal` (threshold 1, apply: false); every value of c is above 1.
    df = pd.DataFrame({"a": [2, 3, 4], "b": ["x", "y", "x"], "c": [5.0, 6.0, 7.0]})

    assert make_io().validate_from_schema(df) is not None  # does not raise


@pytest.mark.unit
def test_valid_data_passes_and_returns_the_io_object_for_chaining(make_io, valid_df):
    io = make_io()

    assert io.validate_from_schema(valid_df) is io


@pytest.mark.unit
def test_exception_str_is_the_payload(make_io, invalid_df):
    with pytest.raises(SchemaValidationError) as exc_info:
        make_io().validate_from_schema(invalid_df)

    assert str(exc_info.value.message) in str(exc_info.value)


@pytest.mark.unit
def test_empty_dataframe_passes_validation(make_io, valid_df):
    make_io().validate_from_schema(valid_df.iloc[0:0])


@pytest.mark.unit
def test_validating_a_dataframe_missing_a_schema_column_raises_key_error(make_io, valid_df):
    with pytest.raises(KeyError):
        make_io().validate_from_schema(valid_df.drop(columns=["b"]))


class TestWhenValidationsRun:
    @pytest.mark.unit
    def test_read_validates_the_cast_frame_and_raises_with_all_violations(self, make_io, invalid_df):
        with pytest.raises(SchemaValidationError) as exc_info:
            make_io(invalid_df, apply_schema_validations=True).read()

        assert set(exc_info.value.message) == {"has_unique_values", "is_greater_than", "is_between", "has_no_null_values", "is_in", "has_acceptable_percentage_of_nulls"}

    @pytest.mark.unit
    def test_read_does_not_validate_by_default(self, make_io, invalid_df):
        assert len(make_io(invalid_df).read()) == 3

    @pytest.mark.unit
    def test_read_with_valid_data_returns_the_frame(self, make_io, valid_df):
        pd.testing.assert_frame_equal(make_io(valid_df, apply_schema_validations=True).read(), valid_df)

    @pytest.mark.unit
    def test_write_validates_before_anything_is_written(self, make_io, invalid_df):
        io = make_io(apply_schema_validations=True)

        with pytest.raises(SchemaValidationError):
            io.write(invalid_df)

        assert not os.path.exists(make_io.csv_file)

    @pytest.mark.unit
    def test_write_validates_only_schema_columns_and_drops_the_rest(self, make_io, valid_df):
        # an extra column that would violate nothing is dropped; the output holds schema columns only.
        make_io(apply_schema_validations=True).write(valid_df.assign(extra=[None, None, None]))

        assert pd.read_csv(make_io.csv_file, index_col=0).columns.to_list() == ["a", "b", "c"]
