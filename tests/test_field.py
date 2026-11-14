"""`Field` declares a column with dynamicio's metrics and validations as named arguments."""

# pylint: disable=missing-class-docstring, missing-function-docstring
import pandas as pd
import pytest
from pandera.typing import Series

from dynamicio.schema import DataModel, Field, Metric, to_dataframe_schema
from dynamicio.schema.validations import ALL_VALIDATORS


def compiled(field, dtype=float):
    """The compiled column ``x`` of a model declared with ``field``."""

    class Model(DataModel, dataset="model"):
        x: Series[dtype] = field

    return to_dataframe_schema(Model).columns["x"]


def validations(column):
    return {v.name: v.options for v in column.validations}


class TestField:
    @pytest.mark.unit
    def test_nullable_has_to_be_decided(self):
        with pytest.raises(TypeError, match="nullable"):
            Field()  # pylint: disable=no-value-for-parameter,missing-kwoa

    @pytest.mark.unit
    def test_nullable_false_adds_the_not_null_validation_and_true_does_not(self):
        assert "has_no_null_values" in validations(compiled(Field(nullable=False)))
        assert validations(compiled(Field(nullable=True))) == {}

    @pytest.mark.unit
    def test_comparisons_unique_and_isin_map_to_their_validations(self):
        column = compiled(Field(nullable=True, unique=True, gt=0, le=10, isin=[1.0, 2.0]))

        assert set(validations(column)) == {"has_unique_values", "is_greater_than", "is_lower_than_or_equal", "is_in"}
        assert validations(column)["is_greater_than"] == {"threshold": 0}  # a bound of 0 is a bound

    @pytest.mark.unit
    def test_a_range_is_a_lower_and_an_upper_bound(self):
        assert validations(compiled(Field(nullable=True, ge=0, lt=5))) == {"is_greater_than_or_equal": {"threshold": 0}, "is_lower_than": {"threshold": 5}}

    @pytest.mark.unit
    def test_bounds_are_not_compared_on_definition(self):
        Field(nullable=True, ge="[[ LATER ]]", le=5)  # a pa.Field(in_range=...) would raise on a string bound

    @pytest.mark.unit
    @pytest.mark.parametrize("removed", ["between", "include_lower", "include_upper"])
    def test_between_is_gone(self, removed):
        with pytest.raises(TypeError, match=removed):
            Field(nullable=True, **{removed: (0, 1)})

    @pytest.mark.unit
    def test_exactly_and_max_null_fraction_and_metrics(self):
        column = compiled(Field(nullable=True, exactly=["a", "b"], max_null_fraction=0.2, metrics=[Metric.COUNTS_PER_LABEL, Metric.MIN]), dtype=str)

        assert validations(column)["is_in"]["exact"] is True
        assert validations(column)["has_acceptable_percentage_of_nulls"] == {"threshold": 0.2}
        assert column.metrics == ["CountsPerLabel", "Min"]

    @pytest.mark.unit
    def test_the_validations_are_the_ones_dynamicio_runs(self):
        column = compiled(Field(nullable=False, ge=1, max_null_fraction=0.5))
        frame = pd.DataFrame({"x": [0.0, 2.0]})

        results = {v.name: ALL_VALIDATORS[v.name]("model", frame, "x", **v.options).valid for v in column.validations}

        assert results == {"has_no_null_values": True, "is_greater_than_or_equal": False, "has_acceptable_percentage_of_nulls": True}

    @pytest.mark.unit
    def test_other_pandera_options_pass_through(self):
        class Model(DataModel, dataset="model"):
            name: Series[str] = Field(nullable=True, str_startswith="ab", alias="the name")

        assert "the name" in Model.to_schema().columns

    @pytest.mark.unit
    def test_metadata_and_isin_with_exactly_are_refused(self):
        with pytest.raises(TypeError, match="metadata"):
            Field(nullable=True, metadata={"x": 1})
        with pytest.raises(TypeError, match="isin= .* or exactly="):
            Field(nullable=True, isin=[1], exactly=[1])

    @pytest.mark.unit
    def test_a_metric_that_is_not_a_member_is_refused(self):
        with pytest.raises(TypeError, match="dynamicio.schema.Metric"):
            Field(nullable=True, metrics=["Min"])
