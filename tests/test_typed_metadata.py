"""What `Field` writes into the `metadata` of a column for dynamicio (`Metric` members and validations), and how the compiler reads it back."""

# pylint: disable=missing-class-docstring, missing-function-docstring
import pandas as pd
import pandera.pandas as pa
import pytest
from pandera.typing import Series

from dynamicio.schema import Field, Metric, to_dataframe_schema
from dynamicio.schema.metadata import METADATA_KEY, DynamicioMetadata
from dynamicio.schema.metrics import get_metric
from dynamicio.schema.validations import ALL_VALIDATORS


def _column(field, dtype=float):
    """The compiled column ``x`` of a plain Pandera model declared with ``field``: `Field` works in any ``DataFrameModel``."""

    class Model(pa.DataFrameModel):
        __dataset_name__ = "model"

        x: Series[dtype] = field

    return to_dataframe_schema(Model).columns["x"]


def _validators(field, dtype=float):
    return {v.name: v for v in _column(field, dtype).validations}


class TestMetric:
    @pytest.mark.unit
    def test_every_member_names_a_registered_metric(self):
        for member in Metric:
            assert get_metric(member.value) is not None

    @pytest.mark.unit
    def test_members_carry_the_names_the_log_reports_and_are_not_strings(self):
        assert Metric.UNIQUE_COUNTS.value == "UniqueCounts" and not isinstance(Metric.MIN, str)  # a str mixin would put every str method in the editor's menu


class TestMetrics:
    @pytest.mark.unit
    def test_metrics_compile_to_their_log_names_in_declaration_order(self):
        assert _column(Field(nullable=True, metrics=[Metric.MAX, Metric.MIN])).metrics == ["Max", "Min"]

    @pytest.mark.unit
    def test_duplicates_are_dropped(self):
        assert _column(Field(nullable=True, metrics=[Metric.MIN, Metric.MIN, Metric.MAX])).metrics == ["Min", "Max"]

    @pytest.mark.unit
    @pytest.mark.parametrize("bad", ["Min", "Nope", 1, None])
    def test_anything_but_a_metric_member_is_a_type_error(self, bad):
        with pytest.raises(TypeError, match="dynamicio.schema.Metric"):
            Field(nullable=True, metrics=[bad])

    @pytest.mark.unit
    def test_the_metadata_is_a_dict_holding_the_typed_declaration(self):
        metadata = Field(nullable=True, metrics=[Metric.MIN]).metadata

        assert metadata == {METADATA_KEY: DynamicioMetadata(metrics=(Metric.MIN,))}

    @pytest.mark.unit
    def test_a_column_that_declares_nothing_for_dynamicio_has_no_metadata(self):
        assert Field(nullable=True).metadata is None


class TestRawDictsAreRefused:
    @pytest.mark.unit
    @pytest.mark.parametrize("raw", [{"dynamicio": {"metrics": ["Min"]}}, {"dynamicio": {"validations": {}}}, {"dynamicio": "x"}])
    def test_a_raw_dynamicio_metadata_dict_is_a_value_error(self, raw):
        with pytest.raises(ValueError, match="dynamicio.schema.Field"):
            _column(pa.Field(nullable=True, metadata=raw))

    @pytest.mark.unit
    def test_metadata_of_other_tools_passes_through(self):
        assert _column(pa.Field(nullable=True, metadata={"owner": "team"})).metrics == []


class TestMaxNullFraction:
    @pytest.mark.unit
    @pytest.mark.parametrize("bad", [0, 1, -0.1, 1.5])
    def test_bounds_are_exclusive(self, bad):
        with pytest.raises(ValueError, match="between 0 and 1"):
            Field(nullable=True, max_null_fraction=bad)

    @pytest.mark.unit
    def test_an_unresolved_dynamic_variable_is_kept_as_any_other_bound_is(self):
        assert _validators(Field(nullable=True, max_null_fraction="[[ NULL_LIMIT ]]"))["has_acceptable_percentage_of_nulls"].options == {"threshold": "[[ NULL_LIMIT ]]"}

    @pytest.mark.unit
    @pytest.mark.parametrize("bad", ["0.2", True, [0.2]])
    def test_anything_else_but_a_number_is_a_type_error(self, bad):
        with pytest.raises(TypeError, match="a number between 0 and 1"):
            Field(nullable=True, max_null_fraction=bad)

    @pytest.mark.unit
    def test_it_compiles_to_the_threshold_validator(self):
        assert _validators(Field(nullable=True, max_null_fraction=0.2))["has_acceptable_percentage_of_nulls"].options == {"threshold": 0.2}

    @pytest.mark.unit
    def test_the_validator_runs_on_the_fraction(self):
        options = _validators(Field(nullable=True, max_null_fraction=0.5))["has_acceptable_percentage_of_nulls"].options
        check = ALL_VALIDATORS["has_acceptable_percentage_of_nulls"]

        assert check("D", pd.DataFrame({"x": [1.0, None, 3.0, 4.0]}), "x", **options).valid
        assert not check("D", pd.DataFrame({"x": [1.0, None, None, 4.0]}), "x", **options).valid


class TestExactly:
    @pytest.mark.unit
    def test_it_compiles_to_is_in_requiring_every_value(self):
        options = _validators(Field(nullable=True, exactly=["a", "b", "a"]), dtype=str)["is_in"].options

        assert options == {"categorical_values": {"a", "b"}, "exact": True}

    @pytest.mark.unit
    def test_values_must_all_be_present_and_no_other_allowed(self):
        options = _validators(Field(nullable=True, exactly=["a", "b"]), dtype=str)["is_in"].options
        check = ALL_VALIDATORS["is_in"]

        assert check("D", pd.DataFrame({"x": ["a", "b", "a"]}), "x", **options).valid
        assert not check("D", pd.DataFrame({"x": ["a", "a"]}), "x", **options).valid  # "b" missing: `isin` alone would accept it
        assert not check("D", pd.DataFrame({"x": ["a", "b", "c"]}), "x", **options).valid

    @pytest.mark.unit
    def test_it_takes_any_iterable(self):
        assert _validators(Field(nullable=True, exactly=iter("ab")), dtype=str)["is_in"].options["categorical_values"] == {"a", "b"}


class TestMetadataValueObjects:
    @pytest.mark.unit
    def test_payloads_are_immutable(self):
        payload = DynamicioMetadata(metrics=(Metric.MIN,))

        with pytest.raises(AttributeError):
            payload.metrics = ()  # type: ignore[misc]
