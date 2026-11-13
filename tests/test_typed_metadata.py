"""The typed `pa.Field(metadata=...)` helpers: `Metric`, `metrics`, `null_fraction_below`, `exactly_values`, `strictly_between`, and how they compile."""

# pylint: disable=missing-class-docstring, missing-function-docstring
import pandas as pd
import pandera.pandas as pa
import pytest
from pandera.typing import Series

from dynamicio.schema import Metric, exactly_values, metrics, null_fraction_below, strictly_between, to_dataframe_schema
from dynamicio.schema.metadata import ColumnMetadata, DynamicioMetadata
from dynamicio.schema.metrics import get_metric
from dynamicio.schema.validations import ALL_VALIDATORS


def _validators(metadata, column="x"):
    """The compiled validations of one column declared with ``metadata``, by validator name."""

    class Model(pa.DataFrameModel):
        x: Series[float] = pa.Field(nullable=True, metadata=metadata)

    return {v.name: v for v in to_dataframe_schema(Model).columns[column].validations}


class TestMetric:
    @pytest.mark.unit
    def test_every_member_names_a_registered_metric(self):
        for member in Metric:
            assert get_metric(member.value) is not None

    @pytest.mark.unit
    def test_members_are_strings_equal_to_the_names_the_log_reports(self):
        assert Metric.UNIQUE_COUNTS == "UniqueCounts" and isinstance(Metric.MIN, str)


class TestMetrics:
    @pytest.mark.unit
    def test_metrics_compile_to_their_log_names_in_declaration_order(self):
        class Model(pa.DataFrameModel):
            x: Series[float] = pa.Field(nullable=True, metadata=metrics(Metric.MAX, Metric.MIN))

        assert to_dataframe_schema(Model).columns["x"].metrics == ["Max", "Min"]

    @pytest.mark.unit
    def test_duplicates_are_dropped(self):
        assert metrics(Metric.MIN, Metric.MIN, Metric.MAX).payload.metrics == (Metric.MIN, Metric.MAX)

    @pytest.mark.unit
    @pytest.mark.parametrize("bad", ["Min", "Nope", 1, None])
    def test_anything_but_a_metric_member_is_a_type_error(self, bad):
        with pytest.raises(TypeError, match="dynamicio.schema.Metric"):
            metrics(bad)

    @pytest.mark.unit
    def test_the_metadata_is_a_dict_as_pandera_requires(self):
        assert isinstance(metrics(Metric.MIN), dict) and isinstance(metrics(Metric.MIN), ColumnMetadata)


class TestCombining:
    @pytest.mark.unit
    def test_or_merges_metrics_and_validations(self):
        combined = metrics(Metric.MIN) | null_fraction_below(0.2) | metrics(Metric.MAX)

        assert combined.payload.metrics == (Metric.MIN, Metric.MAX)
        assert [spec.name for spec in combined.payload.validations] == ["has_acceptable_percentage_of_nulls"]

    @pytest.mark.unit
    def test_or_drops_repeated_metrics(self):
        assert (metrics(Metric.MIN, Metric.MAX) | metrics(Metric.MAX, Metric.MEAN)).payload.metrics == (Metric.MIN, Metric.MAX, Metric.MEAN)

    @pytest.mark.unit
    def test_or_does_not_modify_its_operands(self):
        left, right = metrics(Metric.MIN), metrics(Metric.MAX)

        _ = left | right

        assert left.payload.metrics == (Metric.MIN,) and right.payload.metrics == (Metric.MAX,)

    @pytest.mark.unit
    def test_or_with_a_plain_dict_ends_in_the_raw_dict_error_at_compile_time(self):
        mixed = metrics(Metric.MIN) | {"dynamicio": {"metrics": ["Max"]}}  # dict's own `|` wins: a plain dict, which compilation refuses

        class Model(pa.DataFrameModel):
            x: Series[float] = pa.Field(nullable=True, metadata=mixed)

        with pytest.raises(ValueError, match="metadata=metrics"):
            to_dataframe_schema(Model)

    @pytest.mark.unit
    def test_all_helpers_combine_on_one_column(self):
        found = _validators(metrics(Metric.MIN) | null_fraction_below(0.5) | exactly_values(["a"]) | strictly_between(0, 1))

        assert {"has_acceptable_percentage_of_nulls", "is_in", "is_between"} <= set(found)


class TestRawDictsAreRefused:
    @pytest.mark.unit
    @pytest.mark.parametrize("raw", [{"dynamicio": {"metrics": ["Min"]}}, {"dynamicio": {"validations": {}}}, {"dynamicio": "x"}])
    def test_a_raw_dynamicio_metadata_dict_is_a_value_error(self, raw):
        class Model(pa.DataFrameModel):
            x: Series[float] = pa.Field(nullable=True, metadata=raw)

        with pytest.raises(ValueError, match="metadata=metrics"):
            to_dataframe_schema(Model)

    @pytest.mark.unit
    def test_metadata_of_other_tools_passes_through(self):
        class Model(pa.DataFrameModel):
            x: Series[float] = pa.Field(nullable=True, metadata={"owner": "team"})

        assert to_dataframe_schema(Model).columns["x"].metrics == []


class TestNullFractionBelow:
    @pytest.mark.unit
    @pytest.mark.parametrize("bad", [0, 1, -0.1, 1.5])
    def test_bounds_are_exclusive(self, bad):
        with pytest.raises(ValueError, match="between 0 and 1"):
            null_fraction_below(bad)

    @pytest.mark.unit
    def test_it_compiles_to_the_threshold_validator(self):
        assert _validators(null_fraction_below(0.2))["has_acceptable_percentage_of_nulls"].options == {"threshold": 0.2}

    @pytest.mark.unit
    def test_the_validator_runs_on_the_fraction(self):
        options = _validators(null_fraction_below(0.5))["has_acceptable_percentage_of_nulls"].options
        check = ALL_VALIDATORS["has_acceptable_percentage_of_nulls"]

        assert check("D", pd.DataFrame({"x": [1.0, None, 3.0, 4.0]}), "x", **options).valid
        assert not check("D", pd.DataFrame({"x": [1.0, None, None, 4.0]}), "x", **options).valid


class TestExactlyValues:
    @pytest.mark.unit
    def test_it_compiles_to_is_in_requiring_every_value(self):
        options = _validators(exactly_values(["a", "b", "a"]))["is_in"].options

        assert options == {"categorical_values": {"a", "b"}, "match_all": False}

    @pytest.mark.unit
    def test_values_must_all_be_present_and_no_other_allowed(self):
        options = _validators(exactly_values(["a", "b"]))["is_in"].options
        check = ALL_VALIDATORS["is_in"]

        assert check("D", pd.DataFrame({"x": ["a", "b", "a"]}), "x", **options).valid
        assert not check("D", pd.DataFrame({"x": ["a", "a"]}), "x", **options).valid  # "b" missing: `isin` alone would accept it
        assert not check("D", pd.DataFrame({"x": ["a", "b", "c"]}), "x", **options).valid

    @pytest.mark.unit
    def test_it_takes_any_iterable(self):
        assert exactly_values(iter("ab")).payload.validations[0].options["categorical_values"] == {"a", "b"}


class TestStrictlyBetween:
    @pytest.mark.unit
    def test_bounds_are_excluded_unless_included(self):
        assert _validators(strictly_between(0, 1))["is_between"].options == {"lower": 0, "upper": 1, "include_left": False, "include_right": False}
        assert _validators(strictly_between(0, 1, include_lower=True, include_upper=True))["is_between"].options["include_left"] is True

    @pytest.mark.unit
    def test_the_validator_honours_the_inclusion_flags(self):
        check = ALL_VALIDATORS["is_between"]
        frame = pd.DataFrame({"x": [0.0, 0.5, 1.0]})

        assert not check("D", frame, "x", **_validators(strictly_between(0, 1))["is_between"].options).valid
        assert check("D", frame, "x", **_validators(strictly_between(0, 1, include_lower=True, include_upper=True))["is_between"].options).valid

    @pytest.mark.unit
    def test_bounds_are_not_compared_on_definition(self):
        """Settings values may be strings placeholders or resolved later: `pa.Field(in_range=...)` would raise here, the helper does not."""
        assert strictly_between("[[ LOWER ]]", "[[ UPPER ]]").payload.validations[0].options["lower"] == "[[ LOWER ]]"


class TestMetadataValueObjects:
    @pytest.mark.unit
    def test_payloads_are_immutable(self):
        payload = DynamicioMetadata(metrics=(Metric.MIN,))

        with pytest.raises(AttributeError):
            payload.metrics = ()  # type: ignore[misc]
