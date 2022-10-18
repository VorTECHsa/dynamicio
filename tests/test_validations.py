# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, too-many-public-methods
import pytest

from dynamicio.validations import (
    has_acceptable_percentage_of_nulls,
    has_no_null_values,
    has_unique_values,
    is_between,
    is_greater_than,
    is_greater_than_or_equal,
    is_in,
    is_lower_than,
    is_lower_than_or_equal,
)


class TestHasUniqueValues:
    @pytest.mark.unit
    def test_returns_true_if_column_has_no_duplicate_values(self, input_df):
        # Given
        df = input_df

        # When
        validation = has_unique_values("TEST", df, column="id")

        # Then
        assert validation.valid is True and validation.value == 0 and validation.message == "TEST[id] has unique values"

    @pytest.mark.unit
    def test_returns_false_if_column_has_duplicate_values(self, input_df):
        # Given
        df = input_df

        # When
        validation = has_unique_values("TEST", df, column="activity")

        # Then
        assert not validation.valid and validation.value == 3 and validation.message == "Values ['discharge', 'pass_through', 'load'] for TEST[activity] are duplicated!"


class TestHasNoNullValues:
    @pytest.mark.unit
    def test_returns_true_if_column_in_df_has_no_nulls(self, input_df):
        # Given
        df = input_df

        # When
        validation = has_no_null_values("TEST", df, column="activity")

        # Then
        assert validation.valid is True and validation.value == 0 and validation.message == "TEST[activity] has 0 nulls"

    @pytest.mark.unit
    def test_returns_false_if_column_in_df_has_none_values(self, input_df):
        # Given
        df = input_df

        # When
        validation = has_no_null_values("TEST", df, column="duration_a")

        # Then
        assert not validation.valid and validation.value == 1 and validation.message == "TEST[duration_a] has 1 nulls"

    @pytest.mark.unit
    def test_returns_false_if_column_in_df_has_nat_values(self, input_df):
        # Given
        df = input_df

        # When
        validation = has_no_null_values("TEST", df, column="start_time")

        # Then
        assert not validation.valid and validation.value == 1 and validation.message == "TEST[start_time] has 1 nulls"


class TestHasAcceptablePercentageOfNulls:
    @pytest.mark.unit
    def test_throws_exception_if_threshold_is_greater_than_1(self, input_df):
        # Given
        df = input_df

        # When/Then
        with pytest.raises(ValueError):
            has_acceptable_percentage_of_nulls("TEST", df, column="duration_a", threshold=1.2)

    @pytest.mark.unit
    def test_throws_exception_if_threshold_is_lower_than_0(self, input_df):
        # Given
        df = input_df

        # When/Then
        with pytest.raises(ValueError):
            has_acceptable_percentage_of_nulls("TEST", df, column="duration_a", threshold=-0.1)

    @pytest.mark.unit
    def test_returns_true_if_percentage_threshold_is_not_exceeded(self, input_df):
        # Given
        df = input_df

        # When
        validation = has_acceptable_percentage_of_nulls("TEST", df, column="duration_a", threshold=0.11)

        # Then
        assert validation.valid is True and validation.value == 0.1 and validation.message == "Percentage of nulls of for TEST[duration_a] is 0.1"

    @pytest.mark.unit
    def test_returns_true_if_inpu_df_is_empty(self, empty_df):
        # Given
        df = empty_df

        # When
        validation = has_acceptable_percentage_of_nulls("TEST", df, column="duration_a", threshold=0.11)

        # Then
        assert validation.valid is True and validation.value == 0 and validation.message == "Percentage of nulls of for TEST[duration_a] is 0"

    @pytest.mark.unit
    def test_returns_true_if_threshold_is_not_exceeded_for_any_null_type_value(self, input_df):
        # Given
        df = input_df

        # When
        validation = has_acceptable_percentage_of_nulls("TEST", df, column="duration_b", threshold=0.2)

        # Then
        assert not validation.valid and validation.value == 0.3 and validation.message == "Percentage of nulls of for TEST[duration_b] is 0.3 which exceeds threshold: 0.2"

    @pytest.mark.unit
    def test_returns_false_if_threshold_is_exceeded(self, input_df):
        # Given
        df = input_df

        # When
        validation = has_acceptable_percentage_of_nulls("TEST", df, column="duration_a", threshold=0.09)

        # Then
        assert not validation.valid and validation.value == 0.1 and validation.message == "Percentage of nulls of for TEST[duration_a] is 0.1 which exceeds threshold: 0.09"


class TestHasAcceptableCategoricalValues:
    @pytest.mark.unit
    def test_returns_true_if_columns_unique_values_are_a_subset_of_input_set(self, input_df):
        # Given
        df = input_df

        # When
        validation = is_in(
            "TEST",
            df,
            column="activity",
            categorical_values={"load", "discharge", "pass_through", "one_more"},
        )

        # Then
        assert validation.valid is True and validation.value == 0 and validation.message == "Categorical values for TEST[activity] are acceptable"

    @pytest.mark.unit
    def test_returns_true_only_if_columns_unique_vals_are_an_exact_match_of_the_input_set_when_match_all_is_set_to_false(self, input_df):
        # Given
        df = input_df

        # When
        validation = is_in("TEST", df, column="activity", categorical_values={"load", "discharge", "pass_through"}, match_all=False)

        # Then
        assert validation.valid is True and validation.value == 0 and validation.message == "All acceptable categorical values for TEST[activity] are present"

    @pytest.mark.unit
    def test_returns_false_if_columns_unique_vals_are_less_than_the_acceptable_categoricals_when_match_all_is_set_to_false(self, input_df):
        # Given
        df = input_df

        # When
        validation = is_in("TEST", df, column="activity", categorical_values={"load", "discharge", "pass_through", "one_more"}, match_all=False)

        # Then
        assert validation.valid is False and validation.value == 1 and validation.message == "Missing categorical values for TEST[activity]: {'one_more'}"

    @pytest.mark.unit
    def test_returns_false_if_columns_unique_vals_are_more_than_the_acceptable_categoricals_when_match_all_is_set_to_false(self, input_df):
        # Given
        df = input_df

        # When
        validation = is_in("TEST", df, column="activity", categorical_values={"load", "discharge"}, match_all=False)

        # Then
        assert validation.valid is False and validation.value == 3 and validation.message == "Values {'pass_through'} for TEST[activity] are not acceptable for 3 cells"

    @pytest.mark.unit
    def test_returns_true_if_columns_unique_vals_are_an_exact_match_of_the_input_set(self, input_df):
        # Given
        df = input_df

        # When/Then
        validation = is_in("TEST", df, column="activity", categorical_values={"load", "discharge", "pass_through"})

        # Then
        assert validation.valid is True and validation.value == 0 and validation.message == "Categorical values for TEST[activity] are acceptable"

    @pytest.mark.unit
    def test_returns_false_if_columns_unique_values_are_not_a_subset_of_input_set(self, input_df):
        # Given
        df = input_df

        # When/Then
        validation = is_in("TEST", df, column="activity", categorical_values={"load", "pass_through"})

        # Then
        assert not validation.valid and validation.value == 5 and validation.message == "Values {'discharge'} for TEST[activity] are not acceptable for 5 cells"

    @pytest.mark.unit
    def test_returns_true_if_nulls_are_an_allowed_categorical_value(self, input_df):
        # Given
        df = input_df

        # When
        validation = is_in("TEST", df, column="category_a", categorical_values={"A", "B", "C", None})

        # Then
        assert validation.valid is True and validation.value == 0 and validation.message == "Categorical values for TEST[category_a] are acceptable"

    @pytest.mark.unit
    def test_ignores_the_existence_of_null_values(self, input_df):
        # Given
        df = input_df

        # When
        validation = is_in("TEST", df, column="category_a", categorical_values={"A", "B", "C"})

        # Then
        assert validation.valid is True and validation.value == 0 and validation.message == "Categorical values for TEST[category_a] are acceptable"

    @pytest.mark.unit
    def test_treats_nan_and_na_values_as_nulls_and_returns_true_if_null_is_acceptable(self, input_df):
        # Given
        df = input_df  # where category_b has None, pd.NA and np.nan values

        # When
        validation = is_in("TEST", df, column="category_b", categorical_values={"A", "B", "C", None})

        # Then
        assert validation.valid is True and validation.value == 0 and validation.message == "Categorical values for TEST[category_b] are acceptable"

    @pytest.mark.unit
    def test_ignores_nan_and_na_values_as_it_does_with_nulls(self, input_df):
        # Given
        df = input_df  # where category_b has None, pd.NA and np.nan values

        # When
        validation = is_in("TEST", df, column="category_b", categorical_values={"A", "B", "C"})

        # Then
        assert validation.valid is True and validation.value == 0 and validation.message == "Categorical values for TEST[category_b] are acceptable"


class TestIsGreaterThan:
    @pytest.mark.unit
    def test_returns_true_if_all_column_values_are_above_threshold(self, input_df):
        # Given
        df = input_df

        # When
        validation = is_greater_than("TEST", df, column="weight_a", threshold=4)

        # Then
        assert validation.valid is True and validation.value == 0 and validation.message == "All values of TEST[weight_a] are above 4"

    @pytest.mark.unit
    def test_returns_false_if_any_column_values_are_below_threshold(self, input_df):
        # Given
        df = input_df

        # When
        validation = is_greater_than("TEST", df, column="weight_a", threshold=6)

        # Then
        assert not validation.valid and validation.value == 0.5 and validation.message == "5 cell values for TEST[weight_a] are below 6"

    @pytest.mark.unit
    def test_returns_false_if_any_column_values_are_below_or_equal_to_threshold(self, input_df):
        # Given
        df = input_df

        # When/Then
        validation = is_greater_than("TEST", df, column="weight_a", threshold=5)

        # Then
        assert not validation.valid and validation.value == 0.3 and validation.message == "3 cell values for TEST[weight_a] are below 5"

    @pytest.mark.unit
    def test_is_greater_than_returns_true_if_all_column_values_are_below_threshold_irrespective_of_nulls(self, input_df):
        # Given
        df = input_df

        # When
        validation = is_greater_than("TEST", df, column="weight_b", threshold=4)

        # Then
        assert validation.valid is True and validation.value == 0 and validation.message == "All values of TEST[weight_b] are above 4"


class TestIsGreaterThanOrEqual:
    @pytest.mark.unit
    def test_returns_true_if_all_column_values_are_above_or_equal_to_threshold(self, input_df):
        # Given
        df = input_df

        # When/Then
        validation = is_greater_than_or_equal("TEST", df, column="weight_a", threshold=5)

        # Then
        assert validation.valid is True and validation.value == 0 and validation.message == "All values of TEST[weight_a] are above 5"

    def test_returns_false_if_any_column_values_are_below_the_threshold(self, input_df):
        # Given
        df = input_df

        # When/Then
        validation = is_greater_than_or_equal("TEST", df, column="weight_a", threshold=6)

        # Then
        assert validation.valid is False and validation.value == 0.3 and validation.message == "3 cell values for TEST[weight_a] are below 6"


class TestIsLowerThan:
    @pytest.mark.unit
    def test_returns_true_if_all_column_values_are_below_threshold(self, input_df):
        # Given
        df = input_df

        # When
        validation = is_lower_than("TEST", df, column="weight_a", threshold=10)

        # Then
        assert validation.valid is True and validation.value == 0 and validation.message == "All values of TEST[weight_a] are below 10"

    @pytest.mark.unit
    def test_returns_false_if_any_column_values_are_above_threshold(self, input_df):
        # Given
        df = input_df

        # When/Then
        validation = is_lower_than("TEST", df, column="weight_a", threshold=8)

        # Then
        assert not validation.valid and validation.value == 0.3 and validation.message == "3 cell values for TEST[weight_a] are above 8"

    @pytest.mark.unit
    def test_is_lower_than_returns_false_if_any_column_values_are_below_or_equal_to_threshold(self, input_df):
        # Given
        df = input_df

        # When/Then
        validation = is_lower_than("TEST", df, column="weight_a", threshold=9)

        # Then
        assert not validation.valid and validation.value == 0.1 and validation.message == "1 cell values for TEST[weight_a] are above 9"

    @pytest.mark.unit
    def test_is_lower_than_returns_true_if_all_columns_values_are_below_threshold_irrespective_of_nulls(self, input_df):
        # Given
        df = input_df

        # When/Then
        validation = is_lower_than("TEST", df, column="weight_b", threshold=10)

        # Then
        assert validation.valid is True and validation.value == 0 and validation.message == "All values of TEST[weight_b] are below 10"


class TestIsLowerThanOrEqual:
    @pytest.mark.unit
    def test_returns_true_if_all_column_values_are_below_or_equal_to_threshold(self, input_df):
        # Given
        df = input_df

        # When/Then
        validation = is_lower_than_or_equal("TEST", df, column="weight_a", threshold=9)

        # Then
        assert validation.valid is True and validation.value == 0 and validation.message == "All values of TEST[weight_a] are below 9"

    @pytest.mark.unit
    def test_returns_false_if_any_column_values_are_above_the_threshold(self, input_df):
        # Given
        df = input_df

        # When/Then
        validation = is_lower_than_or_equal("TEST", df, column="weight_a", threshold=8)

        # Then
        assert not validation.valid and validation.value == 0.1 and validation.message == "1 cell values for TEST[weight_a] are above 8"


class TestIsBetween:
    @pytest.mark.integration
    def test_returns_true_if_all_column_values_are_between_upper_and_lower_bounds(self, input_df):
        # Given
        df = input_df

        # When
        validation = is_between("TEST", df, column="weight_a", lower=4, upper=10)

        # Then
        assert validation.valid is True and validation.value == 0 and validation.message == "All values of TEST[weight_a] is between 4 and 10 thresholds"

    @pytest.mark.integration
    def test_returns_false_if_any_column_values_are_below_the_lower_bound(self, input_df):
        # Given
        df = input_df

        # When
        validation = is_between("TEST", df, column="weight_a", lower=6, upper=10)

        # Then
        assert not validation.valid and validation.value == 0.5 and validation.message == "5 cell values for TEST[weight_a] are either below 6 or above 10"

    @pytest.mark.integration
    def test_returns_false_if_any_column_values_are_above_the_upper_bound(self, input_df):
        # Given
        df = input_df

        # When
        validation = is_between("TEST", df, column="weight_a", lower=4, upper=8)

        # Then
        assert not validation.valid and validation.value == 0.3 and validation.message == "3 cell values for TEST[weight_a] are either below 4 or above 8"

    @pytest.mark.integration
    def test_returns_true_if_all_column_values_are_within_bounds_bounds_included(self, input_df):
        # Given
        df = input_df

        # When
        validation = is_between("TEST", df, column="weight_a", lower=5, upper=9, include_left=True, include_right=True)

        # Then
        assert validation.valid is True and validation.value == 0 and validation.message == "All values of TEST[weight_a] is between 5 and 9 thresholds"

    @pytest.mark.integration
    def test_returns_true_if_all_column_values_are_between_upper_and_lower_bounds_irrespective_of_nulls(self, input_df):
        # Given
        df = input_df

        # When
        validation = is_between("TEST", df, column="weight_b", lower=4, upper=10)

        # Then
        assert validation.valid is True and validation.value == 0 and validation.message == "All values of TEST[weight_b] is between 4 and 10 thresholds"
