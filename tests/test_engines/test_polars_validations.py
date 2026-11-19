# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import math
import random
import warnings
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import pytest

from dynamicio.schema import validations

pl = pytest.importorskip("polars")
PolarsValidations = pytest.importorskip("dynamicio.schema.polars_validations").PolarsValidations

NAN = float("nan")

# Each data set is the column `x` as the pandas engine and the polars engine hold it after their casts: pandas has NaN, polars has null (the null rule)
DATA = {
    "floats": ([1.0, NAN, None, 2.0, 2.0, 5.5, -1.0], "float64", pl.Float64),
    "ints": ([3, 1, 1, 3, 2, 2, 7], "int64", pl.Int64),
    "strings": (["a", "b", None, "a", "c", "b", "a"], "object", pl.String),
    "categories": (["a", "b", None, "a", "c", "b", "a"], "category", pl.Categorical),
    "bools": ([True, False, True, True], "bool", pl.Boolean),
    "empty": ([], "float64", pl.Float64),
    "all_null": ([NAN, None, NAN], "float64", pl.Float64),
    "datetimes": ([datetime(2020, 1, 1), datetime(2020, 1, 1), datetime(2021, 6, 1), None], "datetime64[ns]", pl.Datetime("ns")),
    "durations": ([timedelta(hours=1), timedelta(hours=1), timedelta(0)], "timedelta64[ns]", pl.Duration("ns")),
}
NUMERIC = ("floats", "ints", "empty", "all_null")


class TestValidatorParity:
    """Each polars validator gives the result of the pandas validator of the same name, on the same data."""

    @staticmethod
    def frames(name):
        values, pandas_dtype, polars_dtype = DATA[name]
        pandas_frame = pd.DataFrame({"x": pd.Series(values, dtype=pandas_dtype)})
        polars_frame = pl.DataFrame({"x": pl.Series(values, dtype=polars_dtype)})
        if polars_dtype == pl.Float64:
            polars_frame = polars_frame.with_columns(pl.col("x").fill_nan(None))
        return pandas_frame, polars_frame.lazy()

    @staticmethod
    def run(pending):
        return pending.finish(pl.collect_all(list(pending.queries)) if pending.queries else [])

    @classmethod
    def assert_same(cls, data, validator, **options):
        # Given
        pandas_frame, polars_frame = cls.frames(data)

        # When
        expected = getattr(validations, validator)("d", pandas_frame, "x", **options)
        actual = cls.run(PolarsValidations.ALL[validator]("d", polars_frame, "x", **options))

        # Then
        assert (actual.valid, actual.message) == (bool(expected.valid), expected.message)
        assert float(actual.value) == pytest.approx(float(expected.value), nan_ok=True)

    @pytest.mark.unit
    @pytest.mark.parametrize("data", list(DATA))
    def test_has_unique_values_is_the_pandas_result(self, data):
        self.assert_same(data, "has_unique_values")

    @pytest.mark.unit
    @pytest.mark.parametrize("data", list(DATA))
    def test_has_no_null_values_is_the_pandas_result(self, data):
        self.assert_same(data, "has_no_null_values")

    @pytest.mark.unit
    @pytest.mark.parametrize("data", list(DATA))
    @pytest.mark.parametrize("threshold", [0.1, 0.3, 0.9])
    def test_has_acceptable_percentage_of_nulls_is_the_pandas_result(self, data, threshold):
        self.assert_same(data, "has_acceptable_percentage_of_nulls", threshold=threshold)

    @pytest.mark.unit
    @pytest.mark.parametrize("data", NUMERIC)
    @pytest.mark.parametrize("validator", ["is_greater_than", "is_greater_than_or_equal", "is_lower_than", "is_lower_than_or_equal"])
    @pytest.mark.parametrize("threshold", [-5, 1, 2, 10])
    def test_a_comparison_is_the_pandas_result(self, data, validator, threshold):
        self.assert_same(data, validator, threshold=threshold)

    @pytest.mark.unit
    @pytest.mark.parametrize("data", NUMERIC)
    @pytest.mark.parametrize("include_left", [True, False])
    @pytest.mark.parametrize("include_right", [True, False])
    @pytest.mark.parametrize("bounds", [(1, 3), (-1, 5.5), (-10, 10)])
    def test_is_between_is_the_pandas_result(self, data, include_left, include_right, bounds):
        self.assert_same(data, "is_between", lower=bounds[0], upper=bounds[1], include_left=include_left, include_right=include_right)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "data, values",
        [
            ("strings", ["a", "b", "c"]),
            ("strings", ["a"]),
            ("strings", ["a", "b", "c", "d"]),
            ("categories", ["a", "b"]),
            ("categories", ["a", "b", "c"]),
            ("ints", [1, 2, 3]),
            ("ints", [1, 2, 3, 7]),
            ("floats", [1.0, 2.0]),
            ("empty", [1.0]),
            ("all_null", [1.0]),
            ("strings", ["a", "b", None]),
            ("strings", ["a", "b", "c", NAN]),
            ("categories", ["a", "b", "c", None]),
            ("ints", [1, 2, 3, 7, None]),
            ("floats", [1.0, 2.0, 5.5, -1.0, NAN]),
            ("floats", [1.0, 2.0, 5.5, -1.0, None]),
            ("all_null", [NAN]),
            ("bools", [True, None]),
            ("datetimes", [datetime(2020, 1, 1), datetime(2021, 6, 1)]),
            ("datetimes", [datetime(2020, 1, 1), datetime(2021, 6, 1), None]),
            ("datetimes", [datetime(2020, 1, 1)]),
            ("durations", [timedelta(hours=1), timedelta(0)]),
            ("durations", [timedelta(hours=1)]),
            ("ints", [1, 2, 3, 7, "unused"]),
            ("ints", [1, 2, "3"]),
            ("ints", [1.0, 2.0, 3.0, 7.0]),
            ("ints", [1, 2, 3, 7.5]),
            ("floats", [1, 2, 5.5, -1, "x"]),
            ("strings", ["a", "b", "c", 1]),
            ("categories", ["a", "b", "c", 2]),
            ("bools", [True, False, "yes"]),
            ("datetimes", [datetime(2020, 1, 1), datetime(2021, 6, 1), "2020-01-01"]),
            ("durations", [timedelta(hours=1), timedelta(0), 3600]),
        ],
    )
    @pytest.mark.parametrize("exact", [False, True])
    def test_is_in_is_the_pandas_result(self, data, values, exact):
        self.assert_same(data, "is_in", categorical_values=values, exact=exact)

    @pytest.mark.unit
    def test_comparisons_on_seeded_random_floats_are_the_pandas_results(self):
        # Given 200 columns of random floats with NaN and None, the same on every run
        generator = random.Random(25354)
        for _ in range(200):
            values = [generator.choice([None, NAN, round(generator.uniform(-3, 3), 1)]) for _ in range(generator.randint(0, 12))]
            DATA["random"] = (values, "float64", pl.Float64)
            threshold = round(generator.uniform(-3, 3), 1)

            # Then
            for validator in ("is_greater_than", "is_greater_than_or_equal", "is_lower_than", "is_lower_than_or_equal"):
                self.assert_same("random", validator, threshold=threshold)
            self.assert_same("random", "is_between", lower=-threshold, upper=threshold + 1, include_left=True, include_right=False)
            self.assert_same("random", "has_unique_values")
            self.assert_same("random", "has_no_null_values")
            self.assert_same("random", "has_acceptable_percentage_of_nulls", threshold=0.25)
        del DATA["random"]


class TestValidatorEdgeCases:
    @staticmethod
    def run(pending):
        return pending.finish(pl.collect_all(list(pending.queries)) if pending.queries else [])

    @pytest.mark.unit
    @pytest.mark.parametrize("threshold", [0, 1, -0.1, 1.5])
    def test_a_null_threshold_outside_0_and_1_is_refused_when_the_validation_is_built(self, threshold):
        with pytest.raises(ValueError, match="must be a value between 0 and 1"):
            PolarsValidations.has_acceptable_percentage_of_nulls("d", pl.LazyFrame({"x": [1]}), "x", threshold=threshold)

    @pytest.mark.unit
    def test_a_nan_that_the_cast_did_not_change_is_a_value_and_not_a_null(self):
        # Given a frame that did not go through the cast of the engine
        frame = pl.LazyFrame({"x": [1.0, NAN]})

        # When
        result = self.run(PolarsValidations.has_no_null_values("d", frame, "x"))

        # Then the null rule of the cast is what makes NaN a null
        assert result.valid

    @pytest.mark.unit
    def test_duplicates_are_ordered_by_count_and_then_by_first_appearance(self):
        frame = pl.LazyFrame({"x": [3, 1, 1, 3, 2, 2, 2, 9]})

        result = self.run(PolarsValidations.has_unique_values("d", frame, "x"))

        assert result.message == "Values [2, 3, 1] for d[x] are duplicated!"
        assert result.value == 3

    @pytest.mark.unit
    def test_has_unique_values_takes_a_column_with_the_name_of_a_helper_column(self):
        for name in ("value", "row", "count", "_dynamicio_row", "_dynamicio_count"):
            result = self.run(PolarsValidations.has_unique_values("d", pl.LazyFrame({name: [1, 1, 2]}), name))

            assert (result.valid, result.message) == (False, f"Values [1] for d[{name}] are duplicated!")

    @pytest.mark.unit
    @pytest.mark.parametrize("name", ["len", "nulls", "rows"])
    def test_the_null_fraction_takes_a_column_with_the_name_of_an_aggregate(self, name):
        result = self.run(PolarsValidations.has_acceptable_percentage_of_nulls("d", pl.LazyFrame({name: [1, None]}), name, threshold=0.9))

        assert (result.valid, result.value) == (True, 0.5)

    @pytest.mark.unit
    def test_a_comparison_of_a_column_of_nulls_only_is_valid(self):
        frame = pl.LazyFrame({"x": pl.Series([None, None], dtype=pl.Float64)})

        result = self.run(PolarsValidations.is_greater_than("d", frame, "x", threshold=0))

        assert (result.valid, result.value) == (True, 0)

    @pytest.mark.unit
    def test_a_datetime_comparison_takes_a_datetime_threshold(self):
        frame = pl.LazyFrame({"x": [datetime(2020, 1, 1), datetime(2022, 1, 1)]})

        result = self.run(PolarsValidations.is_lower_than("d", frame, "x", threshold=datetime(2021, 1, 1)))

        assert result.message == "1 cell values for d[x] are above 2021-01-01 00:00:00"
        assert result.value == 0.5

    @pytest.mark.unit
    def test_a_consumer_validator_is_registered_under_its_name(self):
        # Given
        def is_even(dataset, frame, column):
            def finish(frames):
                odd = frames[0].item()
                return validations.ValidationResult(valid=odd == 0, message=f"{dataset}[{column}] has {odd} odd values", value=odd)

            return PolarsValidations.Pending((frame.select((pl.col(column) % 2 == 1).sum()),), finish)

        # When
        PolarsValidations.validator(is_even)
        try:
            result = self.run(PolarsValidations.ALL["is_even"]("d", pl.LazyFrame({"x": [1, 2, 3]}), "x"))

            # Then
            assert (result.valid, result.message) == (False, "d[x] has 2 odd values")
            with pytest.raises(ValueError, match="a polars validator named is_even exists"):
                PolarsValidations.validator(is_even)
        finally:
            del PolarsValidations.ALL["is_even"]

    @pytest.mark.unit
    def test_python_values_prints_datetimes_and_durations_as_pandas_does(self):
        series = pl.Series([datetime(2020, 1, 1), None])

        assert PolarsValidations.python_values(series) == [pd.Timestamp("2020-01-01"), None]
        assert repr(PolarsValidations.python_values(pl.Series([timedelta(hours=1)]))) == repr([pd.Timedelta(hours=1)])

    @pytest.mark.unit
    def test_python_values_keeps_the_nanoseconds_of_datetimes_and_durations(self):
        durations = pl.Series([1, None], dtype=pl.Duration("ns"))
        datetimes = pl.Series([5], dtype=pl.Datetime("ns", "UTC"))

        assert PolarsValidations.python_values(durations) == [pd.Timedelta(1), None]
        assert PolarsValidations.python_values(datetimes) == [pd.Timestamp(5, tz="UTC")]

    @pytest.mark.unit
    @pytest.mark.parametrize("dtype", [pl.Categorical, pl.Enum(["c", "b", "a"])])
    def test_has_unique_values_orders_the_categories_with_one_count_as_pandas_does(self, dtype):
        values = ["b", "b", "c", "a", "a", "c", "d", "d", "d"] if dtype == pl.Categorical else ["b", "b", "c", "a", "a", "c"]
        expected = validations.has_unique_values("d", pd.DataFrame({"x": pd.Series(values).astype("category")}), "x")

        result = self.run(PolarsValidations.has_unique_values("d", pl.LazyFrame({"x": pl.Series(values, dtype=dtype)}), "x"))

        assert (result.valid, result.message) == (False, expected.message)

    @pytest.mark.unit
    def test_has_unique_values_reports_a_duplicated_nanosecond_duration_as_pandas_does(self):
        expected = validations.has_unique_values("d", pd.DataFrame({"x": pd.to_timedelta([1, 1], unit="ns")}), "x")

        result = self.run(PolarsValidations.has_unique_values("d", pl.LazyFrame({"x": pl.Series([1, 1], dtype=pl.Duration("ns"))}), "x"))

        assert (result.valid, result.message) == (False, expected.message)
        assert "00:00:00.000000001" in result.message

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ("dtype", "values", "kept"),
        [
            (pl.Int64, [1, "unused"], [1]),
            (pl.Int64, [1.0, 2.5], [1]),
            (pl.Float64, [1, 2.5], [1.0, 2.5]),
            (pl.String, ["a", 1], ["a"]),
            (pl.Enum(["a", "b"]), ["a", "z"], ["a"]),
            (pl.Date, [datetime(2020, 1, 1).date(), pd.Timestamp("2020-01-01")], [datetime(2020, 1, 1).date()]),
            (pl.Datetime("ns", "UTC"), [pd.Timestamp("2020-01-01")], []),
            (pl.Datetime("ns"), [pd.Timestamp("2020-01-01", tz="UTC")], []),
            (pl.Datetime("ns", "UTC"), [pd.Timestamp("2020-01-01 01:00", tz="Europe/London")], [datetime(2020, 1, 1, 1, tzinfo=timezone.utc)]),
            (pl.Int64, [], []),
        ],
    )
    def test_candidates_are_the_acceptable_values_that_a_column_can_hold(self, dtype, values, kept):
        candidates = PolarsValidations.candidates(values, dtype)

        assert candidates.dtype == dtype
        assert candidates.to_list() == kept

    @pytest.mark.unit
    def test_a_nanosecond_duration_candidate_keeps_its_nanoseconds(self):
        result = self.run(PolarsValidations.is_in("d", pl.LazyFrame({"x": pl.Series([1], dtype=pl.Duration("ns"))}), "x", categorical_values=[pd.Timedelta(1)]))

        assert result.valid


class TestIsInEdgeCases:
    @staticmethod
    def run(pending):
        return pending.finish(pl.collect_all(list(pending.queries)) if pending.queries else [])

    @pytest.mark.unit
    @pytest.mark.parametrize("zone", ["UTC", None])
    @pytest.mark.parametrize(
        "value",
        [
            datetime(2024, 1, 1, 12, tzinfo=timezone.utc),
            datetime(2024, 1, 1, 12),
            datetime(2024, 1, 1, 7, tzinfo=timezone(timedelta(hours=-5))),
            datetime(2024, 1, 1, 17, tzinfo=timezone(timedelta(hours=5))),
        ],
        ids=["utc", "naive", "minus_5", "plus_5"],
    )
    def test_is_in_matches_a_python_datetime_as_pandas_isin_does(self, zone, value):
        # Given a column at 12:00 UTC, or at 12:00 without a time zone
        series = pl.Series("x", [datetime(2024, 1, 1, 12, tzinfo=timezone.utc) if zone else datetime(2024, 1, 1, 12)], dtype=pl.Datetime("ns", zone))

        # When
        result = self.run(PolarsValidations.is_in("d", series.to_frame().lazy(), "x", categorical_values=[value]))

        # Then the same instant matches a column with a time zone, and only a naive value matches a naive column
        assert result.valid == bool(series.to_pandas().isin([value]).all())

    @pytest.mark.unit
    def test_is_in_compares_with_the_acceptable_values_without_a_polars_deprecation_warning(self):
        frame = pl.LazyFrame({"x": [1, 2, 3]})

        # `collect_all` does not give the warning, so each query is collected alone. polars gives it from its query engine, so it cannot raise
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            pending = PolarsValidations.is_in("d", frame, "x", categorical_values=[1, 2])
            result = pending.finish([query.collect() for query in pending.queries])

        assert result.value == 1
        assert not [warning for warning in caught if issubclass(warning.category, DeprecationWarning) and "is_in" in str(warning.message)]

    @pytest.mark.unit
    def test_is_in_counts_a_null_as_a_value_that_is_not_acceptable(self):
        frame = pl.LazyFrame({"x": ["a", None, "z"]})

        result = self.run(PolarsValidations.is_in("d", frame, "x", categorical_values=["a"]))

        assert result.message == "Values {'z'} for d[x] are not acceptable for 2 cells"
        assert result.value == 2

    @pytest.mark.unit
    def test_is_in_accepts_a_null_when_the_acceptable_values_hold_none(self):
        frame = pl.LazyFrame({"x": ["a", None, "z"]})

        result = self.run(PolarsValidations.is_in("d", frame, "x", categorical_values=["a", None]))

        assert result.message == "Values {'z'} for d[x] are not acceptable for 1 cells"
        assert result.value == 1

    @pytest.mark.unit
    def test_is_in_accepts_a_time_zone_aware_datetime_of_the_acceptable_values(self):
        at = datetime(2020, 1, 1, tzinfo=timezone.utc)
        frame = pl.LazyFrame({"x": pl.Series([at, at])})

        result = self.run(PolarsValidations.is_in("d", frame, "x", categorical_values=[pd.Timestamp(at)], exact=True))

        assert (result.valid, result.value) == (True, 0)

    @pytest.mark.unit
    def test_is_in_on_an_enum_column_accepts_its_categories(self):
        frame = pl.LazyFrame({"x": pl.Series(["a", "b", None], dtype=pl.Enum(["a", "b", "c"]))})

        result = self.run(PolarsValidations.is_in("d", frame, "x", categorical_values=["a", "b", "c"], exact=True))

        assert (result.valid, result.message) == (False, "Missing categorical values for d[x]: {'c'}")

    @pytest.mark.unit
    def test_is_in_prints_numbers_as_numpy_scalars_as_pandas_does(self):
        result = self.run(PolarsValidations.is_in("d", pl.LazyFrame({"x": [1, 7]}), "x", categorical_values=[1]))

        assert result.message == f"Values {set([np.int64(7)])} for d[x] are not acceptable for 1 cells"
        assert not math.isnan(result.value)
