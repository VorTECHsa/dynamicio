# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, too-many-public-methods, too-few-public-methods
import logging

import pytest

from dynamicio.metrics import Counts, CountsPerLabel, log_metric, Max, Mean, Min, Std, UniqueCounts, Variance


@pytest.fixture(autouse=True, scope="module")
def propagate_logger():
    # We need this because otherwise caplog can't capture the logs
    logging.getLogger("dynamicio.metrics").propagate = True
    yield
    logging.getLogger("dynamicio.metrics").propagate = False


class TestMetricsLogger:
    @pytest.mark.unit
    def test_metric_logging_works_even_if_value_is_nan(self, caplog):
        # Given/ When
        with caplog.at_level(logging.INFO):
            log_metric(dataset="Test-DataSet", column="A", metric="B", value=float("nan"))

        # Then
        assert getattr(caplog.records[0], "message") == '{"message": "METRIC", "dataset": "Test-DataSet", "column": "A", "metric": "B", "value": NaN}'

    @pytest.mark.unit
    def test_metric_logging_works_even_if_value_is_inf(self, caplog):
        # Given/ When
        with caplog.at_level(logging.INFO):
            log_metric(dataset="Test-DataSet", column="A", metric="B", value=float("inf"))

        # Then
        assert getattr(caplog.records[0], "message") == '{"message": "METRIC", "dataset": "Test-DataSet", "column": "A", "metric": "B", "value": Infinity}'


class TestMin:
    @pytest.mark.unit
    def test_metric_generation_and_logging(self, caplog, input_df):
        # Given
        df = input_df
        log_min = Min(dataset_name="Test-DataSet", df=df, column="weight_a")

        # When
        with caplog.at_level(logging.INFO):
            print()  # keep this in for a better test output
            log_min()

        # Then
        assert getattr(caplog.records[0], "message") == '{"message": "METRIC", "dataset": "Test-DataSet", "column": "weight_a", "metric": "Min", "value": 5.0}'


class TestMax:
    @pytest.mark.unit
    def test_metric_generation_and_logging(self, caplog, input_df):
        # Given
        df = input_df
        log_max = Max(dataset_name="Test-DataSet", df=df, column="weight_a")

        # When
        with caplog.at_level(logging.INFO):
            print()
            log_max()

        # Then
        assert getattr(caplog.records[0], "message") == '{"message": "METRIC", "dataset": "Test-DataSet", "column": "weight_a", "metric": "Max", "value": 9.0}'


class TestMean:
    @pytest.mark.unit
    def test_metric_generation_and_logging(self, caplog, input_df):
        # Given
        df = input_df
        log_mean = Mean(dataset_name="Test-DataSet", df=df, column="weight_a")

        # When
        with caplog.at_level(logging.INFO):
            print()
            log_mean()

        # Then
        assert getattr(caplog.records[0], "message") == '{"message": "METRIC", "dataset": "Test-DataSet", "column": "weight_a", "metric": "Mean", "value": 6.6}'


class TestStd:
    @pytest.mark.unit
    def test_metric_generation_and_logging(self, caplog, input_df):
        # Given
        df = input_df
        log_std = Std(dataset_name="Test-DataSet", df=df, column="weight_a")

        # When
        with caplog.at_level(logging.INFO):
            print()
            log_std()

        # Then
        assert getattr(caplog.records[0], "message") == '{"message": "METRIC", "dataset": "Test-DataSet", "column": "weight_a", "metric": "Std", "value": 1.429840705968481}'


class TestVariance:
    @pytest.mark.unit
    def test_metric_generation_and_logging(self, caplog, input_df):
        # Given
        df = input_df
        log_var = Variance(dataset_name="Test-DataSet", df=df, column="weight_a")

        # When
        with caplog.at_level(logging.INFO):
            print()
            log_var()

        # Then
        assert getattr(caplog.records[0], "message") == '{"message": "METRIC", "dataset": "Test-DataSet", "column": "weight_a", "metric": "Variance", "value": 2.0444444444444443}'


class TestCounts:
    @pytest.mark.unit
    def test_metric_generation_and_logging(self, caplog, input_df):
        # Given
        df = input_df
        log_counts = Counts(dataset_name="Test-DataSet", df=df, column="weight_a")

        # When
        with caplog.at_level(logging.INFO):
            print()
            log_counts()

        # Then
        assert getattr(caplog.records[0], "message") == '{"message": "METRIC", "dataset": "Test-DataSet", "column": "weight_a", "metric": "Counts", "value": 10.0}'


class TestUniqueCounts:
    @pytest.mark.unit
    def test_metric_generation_and_logging(self, caplog, input_df):
        # Given
        df = input_df
        log_unique_counts = UniqueCounts(dataset_name="Test-DataSet", df=df, column="weight_a")

        # When
        with caplog.at_level(logging.INFO):
            print()
            log_unique_counts()

        # Then
        assert getattr(caplog.records[0], "message") == '{"message": "METRIC", "dataset": "Test-DataSet", "column": "weight_a", "metric": "UniqueCounts", "value": 5.0}'


class TestCountsPerLabel:
    @pytest.mark.unit
    def test_metric_generation_and_logging(self, caplog, input_df):
        # Given
        df = input_df
        log_counts_per_label = CountsPerLabel(dataset_name="Test-DataSet", df=df, column="activity")

        # When
        with caplog.at_level(logging.INFO):
            print()
            log_counts_per_label()

        # Then
        assert (
            (len(caplog.records) == 3)
            and (getattr(caplog.records[0], "message") == '{"message": "METRIC", "dataset": "Test-DataSet", "column": "activity-discharge", "metric": "CountsPerLabel", "value": 5.0}')
            and (getattr(caplog.records[1], "message") == '{"message": "METRIC", "dataset": "Test-DataSet", "column": "activity-load", "metric": "CountsPerLabel", "value": 2.0}')
            and (
                getattr(caplog.records[2], "message") == '{"message": "METRIC", "dataset": "Test-DataSet", "column": "activity-pass_through", "metric": "CountsPerLabel", "value": 3.0}'
            )
        )
