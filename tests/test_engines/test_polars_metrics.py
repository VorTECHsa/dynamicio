# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import math
import random
from unittest.mock import patch

import pandas as pd
import pytest

from dynamicio.schema import metrics

pl = pytest.importorskip("polars")
PolarsMetrics = pytest.importorskip("dynamicio.schema.polars_metrics").PolarsMetrics

NAN = float("nan")

DATA = {
    "floats": ([1.0, NAN, None, 2.0, 2.0, 5.5, -1.0], "float64", pl.Float64),
    "ints": ([3, 1, 1, 3, 2, 2, 7], "int64", pl.Int64),
    "one": ([4.0], "float64", pl.Float64),
    "empty": ([], "float64", pl.Float64),
    "all_null": ([NAN, None, NAN], "float64", pl.Float64),
    "strings": (["a", "b", None, "a", "c", "b", "a"], "object", pl.String),
    "categories": (["a", "b", None, "a", "c", "b", "a"], "category", pl.Categorical),
    "bools": ([True, False, True, True], "bool", pl.Boolean),
}
NUMERIC = ("floats", "ints", "one", "empty", "all_null")


class TestMetricParity:
    """Each polars metric gives the value and the ``METRIC`` lines of the pandas metric of the same name, on the same data."""

    @staticmethod
    def frames(values, pandas_dtype, polars_dtype):
        pandas_frame = pd.DataFrame({"x": pd.Series(values, dtype=pandas_dtype)})
        polars_frame = pl.DataFrame({"x": pl.Series(values, dtype=polars_dtype)})
        if polars_dtype == pl.Float64:
            polars_frame = polars_frame.with_columns(pl.col("x").fill_nan(None))
        return pandas_frame, polars_frame

    @staticmethod
    def logged(call):
        with patch.object(metrics, "log_metric") as log_metric:
            value = call()
        return value, [tuple(c.args) or (c.kwargs["dataset"], c.kwargs["column"], c.kwargs["metric"], c.kwargs["value"]) for c in log_metric.call_args_list]

    @classmethod
    def assert_same(cls, data, metric):
        # Given
        pandas_frame, polars_frame = cls.frames(*data)

        # When
        expected, expected_lines = cls.logged(metrics.get_metric(metric)("d", pandas_frame, "x"))
        computed = polars_frame.lazy().select(PolarsMetrics.ALL[metric]("x").alias("x")).collect().row(0)[0]
        actual, actual_lines = cls.logged(lambda: PolarsMetrics.log("d", "x", metric, computed))

        # Then
        assert [line[:3] for line in actual_lines] == [line[:3] for line in expected_lines]
        for actual_line, expected_line in zip(actual_lines, expected_lines):
            assert float(actual_line[3]) == pytest.approx(float(expected_line[3]), nan_ok=True)
        if isinstance(expected, dict):
            assert actual == expected

    @pytest.mark.unit
    @pytest.mark.parametrize("data", NUMERIC + ("bools",))
    @pytest.mark.parametrize("metric", ["Min", "Max", "Mean", "Std", "Variance", "Counts", "UniqueCounts"])
    def test_a_numeric_metric_is_the_pandas_metric(self, data, metric):
        self.assert_same(DATA[data], metric)

    @pytest.mark.unit
    @pytest.mark.parametrize("data", ["strings", "categories", "empty"])
    @pytest.mark.parametrize("metric", ["Counts", "UniqueCounts", "CountsPerLabel"])
    def test_a_label_metric_is_the_pandas_metric(self, data, metric):
        self.assert_same(DATA[data] if data != "empty" else ([], "object", pl.String), metric)

    @pytest.mark.unit
    def test_metrics_of_seeded_random_floats_are_the_pandas_metrics(self):
        generator = random.Random(25354)
        for _ in range(100):
            values = [generator.choice([None, NAN, round(generator.uniform(-100, 100), 3)]) for _ in range(generator.randint(0, 15))]
            for metric in ("Min", "Max", "Mean", "Std", "Variance", "Counts", "UniqueCounts"):
                self.assert_same((values, "float64", pl.Float64), metric)


class TestMetricLog:
    @pytest.mark.unit
    def test_the_metric_of_an_empty_column_logs_nan(self):
        with patch.object(metrics, "log_metric") as log_metric:
            PolarsMetrics.log("d", "x", "Min", None)

        assert math.isnan(log_metric.call_args.kwargs["value"])

    @pytest.mark.unit
    @pytest.mark.parametrize(("values", "expected"), [([], {"Counts": 0, "UniqueCounts": 0}), ([None, None], {"Counts": 2, "UniqueCounts": 1})])
    def test_the_count_metrics_of_an_empty_or_null_column_log_counts(self, values, expected):
        frame = pl.DataFrame({"x": values}, schema={"x": pl.Float64})
        collected = frame.select(**{name: PolarsMetrics.ALL[name]("x") for name in expected}).row(0, named=True)

        with patch.object(metrics, "log_metric") as log_metric:
            for name in expected:
                PolarsMetrics.log("d", "x", name, collected[name])

        assert [c.kwargs["value"] for c in log_metric.call_args_list] == list(expected.values())

    @pytest.mark.unit
    def test_counts_per_label_logs_one_line_for_each_label_in_sorted_order(self):
        counts = pl.DataFrame({"x": ["b", "a", None, "b"]}).select(PolarsMetrics.ALL["CountsPerLabel"]("x")).item().to_list()

        with patch.object(metrics, "log_metric") as log_metric:
            value = PolarsMetrics.log("d", "x", "CountsPerLabel", counts)

        assert value == {"x-a": 1, "x-b": 2}
        assert [c.args for c in log_metric.call_args_list] == [("d", "x-a", "CountsPerLabel", 1), ("d", "x-b", "CountsPerLabel", 2)]

    @pytest.mark.unit
    def test_a_patch_of_the_old_module_path_sees_the_polars_metrics(self):
        with patch("dynamicio.metrics.log_metric") as log_metric:
            PolarsMetrics.log("d", "x", "Max", 3)

        log_metric.assert_called_once_with(dataset="d", column="x", metric="Max", value=3)

    @pytest.mark.unit
    def test_a_consumer_metric_is_registered_under_its_name(self):
        PolarsMetrics.metric("Median")(lambda column: pl.col(column).median())
        try:
            assert pl.DataFrame({"x": [1, 2, 9]}).select(PolarsMetrics.ALL["Median"]("x")).item() == 2
            with pytest.raises(ValueError, match="a polars metric named Median exists"):
                PolarsMetrics.metric("Median")(lambda column: pl.col(column).median())
        finally:
            del PolarsMetrics.ALL["Median"]

    @pytest.mark.unit
    def test_the_line_of_a_metric_is_the_json_of_the_pandas_engine(self, caplog):
        with caplog.at_level("INFO", logger="dynamicio.metrics"):
            PolarsMetrics.log("d", "x", "Counts", 3)

        assert caplog.records[-1].getMessage() == '{"message": "METRIC", "dataset": "d", "column": "x", "metric": "Counts", "value": 3.0}'
