"""Behaviour contract: metric logging.

Log shippers and dashboards parse these lines, so the logger name, level, line format, dataset naming, emission order and
the lifecycle (when metrics are and are not emitted) are all pinned here.
"""

# pylint: disable=missing-class-docstring, missing-function-docstring, redefined-outer-name
import json
import logging
from unittest.mock import patch

import pandas as pd
import pytest

import dynamicio
from dynamicio.config.pydantic.table_schema import MetricsName
from dynamicio.core import DynamicDataIO
from dynamicio.errors import SchemaValidationError
from dynamicio.schema import metrics


def _line(dataset, column, metric, value):
    return json.dumps({"message": "METRIC", "dataset": dataset, "column": column, "metric": metric, "value": value})


@pytest.mark.unit
def test_metric_names_are_the_public_yaml_vocabulary():
    assert {m.value for m in MetricsName} == {"Min", "Max", "Mean", "Std", "Variance", "Counts", "CountsPerLabel", "UniqueCounts"}
    assert set(metrics.__metrics__) == {m.value for m in MetricsName}


@pytest.mark.unit
def test_dashboards_logging_config_routes_the_metrics_logger_to_a_raw_message_handler():
    # Consumers apply `dynamicio.logging_config`; the `dynamicio.metrics` logger name and the raw `%(message)s` format are relied upon.
    config = dynamicio.logging_config

    assert config["loggers"]["dynamicio.metrics"] == {"handlers": ["metrics"], "level": "INFO", "propagate": False}
    assert config["formatters"]["generic-metrics"] == {"format": "%(message)s"}
    assert config["handlers"]["metrics"]["formatter"] == "generic-metrics"


@pytest.mark.unit
def test_read_emits_one_json_line_per_metric_in_schema_order(make_io, valid_df, metric_records):
    name = make_io.dataset_name

    make_io(valid_df, log_schema_metrics=True).read()

    assert metric_records() == [
        _line(name, "a", "Min", 2.0),
        _line(name, "a", "Counts", 3.0),
        _line(name, "a", "UniqueCounts", 3.0),
        _line(name, "b-x", "CountsPerLabel", 2.0),  # per-label metrics log "<column>-<label>", labels sorted
        _line(name, "b-y", "CountsPerLabel", 1.0),
        _line(name, "c", "Mean", 1.5),
        _line(name, "c", "Max", 2.5),
        _line(name, "c", "Std", 1.0),
        _line(name, "c", "Variance", 1.0),
    ]


@pytest.mark.unit
def test_records_come_from_the_dynamicio_metrics_logger_at_info(make_io, valid_df, caplog, metric_records):
    make_io(valid_df, log_schema_metrics=True).read()

    records = [r for r in caplog.records if r.getMessage() in metric_records()]
    assert records and {(r.name, r.levelname) for r in records} == {("dynamicio.metrics", "INFO")}


@pytest.mark.unit
def test_dataset_name_is_the_schema_name_uppercased_for_yaml_and_the_snake_cased_class_name_for_dict(make_io):
    assert make_io().name == make_io.dataset_name
    assert make_io.dataset_name == ("CONTRACT_DICT_IO" if make_io.style == "dict_schema" else "CONTRACT_DATASET")


@pytest.mark.unit
def test_metrics_are_not_logged_unless_requested(make_io, valid_df, metric_records):
    make_io(valid_df).read()

    assert metric_records() == []


@pytest.mark.unit
def test_failed_validation_on_read_logs_no_metrics(make_io, invalid_df, metric_records):
    with pytest.raises(SchemaValidationError):
        make_io(invalid_df, apply_schema_validations=True, log_schema_metrics=True).read()

    assert metric_records() == []


@pytest.mark.unit
def test_write_emits_metrics_for_schema_columns_only(make_io, valid_df, metric_records):
    make_io(log_schema_metrics=True).write(valid_df.assign(extra=1))

    assert [json.loads(m)["column"] for m in metric_records()] == ["a", "a", "a", "b-x", "b-y", "c", "c", "c", "c"]


@pytest.mark.unit
def test_empty_dataframe_logs_zero_counts_and_nan_for_statistics(make_io, valid_df, metric_records):
    name = make_io.dataset_name

    make_io(log_schema_metrics=True).log_metrics_from_schema(valid_df.iloc[0:0])

    assert metric_records() == [
        _line(name, "a", "Min", float("nan")),
        _line(name, "a", "Counts", 0.0),
        _line(name, "a", "UniqueCounts", 0.0),
        _line(name, "c", "Mean", float("nan")),
        _line(name, "c", "Max", float("nan")),
        _line(name, "c", "Std", float("nan")),
        _line(name, "c", "Variance", float("nan")),
    ]


@pytest.mark.unit
def test_wire_format_wraps_the_raw_line_in_a_json_envelope_for_the_default_handler():
    # `dynamicio.metrics` attaches a JsonFormatter handler to stdout; the envelope is what log shippers see there.
    record = logging.LogRecord("dynamicio.metrics", logging.INFO, __file__, 1, _line("D", "c", "Min", 1.0), None, None)

    assert json.loads(metrics.formatter.format(record)) == {"message": _line("D", "c", "Min", 1.0)}


class TestLifecycleOrder:
    """read: cast -> validate -> metrics.  write: subset columns -> validate -> metrics -> cast -> write."""

    @pytest.mark.unit
    def test_read_order(self, make_io, valid_df):
        calls = []
        with (
            patch.object(DynamicDataIO, "_apply_schema", lambda self, df: calls.append("cast") or df),
            patch.object(DynamicDataIO, "validate_from_schema", lambda self, df: calls.append("validate")),
            patch.object(DynamicDataIO, "log_metrics_from_schema", lambda self, df: calls.append("metrics")),
        ):
            make_io(valid_df, apply_schema_validations=True, log_schema_metrics=True).read()

        assert calls == ["cast", "validate", "metrics"]

    @pytest.mark.unit
    def test_write_order(self, make_io, valid_df):
        calls = []
        original_cast = DynamicDataIO._apply_schema  # pylint: disable=protected-access
        with (
            patch.object(DynamicDataIO, "_apply_schema", lambda self, df: calls.append("cast") or original_cast(self, df)),
            patch.object(DynamicDataIO, "validate_from_schema", lambda self, df: calls.append("validate")),
            patch.object(DynamicDataIO, "log_metrics_from_schema", lambda self, df: calls.append("metrics")),
        ):
            make_io(apply_schema_validations=True, log_schema_metrics=True).write(valid_df)

        assert calls == ["validate", "metrics", "cast"]


@pytest.mark.unit
def test_nothing_is_derived_from_the_dataframe_index(make_io, metric_records):
    df = pd.DataFrame({"a": [2, 3, 4], "b": ["x", "y", "x"], "c": [0.5, 1.5, 2.5]}, index=[10, 20, 30])

    make_io(log_schema_metrics=True).log_metrics_from_schema(df)

    assert len(metric_records()) == 9
