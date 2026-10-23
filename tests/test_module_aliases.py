"""Old import paths must keep resolving to the relocated modules (downstream pipelines upgrade by version bump only)."""

# pylint: disable=missing-class-docstring,missing-function-docstring
import importlib
import logging

import pytest

from dynamicio.schema import metrics as new_metrics


@pytest.mark.parametrize("name", ["validations", "metrics"])
def test_old_module_path_is_the_relocated_module(name):
    assert importlib.import_module(f"dynamicio.{name}") is importlib.import_module(f"dynamicio.schema.{name}")


def test_metrics_logger_name_does_not_follow_the_module_path():
    assert new_metrics.logger is logging.getLogger("dynamicio.metrics")
