"""The logging config of `dynamicio/__init__.py` must keep the loggers of the dynamicio modules enabled."""

# pylint: disable=missing-class-docstring,missing-function-docstring
import os
import subprocess
import sys

import pytest

MODULES = [
    "dynamicio",
    "dynamicio.core",
    "dynamicio.config.io_config",
    "dynamicio.mixins.utils",
    "dynamicio.mixins.with_athena",
    "dynamicio.mixins.with_kafka",
    "dynamicio.mixins.with_local",
    "dynamicio.mixins.with_postgres",
    "dynamicio.mixins.with_s3",
]


@pytest.mark.parametrize("module", MODULES)
def test_module_logger_is_enabled_after_the_logging_config(module):
    # A new interpreter, so the import runs the logging config of dynamicio/__init__.py, as in a pipeline
    env = {key: value for key, value in os.environ.items() if key != "DYNAMICIO_DO_NOT_CONFIGURE_LOGGING"}
    code = f"import importlib, logging; import dynamicio; logger = importlib.import_module({module!r}).logger; print(logger.name, logger.disabled)"

    result = subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True, check=True)

    assert result.stdout.split() == [module, "False"]
