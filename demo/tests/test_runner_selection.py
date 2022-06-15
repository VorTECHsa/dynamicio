# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, no-self-use

import pytest

from demo.src.runner_selection import choose_module
from demo.src.runners import staging, transform

AIRFLOW_TASK_MODULES = {"staging": staging, "transform": transform}


class TestUtilities:
    @pytest.mark.unit
    def test_choose_module_returns_the_staging_module_with_module_arg_set_to_staging(self):
        # Given
        module_arg = "staging"

        # When
        func = choose_module(module_arg, AIRFLOW_TASK_MODULES)

        # Then
        assert func == staging

    @pytest.mark.unit
    def test_choose_module_returns_the_transform_module_with_module_arg_set_to_transform(self):
        # Given
        module_arg = "transform"

        # When
        func = choose_module(module_arg, AIRFLOW_TASK_MODULES)

        # Then
        assert func == transform

    @pytest.mark.unit
    def test_choose_module_raises_value_error_with_module_arg_set_to_false_input(self):
        # Given
        module_arg = "whatever"

        # When/Then
        with pytest.raises(ValueError):
            choose_module(module_arg, AIRFLOW_TASK_MODULES)
