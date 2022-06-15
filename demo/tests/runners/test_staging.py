# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
# noqa
import pytest


class TestStaging:
    @pytest.mark.unit
    def test_a_function(self):
        # Given

        # When

        # Then
        assert True

    @pytest.mark.integration
    def test_a_combination_of_functions(self):
        # Given

        # When

        # Then
        assert True
