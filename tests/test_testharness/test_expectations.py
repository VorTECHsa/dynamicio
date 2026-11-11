# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import dataclasses

import pytest

from dynamicio.testharness import Expectations


class TestExpectations:
    @pytest.mark.unit
    def test_defaults_ignore_order_but_check_dtypes(self):
        rules = Expectations()

        assert not rules.check_row_order and not rules.check_column_order
        assert rules.check_dtype

    @pytest.mark.unit
    def test_strict_and_lenient_presets(self):
        assert Expectations.strict().check_row_order and Expectations.strict().check_exact
        assert not Expectations.lenient().check_dtype

    @pytest.mark.unit
    def test_with_derives_a_copy_and_leaves_the_original(self):
        base = Expectations()

        derived = base.with_(ignore_columns=["ts"], rtol=1e-2)

        assert derived.ignore_columns == ("ts",) and derived.rtol == 1e-2
        assert base.ignore_columns == ()

    @pytest.mark.unit
    def test_with_rejects_unknown_rules(self):
        with pytest.raises(TypeError):
            Expectations().with_(check_everything=True)

    @pytest.mark.unit
    def test_expectations_are_immutable(self):
        with pytest.raises(dataclasses.FrozenInstanceError):
            Expectations().rtol = 1  # type: ignore[misc]

    @pytest.mark.unit
    def test_also_assert_appends_assertions(self):
        def check(_actual, _expected):
            pass

        assert Expectations().also_assert(check).also_assert(check).assertions == (check, check)
