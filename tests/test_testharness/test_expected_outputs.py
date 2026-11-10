# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import os

import pandas as pd
import pytest

from dynamicio.testharness import ExpectedOutputs, Expectations, OutputFiles, OutputMismatchError
from tests.test_testharness.conftest import PARAMS


def _write(config, df, expected=None):
    files = OutputFiles(config, "OUT", PARAMS)
    df.to_parquet(files.generated_path)
    (df if expected is None else expected).to_parquet(files.expected_path)


def _generated(config):
    return OutputFiles(config, "OUT", PARAMS).generated_path


class TestExpectedOutputs:
    @pytest.mark.unit
    def test_matching_output_passes_and_its_file_is_removed(self, config):
        _write(config, pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}))

        with ExpectedOutputs() as outputs:
            assert outputs.check(config, "OUT", params=PARAMS)

        assert outputs.passed == ["OUT{'day': 'monday'}"]
        assert not os.path.exists(_generated(config))

    @pytest.mark.unit
    def test_keep_files_leaves_a_matching_output(self, config):
        _write(config, pd.DataFrame({"a": [1]}))

        with ExpectedOutputs(keep_files=True) as outputs:
            outputs.check(config, "OUT", params=PARAMS)

        assert os.path.exists(_generated(config))

    @pytest.mark.unit
    def test_mismatches_are_collected_then_raised_once_and_the_file_is_kept(self, config):
        _write(config, pd.DataFrame({"a": [1, 2]}), expected=pd.DataFrame({"a": [1, 3]}))

        with pytest.raises(OutputMismatchError) as error:
            with ExpectedOutputs() as outputs:
                outputs.check(config, "OUT", label="first", params=PARAMS)
                outputs.check(config, "OUT", label="second", params=PARAMS)  # the earlier failure does not stop later checks

        assert [name for name, _ in error.value.errors] == ["first", "second"]
        assert "The following AssertionErrors occurred (2)" in str(error.value)
        assert os.path.exists(_generated(config))

    @pytest.mark.unit
    def test_a_missing_generated_or_expected_file_is_a_collected_failure(self, config):
        outputs = ExpectedOutputs()
        outputs.check(config, "OUT", params=PARAMS)
        pd.DataFrame({"a": [1]}).to_parquet(_generated(config))
        outputs.check(config, "OUT", params=PARAMS)

        assert "no generated output" in outputs.errors[0][1]
        assert "no expected output" in outputs.errors[1][1]

    @pytest.mark.unit
    def test_row_and_column_order_is_ignored(self, config):
        _write(config, pd.DataFrame({"b": ["y", "x"], "a": [2, 1]}, index=[1, 0]), expected=pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}))

        with ExpectedOutputs() as outputs:
            outputs.check(config, "OUT", params=PARAMS)

    @pytest.mark.unit
    def test_list_columns_can_be_compared_ignoring_element_order(self, config):
        _write(config, pd.DataFrame({"tags": [["b", "a"]]}), expected=pd.DataFrame({"tags": [["a", "b"]]}))

        with ExpectedOutputs() as outputs:
            outputs.check(config, "OUT", params=PARAMS, list_columns_to_sort=["tags"])

    @pytest.mark.unit
    def test_long_messages_are_truncated(self):
        error = OutputMismatchError([("x", "m" * 5000)])

        assert len(str(error)) < 1200

    @pytest.mark.unit
    def test_update_expected_writes_the_reference_instead_of_comparing(self, config):
        _write(config, pd.DataFrame({"a": [1, 2]}), expected=pd.DataFrame({"a": [9]}))

        with ExpectedOutputs(update_expected=True) as outputs:
            outputs.check(config, "OUT", params=PARAMS)

        assert outputs.updated and not outputs.errors
        pd.testing.assert_frame_equal(ExpectedOutputs.read_expected(config, "OUT", PARAMS), pd.DataFrame({"a": [1, 2]}))

    @pytest.mark.unit
    def test_update_expected_is_enabled_by_the_environment(self, monkeypatch):
        monkeypatch.setenv("DYNAMICIO_UPDATE_EXPECTED", "1")

        assert ExpectedOutputs().update_expected

    @pytest.mark.unit
    def test_an_error_from_the_task_is_not_masked_by_the_report(self, config):
        with pytest.raises(RuntimeError, match="task failed"):
            with ExpectedOutputs() as outputs:
                outputs.check(config, "OUT", params=PARAMS)  # would fail: nothing generated
                raise RuntimeError("task failed")

    @pytest.mark.unit
    def test_clear_removes_stale_outputs(self, config):
        _write(config, pd.DataFrame({"a": [1]}))

        ExpectedOutputs.clear(config, "OUT", params=PARAMS)
        ExpectedOutputs.clear(config, "OUT", params=PARAMS)  # absent files are fine

        assert not os.path.exists(_generated(config))

    @pytest.mark.unit
    def test_rules_can_be_relaxed_for_one_check_only(self, config):
        _write(config, pd.DataFrame({"a": [1], "ts": [1]}), expected=pd.DataFrame({"a": [1], "ts": [2]}))
        outputs = ExpectedOutputs(keep_files=True)

        assert outputs.check(config, "OUT", params=PARAMS, ignore_columns=["ts"])
        assert not outputs.check(config, "OUT", params=PARAMS)

    @pytest.mark.unit
    def test_session_expectations_apply_to_every_check_and_can_be_replaced_per_check(self, config):
        _write(config, pd.DataFrame({"a": [1]}), expected=pd.DataFrame({"a": [1.0]}))
        outputs = ExpectedOutputs(Expectations.lenient(), keep_files=True)

        assert outputs.check(config, "OUT", params=PARAMS)
        assert not outputs.check(config, "OUT", params=PARAMS, expectations=Expectations.strict())

    @pytest.mark.unit
    def test_a_failing_custom_assertion_is_reported_with_its_message(self, config):
        def fewer_than_two_rows(actual, _expected):
            assert len(actual) < 2, "too many rows"

        _write(config, pd.DataFrame({"a": [1, 2]}))
        outputs = ExpectedOutputs()

        outputs.check(config, "OUT", params=PARAMS, assertions=[fewer_than_two_rows])

        assert "too many rows" in outputs.errors[0][1]
