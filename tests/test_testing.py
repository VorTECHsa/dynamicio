# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, redefined-outer-name
import os
from types import ModuleType

import pandas as pd
import pytest

from dynamicio.config import IOConfig
from dynamicio.testing import ExpectedOutputs, OutputMismatchError

DEFINITION = """
---
OUT:
  LOCAL:
    type: "local"
    local:
      file_path: "[[ ROOT ]]/out_{day}.parquet"
      file_type: "parquet"
    expected:
      path: "[[ ROOT ]]/expected/out_{day}.parquet"
  PROD:
    type: "kafka"
    kafka:
      kafka_server: "broker"
      kafka_topic: "topic"
NO_EXPECTED:
  LOCAL:
    type: "local"
    local:
      file_path: "[[ ROOT ]]/no_expected.csv"
      file_type: "csv"
"""

PARAMS = {"day": "monday"}


@pytest.fixture
def config(tmp_path):
    (tmp_path / "expected").mkdir()
    (tmp_path / "definition.yaml").write_text(DEFINITION)
    module = ModuleType("vars")
    module.ROOT = str(tmp_path)
    return IOConfig(str(tmp_path / "definition.yaml"), env_identifier="LOCAL", dynamic_vars=module)


def _write(config, df, expected=None):
    df.to_parquet(ExpectedOutputs.generated_path(config, "OUT", PARAMS))
    (df if expected is None else expected).to_parquet(ExpectedOutputs.expected_path(config, "OUT", PARAMS))


class TestExpectedKey:
    @pytest.mark.unit
    def test_expected_is_optional_and_per_environment(self, config):
        assert config.get("OUT").expected.path.endswith("expected/out_{day}.parquet")
        assert config.get("NO_EXPECTED").expected is None
        prod = IOConfig(config.path_to_source_yaml, env_identifier="PROD", dynamic_vars=config.dynamic_vars)
        assert prod.get("OUT").expected is None

    @pytest.mark.unit
    def test_a_resource_without_expected_cannot_be_checked(self, config):
        with pytest.raises(ValueError, match="declares no `expected` path"):
            ExpectedOutputs.expected_path(config, "NO_EXPECTED")

    @pytest.mark.unit
    def test_only_single_file_local_resources_can_be_checked(self, config):
        prod = IOConfig(config.path_to_source_yaml, env_identifier="PROD", dynamic_vars=config.dynamic_vars)
        with pytest.raises(ValueError, match="not a single-file local resource"):
            ExpectedOutputs.generated_path(prod, "OUT")


class TestExpectedOutputs:
    @pytest.mark.unit
    def test_matching_output_passes_and_its_file_is_removed(self, config):
        _write(config, pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}))

        with ExpectedOutputs() as outputs:
            assert outputs.check(config, "OUT", params=PARAMS)

        assert outputs.passed == ["OUT{'day': 'monday'}"]
        assert not os.path.exists(ExpectedOutputs.generated_path(config, "OUT", PARAMS))

    @pytest.mark.unit
    def test_keep_files_leaves_a_matching_output(self, config):
        _write(config, pd.DataFrame({"a": [1]}))

        with ExpectedOutputs(keep_files=True) as outputs:
            outputs.check(config, "OUT", params=PARAMS)

        assert os.path.exists(ExpectedOutputs.generated_path(config, "OUT", PARAMS))

    @pytest.mark.unit
    def test_mismatches_are_collected_then_raised_once_and_the_file_is_kept(self, config):
        _write(config, pd.DataFrame({"a": [1, 2]}), expected=pd.DataFrame({"a": [1, 3]}))

        with pytest.raises(OutputMismatchError) as error:
            with ExpectedOutputs() as outputs:
                outputs.check(config, "OUT", label="first", params=PARAMS)
                outputs.check(config, "OUT", label="second", params=PARAMS)  # the earlier failure does not stop later checks

        assert [name for name, _ in error.value.errors] == ["first", "second"]
        assert "The following AssertionErrors occurred (2)" in str(error.value)
        assert os.path.exists(ExpectedOutputs.generated_path(config, "OUT", PARAMS))

    @pytest.mark.unit
    def test_a_missing_generated_or_expected_file_is_a_collected_failure(self, config):
        outputs = ExpectedOutputs()
        outputs.check(config, "OUT", params=PARAMS)
        pd.DataFrame({"a": [1]}).to_parquet(ExpectedOutputs.generated_path(config, "OUT", PARAMS))
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
    def test_dict_columns_are_compared_by_content(self):
        outputs = ExpectedOutputs()

        assert outputs.compare(pd.DataFrame({"d": [{"a": 1, "b": 2}]}), pd.DataFrame({"d": [{"b": 2, "a": 1}]}), "same")
        assert not outputs.compare(pd.DataFrame({"d": [{"a": 1}]}), pd.DataFrame({"d": [{"a": 2}]}), "different")
        assert [name for name, _ in outputs.errors] == ["different"]

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

        assert not os.path.exists(ExpectedOutputs.generated_path(config, "OUT", PARAMS))
