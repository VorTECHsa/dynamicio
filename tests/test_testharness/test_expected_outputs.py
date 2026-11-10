# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import os
from types import ModuleType

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from dynamicio.config import IOConfig
from dynamicio.testharness import EnvironmentSwitch, ExpectedOutputs, Expectations, OutputFiles, OutputMismatchError, OutputSandbox
from tests.test_testharness.conftest import PARAMS


def _write(config, df, expected=None):
    files = OutputFiles(config, "OUT", PARAMS)
    df.to_parquet(files.generated_path)
    (df if expected is None else expected).to_parquet(files.expected_path)


def _write_day(config, day, df, expected=None):
    files = OutputFiles(config, "OUT", {"day": day})
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


class TestCheckEach:
    @pytest.mark.unit
    def test_every_combination_of_the_grid_is_checked_and_reported_by_its_own_params(self, config):
        for day in ("monday", "tuesday"):
            _write_day(config, day, pd.DataFrame({"a": [1]}), expected=pd.DataFrame({"a": [2]}) if day == "tuesday" else None)

        outputs = ExpectedOutputs()

        assert not outputs.check_each(config, "OUT", {"day": ["monday", "tuesday"]})
        assert outputs.passed == ["OUT{'day': 'monday'}"]
        assert [name for name, _ in outputs.errors] == ["OUT{'day': 'tuesday'}"]

    @pytest.mark.unit
    def test_a_grid_with_2_placeholders_is_a_cartesian_product(self, tmp_path):
        module = ModuleType("vars")
        module.ROOT = str(tmp_path)
        (tmp_path / "expected").mkdir()
        (tmp_path / "definition.yaml").write_text(
            'PART:\n  LOCAL:\n    type: "local"\n    local:\n      file_path: "[[ ROOT ]]/{table}_{shard}.csv"\n      file_type: "csv"\n'
            '    expected:\n      path: "[[ ROOT ]]/expected/{table}_{shard}.csv"\n'
        )
        grid_config = IOConfig(str(tmp_path / "definition.yaml"), env_identifier="LOCAL", dynamic_vars=module)
        for table in ("a", "b"):
            for shard in (0, 1):
                for directory in (tmp_path, tmp_path / "expected"):
                    pd.DataFrame({"x": [shard]}).to_csv(directory / f"{table}_{shard}.csv", index=False)

        with ExpectedOutputs() as outputs:
            outputs.check_each(grid_config, "PART", {"table": ["a", "b"], "shard": range(2)})

        assert len(outputs.passed) == 4

    @pytest.mark.unit
    def test_clear_each_removes_the_file_of_every_combination(self, config):
        for day in ("monday", "tuesday"):
            _write_day(config, day, pd.DataFrame({"a": [1]}))

        ExpectedOutputs.clear_each(config, "OUT", {"day": ["monday", "tuesday"]})

        assert not any(os.path.exists(OutputFiles(config, "OUT", {"day": day}).generated_path) for day in ("monday", "tuesday"))


class TestParquetSchema:
    @staticmethod
    def _write_with_a_stored_range_index(config):
        """The same frame twice; one file stores its default index as a column, which pandas loads back as an equal index."""
        files = OutputFiles(config, "OUT", PARAMS)
        df = pd.DataFrame({"a": [1, 2]})
        pq.write_table(pa.Table.from_pandas(df, preserve_index=True), files.generated_path)
        df.to_parquet(files.expected_path)

    @pytest.mark.unit
    def test_a_difference_only_in_the_file_layout_is_not_seen_unless_asked(self, config):
        self._write_with_a_stored_range_index(config)

        with ExpectedOutputs() as outputs:
            outputs.check(config, "OUT", params=PARAMS)

    @pytest.mark.unit
    def test_a_different_file_layout_fails_when_asked(self, config):
        self._write_with_a_stored_range_index(config)
        outputs = ExpectedOutputs()

        assert not outputs.check(config, "OUT", params=PARAMS, check_parquet_schema=True)
        assert "parquet schema" in outputs.errors[0][1] and "pandas index" in outputs.errors[0][1]

    @pytest.mark.unit
    def test_the_rule_is_ignored_for_other_file_types(self, config):
        files = OutputFiles(config, "JSON_INDEX")
        pd.DataFrame({"a": [1]}).to_json(files.generated_path, orient="index")
        pd.DataFrame({"a": [1]}).to_json(files.expected_path, orient="index")

        with ExpectedOutputs(Expectations(check_parquet_schema=True)) as outputs:
            outputs.check(config, "JSON_INDEX")

    @pytest.mark.unit
    def test_an_identical_layout_passes(self, config):
        _write(config, pd.DataFrame({"a": [1, 2]}))

        with ExpectedOutputs(Expectations(check_parquet_schema=True)) as outputs:
            outputs.check(config, "OUT", params=PARAMS)


class TestOutputSandbox:
    @pytest.mark.unit
    def test_outputs_are_redirected_below_the_directory_and_restored(self, config, tmp_path):
        original = OutputFiles(config, "OUT", PARAMS).generated_path
        scratch = tmp_path / "scratch"

        with OutputSandbox(scratch, config):
            redirected = OutputFiles(config, "OUT", PARAMS).generated_path
            assert redirected == os.path.join(str(scratch), original.lstrip("/"))
            assert os.path.isdir(os.path.dirname(redirected))  # the task can write there without creating the directory
            assert OutputFiles(config, "OUT", PARAMS).expected_path == original.replace("out_", "expected/out_")

        assert OutputFiles(config, "OUT", PARAMS).generated_path == original

    @pytest.mark.unit
    def test_checks_follow_the_redirected_files_and_environments_without_a_local_section_are_left_alone(self, config, prod_config, tmp_path):
        with OutputSandbox(tmp_path / "scratch", config, prod_config):
            files = OutputFiles(config, "OUT", PARAMS)
            pd.DataFrame({"a": [1]}).to_parquet(files.generated_path)
            pd.DataFrame({"a": [1]}).to_parquet(files.expected_path)

            with ExpectedOutputs() as outputs:
                outputs.check(config, "OUT", params=PARAMS)

            assert not os.path.exists(files.generated_path)


class TestEnvironment:
    @staticmethod
    def _write_variant(config, df):
        files = OutputFiles(config, "OUT", PARAMS, environment="VARIANT")
        df.to_parquet(files.generated_path)
        df.to_parquet(files.expected_path)
        return files

    @pytest.mark.unit
    def test_a_session_checks_the_environment_it_is_given_whatever_the_config_selects(self, config):
        files = self._write_variant(config, pd.DataFrame({"a": [1]}))

        with ExpectedOutputs(environment="VARIANT") as outputs:
            outputs.check(config, "OUT", params=PARAMS)

        assert files.environment_name == "VARIANT" and not os.path.exists(files.generated_path)

    @pytest.mark.unit
    def test_a_check_can_override_the_environment_of_the_session(self, config):
        self._write_variant(config, pd.DataFrame({"a": [1]}))
        outputs = ExpectedOutputs(environment="LOCAL")

        assert outputs.check(config, "OUT", params=PARAMS, environment="VARIANT")
        assert not outputs.check(config, "OUT", params=PARAMS)  # LOCAL: nothing generated there

    @pytest.mark.unit
    def test_an_environment_the_resource_does_not_define_is_a_collected_failure_that_names_the_options(self, config):
        outputs = ExpectedOutputs(environment="NOPE")

        assert not outputs.check(config, "OUT", params=PARAMS)
        assert "no environment 'NOPE'" in outputs.errors[0][1] and "VARIANT" in outputs.errors[0][1]

    @pytest.mark.unit
    def test_the_switch_selects_the_environment_and_restores_the_original(self, config, prod_config):
        with EnvironmentSwitch("VARIANT", config, prod_config) as switch:
            assert (config.env_identifier, prod_config.env_identifier) == ("VARIANT", "VARIANT")
            assert switch.environment == "VARIANT"

        assert (config.env_identifier, prod_config.env_identifier) == ("LOCAL", "PROD")

    @pytest.mark.unit
    def test_the_switch_restores_the_original_when_the_test_body_fails(self, config):
        with pytest.raises(RuntimeError):
            with EnvironmentSwitch("VARIANT", config):
                raise RuntimeError("boom")

        assert config.env_identifier == "LOCAL"

    @pytest.mark.unit
    def test_the_switch_rejects_an_environment_no_resource_defines(self, config):
        with pytest.raises(ValueError, match="defines no resource in environment 'TYPO'.*VARIANT"):
            with EnvironmentSwitch("TYPO", config):
                pass

        assert config.env_identifier == "LOCAL"
