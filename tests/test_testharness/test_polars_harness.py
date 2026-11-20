# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, redefined-outer-name
import os
import subprocess
import sys
import textwrap

import pandas as pd
import pytest

# Application Imports
from dynamicio.engines import EngineSetting
from dynamicio.testharness import Expectations, ExpectedOutputs, FrameComparison, OutputFiles, pytest_plugin
from tests.test_testharness.conftest import PARAMS

pl = pytest.importorskip("polars")
PolarsFrameComparison = pytest.importorskip("dynamicio.testharness.polars_frame_comparison").PolarsFrameComparison

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture(autouse=True)
def default_engine():
    EngineSetting.reset()
    yield
    EngineSetting.reset()


@pytest.fixture
def polars_engine():
    EngineSetting.reset("polars")
    yield
    EngineSetting.reset()


class Frames:
    @staticmethod
    def compare(actual, expected, **rules):
        FrameComparison(Expectations(**rules)).compare(actual, expected)


class TestPolarsFrameComparison:
    @pytest.mark.unit
    def test_row_and_column_order_are_ignored_by_default(self):
        Frames.compare(pl.DataFrame({"b": ["y", "x"], "a": [2, 1]}), pl.DataFrame({"a": [1, 2], "b": ["x", "y"]}))

    @pytest.mark.unit
    def test_rows_compare_as_a_multiset_by_default(self):
        with pytest.raises(AssertionError):
            Frames.compare(pl.DataFrame({"a": [1, 1, 2]}), pl.DataFrame({"a": [1, 2, 2]}))

    @pytest.mark.unit
    def test_row_order_can_be_required(self):
        with pytest.raises(AssertionError):
            Frames.compare(pl.DataFrame({"a": [2, 1]}), pl.DataFrame({"a": [1, 2]}), check_row_order=True)

    @pytest.mark.unit
    def test_column_order_can_be_required(self):
        with pytest.raises(AssertionError):
            Frames.compare(pl.DataFrame({"b": [1], "a": [2]}), pl.DataFrame({"a": [2], "b": [1]}), check_column_order=True)

    @pytest.mark.unit
    def test_sort_by_orders_the_rows_before_the_comparison(self):
        Frames.compare(pl.DataFrame({"k": [2, 1], "v": ["y", "x"]}), pl.DataFrame({"k": [1, 2], "v": ["x", "y"]}), sort_by=("k",))

    @pytest.mark.unit
    def test_sort_by_keeps_the_order_of_equal_keys(self):
        with pytest.raises(AssertionError):
            Frames.compare(pl.DataFrame({"k": [1, 1], "v": ["y", "x"]}), pl.DataFrame({"k": [1, 1], "v": ["x", "y"]}), sort_by=("k",))

    @pytest.mark.unit
    def test_dtypes_are_checked_unless_relaxed(self):
        ints, floats = pl.DataFrame({"a": [1, 2]}), pl.DataFrame({"a": [1.0, 2.0]})

        with pytest.raises(AssertionError):
            Frames.compare(ints, floats)
        Frames.compare(ints, floats, check_dtype=False)

    @pytest.mark.unit
    def test_float_tolerance_can_be_relaxed_or_made_exact(self):
        close, base = pl.DataFrame({"a": [1.0001]}), pl.DataFrame({"a": [1.0]})

        Frames.compare(close, base, rtol=1e-3)
        with pytest.raises(AssertionError):
            Frames.compare(close, base)
        with pytest.raises(AssertionError):
            Frames.compare(pl.DataFrame({"a": [1.0 + 1e-9]}), base, check_exact=True)

    @pytest.mark.unit
    def test_ignored_columns_are_left_out_and_only_columns_restricts(self):
        actual, expected = pl.DataFrame({"a": [1], "run": ["x"], "b": [2]}), pl.DataFrame({"a": [1], "run": ["y"], "b": [3]})

        Frames.compare(actual, expected, ignore_columns=("run", "b", "absent"))
        Frames.compare(actual, expected, only_columns=("a",))

    @pytest.mark.unit
    def test_a_missing_column_of_a_rule_fails_and_names_the_side(self):
        with pytest.raises(AssertionError, match=r"expected output: Columns \['k'\] named in `sort_by` are missing"):
            Frames.compare(pl.DataFrame({"k": [1]}), pl.DataFrame({"a": [1]}), sort_by=("k",))
        with pytest.raises(AssertionError, match=r"actual output: Columns \['z'\] named in `only_columns`"):
            Frames.compare(pl.DataFrame({"a": [1]}), pl.DataFrame({"a": [1]}), only_columns=("z",))

    @pytest.mark.unit
    def test_list_columns_can_be_compared_without_their_order(self):
        actual, expected = pl.DataFrame({"tags": [["b", "a"], []]}), pl.DataFrame({"tags": [["a", "b"], []]})

        Frames.compare(actual, expected, list_columns_to_sort=("tags",))
        with pytest.raises(AssertionError):
            Frames.compare(actual, expected, check_row_order=True)

    @pytest.mark.unit
    def test_nulls_and_struct_columns_compare(self):
        frame = pl.DataFrame({"a": [None, 1], "s": [{"x": 1}, None]})

        Frames.compare(frame, frame.reverse())

    @pytest.mark.unit
    def test_lazy_frames_are_collected(self):
        Frames.compare(pl.LazyFrame({"a": [1]}), pl.DataFrame({"a": [1]}))

    @pytest.mark.unit
    def test_the_assertions_get_the_prepared_polars_frames(self):
        seen = []

        def assertion(actual, expected):
            seen.append((type(actual), actual.columns, expected.columns))

        Frames.compare(pl.DataFrame({"a": [1], "run": [2]}), pl.DataFrame({"a": [1], "run": [3]}), ignore_columns=("run",), assertions=(assertion,))

        assert seen == [(pl.DataFrame, ["a"], ["a"])]

    @pytest.mark.unit
    def test_a_failing_assertion_fails_the_comparison(self):
        def positive(actual, _):
            assert actual["a"].min() > 0, "a must be positive"

        with pytest.raises(AssertionError, match="a must be positive"):
            Frames.compare(pl.DataFrame({"a": [-1]}), pl.DataFrame({"a": [-1]}), assertions=(positive,))

    @pytest.mark.unit
    @pytest.mark.parametrize("actual, expected, side", [("polars", "pandas", "expected"), ("pandas", "polars", "actual")])
    def test_a_pandas_frame_against_a_polars_frame_fails(self, actual, expected, side):
        frames = {"polars": pl.DataFrame({"a": [1]}), "pandas": pd.DataFrame({"a": [1]})}

        with pytest.raises(AssertionError, match=f"{side} output: a polars comparison got a pandas"):
            Frames.compare(frames[actual], frames[expected])

    @pytest.mark.unit
    @pytest.mark.parametrize("base", [pl.DataFrame, pl.LazyFrame])
    def test_subclasses_of_polars_frames_go_to_the_polars_comparison(self, base):
        # Given
        App = type("AppFrame", (base,), {"__module__": "app.frames"})
        expected = App({"a": [1, 2], "b": ["x", "y"]})

        # When / Then
        Frames.compare(App({"b": ["y", "x"], "a": [2, 1]}), expected)
        with pytest.raises(AssertionError):
            Frames.compare(App({"a": [1, 3], "b": ["x", "y"]}), expected)

    @pytest.mark.unit
    def test_a_pandas_comparison_does_not_import_polars(self):
        # When
        result = subprocess.run(
            [
                sys.executable,
                "-c",
                "import sys, pandas as pd; from dynamicio.testharness import FrameComparison, Expectations; "
                "FrameComparison(Expectations()).compare(pd.DataFrame({'a': [1]}), pd.DataFrame({'a': [1]})); print('polars' in sys.modules)",
            ],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )

        # Then
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip().splitlines()[-1] == "False"


class TestPolarsComparisonErrors:
    @pytest.mark.unit
    @pytest.mark.parametrize("check_dtype", [True, False])
    @pytest.mark.parametrize("lists_side", ["actual", "expected"])
    def test_a_list_column_that_became_a_string_column_fails_the_comparison(self, check_dtype, lists_side):
        # Given
        lists, strings = pl.DataFrame({"k": [1, 2], "v": [["y", "x"], ["z"]]}), pl.DataFrame({"k": [1, 2], "v": ["x,y", "z"]})
        actual, expected = (lists, strings) if lists_side == "actual" else (strings, lists)

        # When / Then
        with pytest.raises(AssertionError):
            Frames.compare(actual, expected, list_columns_to_sort=("v",), check_dtype=check_dtype)

    @pytest.mark.unit
    @pytest.mark.parametrize("dtype", [pl.List(pl.Int64), pl.Array(pl.Int64, 2)])
    def test_the_lists_of_a_list_or_array_column_are_sorted(self, dtype):
        Frames.compare(pl.DataFrame({"v": [[2, 1]]}, schema={"v": dtype}), pl.DataFrame({"v": [[1, 2]]}, schema={"v": dtype}), list_columns_to_sort=("v",))

    @pytest.mark.unit
    @pytest.mark.parametrize("rules", [{}, {"sort_by": ("v",)}], ids=["multiset", "sort_by"])
    def test_a_column_that_polars_cannot_sort_fails_the_comparison(self, rules):
        # Given
        frame = pl.DataFrame({"v": pl.Series([{1}, {2}], dtype=pl.Object)})

        # When / Then
        with pytest.raises(AssertionError, match="polars cannot compare the frames: InvalidOperationError"):
            Frames.compare(frame, frame, **rules)

    @pytest.mark.unit
    def test_expected_outputs_records_a_polars_error_and_checks_the_next_output(self):
        # Given
        outputs = ExpectedOutputs()
        frame = pl.DataFrame({"k": [1], "v": ["x"]})

        # When
        first = outputs.compare(frame, frame, "lists", Expectations(list_columns_to_sort=("v",)))
        second = outputs.compare(pl.DataFrame({"v": pl.Series([{1}], dtype=pl.Object)}), pl.DataFrame({"v": pl.Series([{1}], dtype=pl.Object)}), "objects")
        third = outputs.compare(frame, frame, "plain")

        # Then
        assert (first, second, third) == (True, False, True)
        assert [name for name, _ in outputs.errors] == ["objects"]


class TestPolarsOutputFiles:
    @pytest.mark.unit
    def test_parquet_is_read_as_a_polars_frame(self, config, polars_engine):  # pylint: disable=unused-argument
        # Given
        files = OutputFiles(config, "OUT", PARAMS)
        pl.DataFrame({"a": [1, None]}).write_parquet(files.generated_path)

        # When
        frame = files.read_generated()

        # Then
        assert isinstance(frame, pl.DataFrame) and frame["a"].to_list() == [1, None]

    @pytest.mark.unit
    def test_csv_is_read_as_a_polars_frame(self, config, polars_engine):  # pylint: disable=unused-argument
        # Given
        files = OutputFiles(config, "NO_EXPECTED")
        pd.DataFrame({"a": [1, 2], "b": ["x", None]}).to_csv(files.generated_path)

        # When
        frame = files.read_generated()

        # Then
        assert isinstance(frame, pl.DataFrame)
        assert frame.columns == ["", "a", "b"] and frame["b"].to_list() == ["x", None]

    @pytest.mark.unit
    @pytest.mark.parametrize("source_key, lines", [("JSON_RECORDS", False), ("JSON_LINES", True)])
    def test_json_records_are_read_with_and_without_lines(self, config, polars_engine, source_key, lines):  # pylint: disable=unused-argument
        # Given
        files = OutputFiles(config, source_key)
        pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}).to_json(files.generated_path, orient="records", lines=lines)

        # When
        frame = files.read_generated()

        # Then
        assert frame.to_dicts() == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]

    @pytest.mark.unit
    def test_a_json_object_without_index_column_is_refused(self, config, polars_engine):  # pylint: disable=unused-argument
        # Given
        files = OutputFiles(config, "JSON_INDEX")
        pd.DataFrame({"a": [1]}).to_json(files.expected_path, orient="index")

        # When / Then
        with pytest.raises(ValueError, match="only with the option index_column"):
            files.read_expected()

    @pytest.mark.unit
    @pytest.mark.parametrize("orient", ["index", "columns"])
    def test_a_json_object_gives_its_keys_in_the_index_column(self, config, polars_engine, orient):  # pylint: disable=unused-argument
        # Given
        files = OutputFiles(config, "JSON_INDEX_COLUMN")
        pd.DataFrame({"a": [1, 2]}, index=["r1", "r2"]).to_json(files.generated_path, orient=orient)

        # When
        frame = OutputFiles.read_file(files.generated_path, "json", orient=orient, index_column="label")

        # Then
        assert frame.to_dicts() == [{"label": "r1", "a": 1}, {"label": "r2", "a": 2}]

    @pytest.mark.unit
    def test_the_index_column_of_the_resource_reaches_the_reader(self, config, polars_engine):  # pylint: disable=unused-argument
        # Given
        files = OutputFiles(config, "JSON_INDEX_COLUMN")
        pd.DataFrame({"a": [1, 2]}, index=["r1", "r2"]).to_json(files.generated_path, orient="index")
        files.promote_generated()

        # When
        generated, expected = files.read_generated(), files.read_expected()

        # Then
        assert files.read_options == {"orient": "index", "index_column": "label"}
        assert generated.equals(expected) and generated.columns == ["label", "a"]

    @pytest.mark.unit
    def test_a_json_orient_other_than_records_index_and_columns_is_refused(self, polars_engine, tmp_path):  # pylint: disable=unused-argument
        # Given
        path = tmp_path / "split.json"
        pd.DataFrame({"a": [1]}).to_json(path, orient="split")

        # When / Then
        with pytest.raises(ValueError, match="records, index and columns only"):
            OutputFiles.read_file(str(path), "json", orient="split")

    @pytest.mark.unit
    @pytest.mark.parametrize("file_type", ["hdf", "txt"])
    def test_a_file_type_without_a_polars_reader_is_refused(self, polars_engine, file_type):  # pylint: disable=unused-argument
        with pytest.raises(ValueError, match="on the polars engine"):
            OutputFiles.read_file("x", file_type)

    @pytest.mark.unit
    def test_the_pandas_engine_reads_with_pandas(self, config):
        # Given
        files = OutputFiles(config, "OUT", PARAMS)
        pl.DataFrame({"a": [1]}).write_parquet(files.generated_path)

        # When / Then
        assert isinstance(files.read_generated(), pd.DataFrame)


class TestPolarsExpectedOutputs:
    @pytest.mark.unit
    def test_a_matching_polars_output_passes_and_is_reported(self, config, polars_engine):  # pylint: disable=unused-argument
        # Given
        files = OutputFiles(config, "OUT", PARAMS)
        pl.DataFrame({"a": [1, 2], "b": ["x", "y"]}).write_parquet(files.expected_path)
        pl.DataFrame({"b": ["y", "x"], "a": [2, 1]}).write_parquet(files.generated_path)

        # When
        with ExpectedOutputs() as outputs:
            matched = outputs.check(config, "OUT", params=PARAMS)

        # Then
        assert matched
        assert outputs.report.lines()[0][0] == "matched"
        assert "2 rows x 2 columns" in outputs.report.lines()[0][1]
        assert not os.path.exists(files.generated_path)

    @pytest.mark.unit
    def test_a_differing_polars_output_fails_and_keeps_the_generated_file(self, config, polars_engine):  # pylint: disable=unused-argument
        # Given
        files = OutputFiles(config, "OUT", PARAMS)
        pl.DataFrame({"a": [1, 2]}).write_parquet(files.expected_path)
        pl.DataFrame({"a": [1, 3]}).write_parquet(files.generated_path)

        # When
        outputs = ExpectedOutputs()
        matched = outputs.check(config, "OUT", params=PARAMS)

        # Then
        assert not matched
        assert outputs.errors and outputs.errors[0][0] == "OUT{'day': 'monday'}"
        assert os.path.exists(files.generated_path)

    @pytest.mark.unit
    def test_the_parquet_schema_check_runs_on_the_polars_engine(self, config, polars_engine):  # pylint: disable=unused-argument
        # Given
        files = OutputFiles(config, "OUT", PARAMS)
        pl.DataFrame({"a": [1]}, schema={"a": pl.Int64}).write_parquet(files.expected_path)
        pl.DataFrame({"a": [1]}, schema={"a": pl.Int32}).write_parquet(files.generated_path)

        # When
        outputs = ExpectedOutputs()
        matched = outputs.check(config, "OUT", params=PARAMS, check_dtype=False, check_parquet_schema=True)

        # Then
        assert not matched
        assert "parquet schema" in outputs.errors[0][1]


class TestUnreadablePolarsOutputs:
    @pytest.mark.unit
    @pytest.mark.parametrize("source_key, body", [("OUT", b"not a parquet file"), ("JSON_RECORDS", b"[{not json")])
    def test_an_unreadable_file_is_a_value_error_with_the_path_and_the_polars_cause(self, config, polars_engine, source_key, body):  # pylint: disable=unused-argument
        # Given
        files = OutputFiles(config, source_key, PARAMS if source_key == "OUT" else None)
        with open(files.expected_path, "wb") as file:
            file.write(body)

        # When / Then
        with pytest.raises(ValueError, match="polars cannot read") as error:
            files.read_expected()
        assert files.expected_path in str(error.value)
        assert isinstance(error.value.__cause__, pl.exceptions.PolarsError)

    @pytest.mark.unit
    def test_an_unreadable_output_is_recorded_and_the_next_output_is_checked(self, config, polars_engine):  # pylint: disable=unused-argument
        # Given
        files = OutputFiles(config, "OUT", PARAMS)
        with open(files.expected_path, "wb") as file:
            file.write(b"not a parquet file")
        pl.DataFrame({"a": [1]}).write_parquet(files.generated_path)
        json_files = OutputFiles(config, "JSON_RECORDS")
        pl.DataFrame({"a": [1]}).write_json(json_files.expected_path)
        pl.DataFrame({"a": [1]}).write_json(json_files.generated_path)

        # When
        outputs = ExpectedOutputs()
        first, second = outputs.check(config, "OUT", params=PARAMS), outputs.check(config, "JSON_RECORDS")

        # Then
        assert not first and second
        assert outputs.errors[0][1].startswith("ValueError: polars cannot read")


class FakeParser:
    def __init__(self):
        self.options = {}

    def addoption(self, name, **kwargs):
        self.options[name] = kwargs


class FakeConfig:
    def __init__(self, engine):
        self.engine = engine

    def getoption(self, name, default=None):
        return self.engine if name == "dynamicio_engine" else default


class TestEngineOption:
    @pytest.mark.unit
    def test_the_plugin_adds_the_engine_option(self):
        # Given
        parser = FakeParser()

        # When
        pytest_plugin.pytest_addoption(parser)

        # Then
        assert parser.options["--dynamicio-engine"]["choices"] == ("pandas", "polars")
        assert parser.options["--dynamicio-engine"]["default"] is None

    @pytest.mark.unit
    def test_the_engine_option_configures_the_engine(self):
        # When
        pytest_plugin.pytest_configure(FakeConfig("polars"))

        # Then
        assert EngineSetting.current().name == "polars"

    @pytest.mark.unit
    def test_without_the_option_the_engine_does_not_change(self):
        # When
        pytest_plugin.pytest_configure(FakeConfig(None))

        # Then
        assert EngineSetting.current().name == "pandas"

    TEST_FILE = textwrap.dedent("""
        from dynamicio.engines import EngineSetting

        def test_engine():
            assert EngineSetting.current().name == "{engine}"
        """)

    @pytest.mark.unit
    @pytest.mark.parametrize("engine", ["pandas", "polars"])
    def test_a_pytest_run_with_the_option_runs_its_tests_on_the_engine(self, tmp_path, engine):
        # Given
        (tmp_path / "test_engine.py").write_text(self.TEST_FILE.format(engine=engine))

        # When
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-o", "addopts=", f"--dynamicio-engine={engine}", str(tmp_path)],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )

        # Then
        assert result.returncode == 0, result.stdout + result.stderr

    @pytest.mark.unit
    def test_an_unknown_engine_is_a_usage_error(self, tmp_path):
        # When
        result = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-o", "addopts=", "--dynamicio-engine=spark", str(tmp_path)],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            check=False,
            timeout=120,
        )

        # Then
        assert result.returncode == 4
        assert "invalid choice: 'spark'" in result.stderr
