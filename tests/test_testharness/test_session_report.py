# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import pandas as pd
import pytest

from dynamicio.testharness import ExpectedOutputs, OutputFiles, OutputMismatchError, SessionReport, pytest_plugin


class FakeReporter:
    def __init__(self):
        self.written = []

    def write_sep(self, sep, title):
        self.written.append((sep, title, {}))

    def write_line(self, line, **markup):
        self.written.append(("line", line, markup))


@pytest.fixture(autouse=True)
def clean_registry():
    SessionReport.recorded.clear()
    yield
    SessionReport.recorded.clear()


def _write(config, source_key, df, expected_df=None):
    files = OutputFiles(config, source_key)
    df.to_parquet(files.generated_path)
    (df if expected_df is None else expected_df).to_parquet(files.expected_path)


class TestSessionReport:
    @pytest.mark.unit
    def test_a_passing_session_lists_every_matched_output_with_its_size(self, task_config, capsys):
        frame = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        _write(task_config, "FIRST", frame)
        _write(task_config, "SECOND", frame)

        with ExpectedOutputs() as outputs:
            outputs.check_all(task_config)

        printed = capsys.readouterr().out
        assert "2 matched, 0 failed" in printed
        assert "✓ FIRST  2 rows x 2 columns, as first.parquet" in printed
        assert "✓ SECOND" in printed
        assert "- BY_DAY" in printed  # skipped outputs are listed too
        assert len(SessionReport.recorded) == 1

    @pytest.mark.unit
    def test_a_failing_session_lists_the_failures_and_the_matched_outputs_in_the_error(self, task_config):
        _write(task_config, "FIRST", pd.DataFrame({"a": [1]}))
        _write(task_config, "SECOND", pd.DataFrame({"a": [1]}), pd.DataFrame({"a": [2]}))

        with pytest.raises(OutputMismatchError) as caught:
            with ExpectedOutputs() as outputs:
                outputs.check_all(task_config)

        assert "Matched (1): FIRST" in str(caught.value)
        assert caught.value.passed == ["FIRST"]
        assert [status for status, _ in outputs.report.lines()] == ["matched", "failed", "skipped"]

    @pytest.mark.unit
    def test_a_session_that_checked_nothing_reports_nothing(self, capsys):
        with ExpectedOutputs():
            pass

        assert capsys.readouterr().out == ""
        assert not SessionReport.recorded

    @pytest.mark.unit
    def test_updated_outputs_are_listed(self, task_config):
        _write(task_config, "FIRST", pd.DataFrame({"a": [1]}))

        with ExpectedOutputs(update_expected=True) as outputs:
            outputs.check(task_config, "FIRST")

        assert outputs.report.lines() == [("updated", "↻ FIRST  expected file rewritten")]
        assert "1 updated" in outputs.report.title


class TestPytestPlugin:
    @pytest.mark.unit
    def test_the_summary_prints_nothing_when_no_session_ran(self):
        reporter = FakeReporter()

        pytest_plugin.pytest_terminal_summary(reporter)

        assert not reporter.written

    @pytest.mark.unit
    def test_the_summary_colours_matched_outputs_green_and_failed_ones_red(self):
        SessionReport.recorded.append(SessionReport({"GOOD": "1 rows x 1 columns"}, [("BAD", "differs\nmore")], [], []))
        reporter = FakeReporter()

        pytest_plugin.pytest_terminal_summary(reporter)

        by_text = {text.strip(): markup for _, text, markup in reporter.written}
        assert by_text["✓ GOOD  1 rows x 1 columns"] == {"green": True}
        assert by_text["✗ BAD  differs"] == {"red": True, "bold": True}

    @pytest.mark.unit
    def test_the_plugin_is_registered_for_pytest(self):
        from importlib.metadata import entry_points  # pylint: disable=import-outside-toplevel

        names = {entry.name: entry.value for entry in entry_points(group="pytest11")}
        assert names["dynamicio"] == "dynamicio.testharness.pytest_plugin"
