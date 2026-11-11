"""A pytest plugin that lists, at the end of the run, the outputs every :class:`ExpectedOutputs` session verified.

Pytest hides the output of passing tests, and which outputs a test verifies can be decided at runtime (``check_all``), so a green run would not say what it
proved. The plugin loads automatically when dynamicio is installed (``-p no:dynamicio`` turns it off) and prints nothing when no session ran. It needs
no import of pytest: pytest calls the hook below with its terminal reporter.
"""

from dynamicio.testharness.session_report import SessionReport

COLOURS = {"matched": {"green": True}, "failed": {"red": True, "bold": True}, "updated": {"yellow": True}, "skipped": {"yellow": True}}


def pytest_terminal_summary(terminalreporter) -> None:
    """Print every recorded session report: green for matched outputs, red for failed ones."""
    if not SessionReport.recorded:
        return
    terminalreporter.write_sep("=", "dynamicio expected outputs")
    for report in SessionReport.recorded:
        terminalreporter.write_line(report.title, bold=True)
        for status, line in report.lines():
            terminalreporter.write_line(f"  {line}", **COLOURS[status])
