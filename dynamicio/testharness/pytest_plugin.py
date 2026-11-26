"""A pytest plugin that lists, at the end of the run, the outputs every :class:`ExpectedOutputs` session verified.

Pytest hides the output of passing tests, and which outputs a test verifies can be decided at runtime (``check_all``), so a green run would not say what it
proved. The plugin loads automatically when dynamicio is installed (``-p no:dynamicio`` turns it off) and prints nothing when no session ran. It needs
no import of pytest: pytest calls the hooks below with its parser, its config and its terminal reporter.

The option ``--dynamicio-engine=pandas|polars`` calls ``dynamicio.configure()`` before the first test, so one test suite runs on each engine. One process
cannot change its engine, so CI runs one job for each engine.

Examples:
    Run the tests of a repo on each engine, in two processes:

    ```bash
    pytest --dynamicio-engine=pandas tests
    pytest --dynamicio-engine=polars tests
    ```
"""

from dynamicio.testharness.session_report import SessionReport

ENGINES = ("pandas", "polars")
COLOURS = {"matched": {"green": True}, "failed": {"red": True, "bold": True}, "updated": {"yellow": True}, "skipped": {"yellow": True}}


def pytest_addoption(parser) -> None:
    """Add the option ``--dynamicio-engine``."""
    parser.addoption("--dynamicio-engine", choices=ENGINES, default=None, help="Call dynamicio.configure(engine=...) before the first test.")


def pytest_configure(config) -> None:
    """Configure the engine of the option ``--dynamicio-engine``, if it is given."""
    engine = config.getoption("dynamicio_engine", default=None)
    if engine is not None:
        import dynamicio  # pylint: disable=import-outside-toplevel

        dynamicio.configure(engine=engine)


def pytest_terminal_summary(terminalreporter) -> None:
    """Print every recorded session report: green for matched outputs, red for failed ones."""
    if not SessionReport.recorded:
        return
    terminalreporter.write_sep("=", "dynamicio expected outputs")
    for report in SessionReport.recorded:
        terminalreporter.write_line(report.title, bold=True)
        for status, line in report.lines():
            terminalreporter.write_line(f"  {line}", **COLOURS[status])
