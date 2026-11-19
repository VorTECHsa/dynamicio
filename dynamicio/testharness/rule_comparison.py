"""The steps of a comparison under a set of :class:`Expectations` that do not depend on the dataframe library."""

from typing import Any, Sequence, Tuple

from dynamicio.testharness.expectations import Expectations


class RuleComparison:
    """Prepares the two frames, compares them, then runs the custom assertions. A subclass prepares and compares the frames of one library."""

    def __init__(self, expectations: Expectations):
        self.expectations = expectations

    @staticmethod
    def _require_columns(df: Any, columns: Sequence[str], rule: str) -> None:
        """Fail the comparison, instead of raising a ``KeyError`` that would abort the whole session, when a column a rule names is missing."""
        missing = [column for column in columns if column not in df.columns]
        if missing:
            raise AssertionError(f"Columns {missing} named in `{rule}` are missing; the frame has {list(df.columns)}")

    def prepare(self, df: Any) -> Any:
        """The frame as the comparison sees it."""
        raise NotImplementedError

    def _matched(self, actual: Any, expected: Any) -> Tuple[Any, Any]:
        """Assert that the prepared frames are equal, and return the frames that the assertions get."""
        raise NotImplementedError

    def _prepared(self, df: Any, side: str) -> Any:
        try:
            return self.prepare(df)
        except AssertionError as error:
            raise AssertionError(f"{side} output: {error}") from None

    def compare(self, actual: Any, expected: Any) -> None:
        """Raise ``AssertionError`` if ``actual`` does not satisfy the expectations against ``expected``."""
        actual, expected = self._matched(self._prepared(actual, "actual"), self._prepared(expected, "expected"))
        for assertion in self.expectations.assertions:
            assertion(actual, expected)
