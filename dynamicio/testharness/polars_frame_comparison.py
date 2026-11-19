"""Compares two polars frames under a set of :class:`Expectations`, as :class:`FrameComparison` compares two pandas frames."""

from typing import Any, Tuple

import polars as pl
from polars.testing import assert_frame_equal

from dynamicio.testharness.rule_comparison import RuleComparison


class PolarsFrameComparison(RuleComparison):
    """Applies the rules of ``expectations`` to a generated and an expected polars frame, and raises ``AssertionError`` on the first difference.

    A polars frame has no index. Thus ``check_row_order=False`` without ``sort_by`` compares the rows as a multiset: polars sorts both frames by all
    their columns first. A ``LazyFrame`` is collected first.
    """

    def prepare(self, df: Any) -> pl.DataFrame:
        """The frame as the comparison sees it: collected, columns selected, list columns and rows ordered.

        Raises:
            AssertionError: The frame is not a polars frame, or a column that a rule names is missing.
        """
        if isinstance(df, pl.LazyFrame):
            df = df.collect()
        if not isinstance(df, pl.DataFrame):
            raise AssertionError(f"a polars comparison got a {type(df).__module__}.{type(df).__name__}, and not a polars frame")
        rules = self.expectations
        if rules.only_columns is not None:
            self._require_columns(df, rules.only_columns, "only_columns")
            df = df.select(list(rules.only_columns))
        df = df.drop([column for column in rules.ignore_columns if column in df.columns])
        if rules.list_columns_to_sort:
            self._require_columns(df, rules.list_columns_to_sort, "list_columns_to_sort")
            # As in the pandas comparison, a column without lists keeps its values, so the comparison reports the difference
            df = df.with_columns(self.sorted_lists(column, df.schema[column]) for column in rules.list_columns_to_sort)
        if not rules.check_row_order and rules.sort_by:
            self._require_columns(df, rules.sort_by, "sort_by")
            df = df.sort(list(rules.sort_by), maintain_order=True)
        return df

    @staticmethod
    def sorted_lists(column: str, dtype: Any) -> pl.Expr:
        """The expression that sorts each list of a ``List`` or ``Array`` column, and keeps the values of another column."""
        if isinstance(dtype, pl.List):
            return pl.col(column).list.sort()
        if isinstance(dtype, pl.Array):
            return pl.col(column).arr.sort()
        return pl.col(column)

    def compare(self, actual: Any, expected: Any) -> None:
        """Raise ``AssertionError`` if ``actual`` does not satisfy the expectations against ``expected``.

        A polars error of the comparison, for example a sort of a type that polars cannot sort, also raises ``AssertionError``, so the harness records
        the failure and checks the other outputs.
        """
        try:
            super().compare(actual, expected)
        except pl.exceptions.PolarsError as error:
            raise AssertionError(f"polars cannot compare the frames: {type(error).__name__}: {error}") from error

    def _matched(self, actual: pl.DataFrame, expected: pl.DataFrame) -> Tuple[pl.DataFrame, pl.DataFrame]:
        rules = self.expectations
        assert_frame_equal(
            actual,
            expected,
            check_row_order=rules.check_row_order or bool(rules.sort_by),
            check_column_order=rules.check_column_order,
            check_dtypes=rules.check_dtype,
            check_exact=rules.check_exact,
            rel_tol=rules.rtol,
            abs_tol=rules.atol,
        )
        return actual, expected
