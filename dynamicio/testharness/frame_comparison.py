"""Compares two dataframes under a set of :class:`Expectations`."""

import json
from typing import Sequence

import pandas as pd

from dynamicio.testharness.expectations import Expectations


class FrameComparison:
    """Applies the rules of ``expectations`` to a generated and an expected frame and raises ``AssertionError`` on the first difference found."""

    def __init__(self, expectations: Expectations):
        self.expectations = expectations

    @staticmethod
    def _require_columns(df: pd.DataFrame, columns: Sequence[str], rule: str) -> None:
        """Fail the comparison, instead of raising a ``KeyError`` that would abort the whole session, when a column a rule names is missing."""
        missing = [column for column in columns if column not in df.columns]
        if missing:
            raise AssertionError(f"Columns {missing} named in `{rule}` are missing; the frame has {list(df.columns)}")

    @staticmethod
    def _sort_list_columns(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
        df = df.copy()
        for column in columns:
            df[column] = df[column].apply(lambda value: sorted(value) if isinstance(value, (list, tuple, set)) or hasattr(value, "tolist") else value)
        return df

    @staticmethod
    def _stringify_unhashable(df: pd.DataFrame) -> pd.DataFrame:
        """Dict (and list) cells can not be hashed by pandas' comparison; compare their canonical JSON text instead."""
        df = df.copy()
        for column in df.columns:
            if df[column].map(lambda value: isinstance(value, (dict, list, set))).any():
                df[column] = df[column].map(lambda value: json.dumps(value, sort_keys=True, default=str) if isinstance(value, (dict, list, set)) else value)
        return df

    def _select_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        rules = self.expectations
        if rules.only_columns is not None:
            self._require_columns(df, rules.only_columns, "only_columns")
            df = df[list(rules.only_columns)]
        return df.drop(columns=[column for column in rules.ignore_columns if column in df.columns])

    def _order_rows(self, df: pd.DataFrame) -> pd.DataFrame:
        rules = self.expectations
        if rules.check_row_order or not rules.sort_by:
            return df
        self._require_columns(df, rules.sort_by, "sort_by")
        return df.sort_values(list(rules.sort_by), kind="stable").reset_index(drop=True)

    def prepare(self, df: pd.DataFrame) -> pd.DataFrame:
        """The frame as the comparison sees it: columns selected, list columns and rows ordered."""
        df = self._select_columns(df)
        if self.expectations.list_columns_to_sort:
            self._require_columns(df, self.expectations.list_columns_to_sort, "list_columns_to_sort")
            df = self._sort_list_columns(df, self.expectations.list_columns_to_sort)
        return self._order_rows(df)

    def _assert_equal(self, actual: pd.DataFrame, expected: pd.DataFrame) -> None:
        rules = self.expectations
        if rules.check_column_order and list(actual.columns) != list(expected.columns) and set(actual.columns) == set(expected.columns):
            raise AssertionError(f"Columns are in a different order: {list(actual.columns)} != {list(expected.columns)}")
        pd.testing.assert_frame_equal(
            actual,
            expected,
            check_like=not rules.check_row_order and not rules.sort_by,
            check_dtype=rules.check_dtype,
            check_exact=rules.check_exact,
            rtol=rules.rtol,
            atol=rules.atol,
        )

    def compare(self, actual: pd.DataFrame, expected: pd.DataFrame) -> None:
        """Raise ``AssertionError`` if ``actual`` does not satisfy the expectations against ``expected``."""
        try:
            actual = self.prepare(actual)
        except AssertionError as error:
            raise AssertionError(f"actual output: {error}") from None
        try:
            expected = self.prepare(expected)
        except AssertionError as error:
            raise AssertionError(f"expected output: {error}") from None
        try:
            self._assert_equal(actual, expected)
        except TypeError as error:
            if "unhashable type" not in str(error):
                raise
            actual, expected = self._stringify_unhashable(actual), self._stringify_unhashable(expected)
            self._assert_equal(actual, expected)
        for assertion in self.expectations.assertions:
            assertion(actual, expected)
