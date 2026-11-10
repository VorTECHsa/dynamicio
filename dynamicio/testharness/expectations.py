"""How strictly a generated output is compared with its expected counterpart."""

import dataclasses
from dataclasses import dataclass
from typing import Callable, Optional, Tuple

import pandas as pd

FrameAssertion = Callable[[pd.DataFrame, pd.DataFrame], None]
"""A custom check ``(actual, expected)`` that raises ``AssertionError`` when the output is not acceptable."""


@dataclass(frozen=True)
class Expectations:  # pylint: disable=too-many-instance-attributes
    """The curated rules of a comparison. Immutable: derive stricter or more relaxed variants with :meth:`with_`.

    The defaults are what a pipeline test usually wants: the same data, whatever the order of rows and columns, dtypes included.

    Attributes:
        check_row_order: Require rows in the same order (otherwise they are aligned by index label, or sorted by ``sort_by``).
        check_column_order: Require columns in the same order.
        check_dtype: Require identical dtypes (``int64`` is not ``float64``).
        check_exact: Require exactly equal floats (otherwise ``rtol`` / ``atol`` apply).
        rtol: Relative tolerance for floats.
        atol: Absolute tolerance for floats.
        sort_by: Columns to sort both frames by before comparing, for outputs without a meaningful index. Only used when ``check_row_order`` is False.
        only_columns: Compare just these columns.
        ignore_columns: Leave these columns out of the comparison (timestamps, run ids ...).
        list_columns_to_sort: List-valued columns whose element order does not matter.
        assertions: Extra checks that run after the frames matched, e.g. ``lambda actual, expected: assert actual["price"].min() >= 0``.
    """

    check_row_order: bool = False
    check_column_order: bool = False
    check_dtype: bool = True
    check_exact: bool = False
    rtol: float = 1e-5
    atol: float = 1e-8
    sort_by: Tuple[str, ...] = ()
    only_columns: Optional[Tuple[str, ...]] = None
    ignore_columns: Tuple[str, ...] = ()
    list_columns_to_sort: Tuple[str, ...] = ()
    assertions: Tuple[FrameAssertion, ...] = ()

    @classmethod
    def strict(cls) -> "Expectations":
        """Rows and columns in order, dtypes and floats exact."""
        return cls(check_row_order=True, check_column_order=True, check_exact=True)

    @classmethod
    def lenient(cls) -> "Expectations":
        """Values only: dtypes may differ (``int64`` vs ``float64``) and floats are compared with a looser tolerance."""
        return cls(check_dtype=False, rtol=1e-3)

    def with_(self, **changes) -> "Expectations":
        """A copy with the given rules changed. Sequences are accepted for the tuple-valued rules.

        Raises:
            TypeError: If a rule does not exist.
        """
        for name in ("sort_by", "only_columns", "ignore_columns", "list_columns_to_sort", "assertions"):
            if changes.get(name) is not None:
                changes[name] = tuple(changes[name])
        return dataclasses.replace(self, **changes)

    def also_assert(self, *assertions: FrameAssertion) -> "Expectations":
        """A copy that additionally runs ``assertions``."""
        return self.with_(assertions=self.assertions + assertions)
