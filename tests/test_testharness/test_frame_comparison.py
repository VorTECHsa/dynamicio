# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import pandas as pd
import pytest

from dynamicio.testharness import Expectations, FrameComparison


def compare(actual, expected, **rules):
    FrameComparison(Expectations(**rules)).compare(actual, expected)


class TestFrameComparison:
    @pytest.mark.unit
    def test_row_and_column_order_are_ignored_by_default(self):
        compare(pd.DataFrame({"b": ["y", "x"], "a": [2, 1]}, index=[1, 0]), pd.DataFrame({"a": [1, 2], "b": ["x", "y"]}))

    @pytest.mark.unit
    def test_row_order_can_be_required(self):
        with pytest.raises(AssertionError):
            compare(pd.DataFrame({"a": [2, 1]}), pd.DataFrame({"a": [1, 2]}), check_row_order=True)

    @pytest.mark.unit
    def test_column_order_can_be_required(self):
        with pytest.raises(AssertionError, match="different order"):
            compare(pd.DataFrame({"b": [1], "a": [2]}), pd.DataFrame({"a": [2], "b": [1]}), check_column_order=True)

    @pytest.mark.unit
    def test_sort_by_aligns_frames_without_a_meaningful_index(self):
        compare(pd.DataFrame({"k": [2, 1], "v": ["y", "x"]}, index=[7, 9]), pd.DataFrame({"k": [1, 2], "v": ["x", "y"]}), sort_by=("k",))

    @pytest.mark.unit
    def test_dtypes_are_checked_unless_relaxed(self):
        ints, floats = pd.DataFrame({"a": [1, 2]}), pd.DataFrame({"a": [1.0, 2.0]})

        with pytest.raises(AssertionError):
            compare(ints, floats)
        compare(ints, floats, check_dtype=False)

    @pytest.mark.unit
    def test_float_tolerance_can_be_relaxed_or_made_exact(self):
        close, base = pd.DataFrame({"a": [1.0001]}), pd.DataFrame({"a": [1.0]})

        compare(close, base, rtol=1e-3)
        with pytest.raises(AssertionError):
            compare(close, base)
        with pytest.raises(AssertionError):
            compare(pd.DataFrame({"a": [1.0 + 1e-9]}), base, check_exact=True)

    @pytest.mark.unit
    def test_ignored_columns_are_left_out_and_only_columns_restricts(self):
        actual, expected = pd.DataFrame({"a": [1], "ts": [1]}), pd.DataFrame({"a": [1], "ts": [2]})

        compare(actual, expected, ignore_columns=("ts",))
        compare(actual, expected, only_columns=("a",))
        with pytest.raises(AssertionError):
            compare(actual, expected)

    @pytest.mark.unit
    def test_list_columns_can_ignore_element_order(self):
        compare(pd.DataFrame({"tags": [["b", "a"]]}), pd.DataFrame({"tags": [["a", "b"]]}), list_columns_to_sort=("tags",))

    @pytest.mark.unit
    def test_dict_columns_are_compared_by_content(self):
        compare(pd.DataFrame({"d": [{"a": 1, "b": 2}]}), pd.DataFrame({"d": [{"b": 2, "a": 1}]}))
        with pytest.raises(AssertionError):
            compare(pd.DataFrame({"d": [{"a": 1}]}), pd.DataFrame({"d": [{"a": 2}]}))

    @pytest.mark.unit
    def test_custom_assertions_run_after_the_frames_matched(self):
        def never_negative(actual, _expected):
            assert (actual["a"] >= 0).all(), "negative value"

        compare(pd.DataFrame({"a": [1]}), pd.DataFrame({"a": [1]}), assertions=(never_negative,))
        with pytest.raises(AssertionError, match="negative value"):
            compare(pd.DataFrame({"a": [-1]}), pd.DataFrame({"a": [-1]}), assertions=(never_negative,))
