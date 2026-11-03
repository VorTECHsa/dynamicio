"""A column whose dtype already is the declared one is not cast again, whatever the name pandas gives that dtype."""

# pylint: disable=missing-function-docstring, protected-access
from unittest.mock import patch

import pandas as pd
import pytest

from dynamicio import UnifiedIO
from dynamicio.config.pydantic import DataframeSchema


def _io(dtype: str) -> UnifiedIO:
    io = object.__new__(UnifiedIO)
    io.schema = DataframeSchema(name="s", columns={"a": {"type": dtype}})
    io.name, io.options, io.show_casting_warnings, io.strict_dtype_check = "S", {}, True, False
    return io


@pytest.mark.unit
@pytest.mark.parametrize(
    "dtype, series",
    [
        ("string[pyarrow]", pd.Series(["x"], dtype="string[pyarrow]")),  # dtype.name is "string"
        ("string", pd.Series(["x"], dtype="string")),
        ("Int64", pd.Series([1, None], dtype="Int64")),
        ("datetime64[ns, UTC]", pd.Series(pd.to_datetime(["2020-01-01"], utc=True))),
        ("category", pd.Series(["x"], dtype="category")),
        ("int64", pd.Series([1])),
    ],
)
def test_a_column_that_already_has_the_declared_dtype_is_not_cast_or_reported(dtype, series):
    df = pd.DataFrame({"a": series})

    with patch("dynamicio.core.logger") as logger:
        assert _io(dtype)._has_valid_dtypes(df)

    logger.info.assert_not_called()


@pytest.mark.unit
def test_a_column_with_another_dtype_is_still_cast():
    df = pd.DataFrame({"a": ["1", "2"]})

    assert _io("int64")._has_valid_dtypes(df) and str(df["a"].dtype) == "int64"
