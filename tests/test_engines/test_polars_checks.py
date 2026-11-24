# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
from types import SimpleNamespace

import pytest

from dynamicio.engines.polars_checks import PolarsChecks
from dynamicio.errors import ColumnsDataTypeError

pl = pytest.importorskip("polars")


class TestFailedCasts:
    @staticmethod
    def collect(query: pl.LazyFrame) -> pl.DataFrame:
        with PolarsChecks.failed_casts(SimpleNamespace(name="ORDERS"), "validations"):
            return query.collect()

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "expression",
        [
            pl.col("a").cast(pl.Int64, strict=True),
            pl.col("a").str.to_datetime(time_unit="ns", strict=True),
            pl.col("a").cast(pl.List(pl.Int64)),
        ],
        ids=["strict cast", "datetime parse", "cast that polars does not support"],
    )
    def test_a_failed_cast_raises_columns_data_type_error(self, expression):
        # Given
        query = pl.LazyFrame({"a": ["x"]}).select(expression)

        # When / Then
        with pytest.raises(ColumnsDataTypeError):
            self.collect(query)

    @pytest.mark.unit
    def test_an_error_that_is_not_a_cast_keeps_its_type(self):
        # Given
        query = pl.LazyFrame({"a": ["x"]}).select(pl.col("a").sum())

        # When / Then
        with pytest.raises(pl.exceptions.InvalidOperationError, match="`sum` operation not supported"):
            self.collect(query)
