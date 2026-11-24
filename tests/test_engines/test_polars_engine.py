# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, redefined-outer-name
import logging
import math
from datetime import datetime
from typing import Annotated, Optional
from unittest.mock import patch

import pandas as pd
import pandera.pandas as pa
import pytest
from pandera.errors import SchemaWarning
from pandera.typing import Series

import dynamicio
from dynamicio.engines import EngineSetting
from dynamicio.errors import ColumnsDataTypeError, EngineResourceError, SchemaValidationError
from dynamicio.schema import Field, Metric, metrics
from tests.test_engines.ios import EngineCheckIO

pl = pytest.importorskip("polars")
pa_polars = pytest.importorskip("pandera.polars")
DataModel = pytest.importorskip("dynamicio.schema.polars").DataModel


class Orders(DataModel, dataset="orders"):
    order_id: int = Field(nullable=False, unique=True, gt=0, metrics=[Metric.MIN, Metric.MAX, Metric.COUNTS])
    price: float = Field(nullable=True, ge=0.0, le=100.0, max_null_fraction=0.5, metrics=[Metric.MEAN, Metric.STD])
    status: str = Field(nullable=False, isin=["open", "closed"], metrics=[Metric.COUNTS_PER_LABEL])


class PandasOrders(pa.DataFrameModel):
    __dataset_name__ = "orders"

    order_id: Series[int] = Field(nullable=False, unique=True, gt=0, metrics=[Metric.MIN, Metric.MAX, Metric.COUNTS])
    price: Series[float] = Field(nullable=True, ge=0.0, le=100.0, max_null_fraction=0.5, metrics=[Metric.MEAN, Metric.STD])
    status: Series[str] = Field(nullable=False, isin=["open", "closed"], metrics=[Metric.COUNTS_PER_LABEL])


class Events(DataModel, dataset="events"):
    at: Annotated[pl.Datetime, "us", "UTC"]
    count: int = Field(nullable=True, pandas_dtype="Int64")


@pytest.fixture
def polars_io():
    """Make an IO object of a polars model on the polars engine: ``polars_io(tmp_path, model, **options)``."""

    def make(tmp_path, model=Orders, **options) -> EngineCheckIO:
        dynamicio.configure(engine="polars")
        return EngineCheckIO.make(tmp_path, model, **options)

    return make


@pytest.fixture
def orders():
    return pl.DataFrame({"order_id": [1, 2, 3], "price": [1.5, float("nan"), 3.0], "status": ["open", "closed", "open"]})


class TestSelectColumns:
    @pytest.mark.unit
    @pytest.mark.parametrize("lazy", [False, True])
    def test_the_schema_columns_are_kept_in_the_order_of_the_frame(self, polars_io, tmp_path, lazy):
        io = polars_io(tmp_path)
        frame = pl.DataFrame({"status": ["open"], "extra": [0], "order_id": [1], "price": [1.0]})
        frame = frame.lazy() if lazy else frame

        selected = EngineSetting.current().select_columns(io, frame)

        assert selected.collect_schema().names() == ["status", "order_id", "price"]
        assert frame.collect_schema().names() == ["status", "extra", "order_id", "price"]


class TestCast:
    @pytest.mark.unit
    def test_a_frame_of_the_model_dtypes_keeps_its_values_and_nan_becomes_null(self, polars_io, tmp_path, orders):
        io = polars_io(tmp_path)

        cast = io._apply_schema(orders)  # pylint: disable=protected-access

        assert cast.schema == pl.Schema({"order_id": pl.Int64, "price": pl.Float64, "status": pl.String})
        assert cast["price"].to_list() == [1.5, None, 3.0]
        assert cast["price"].is_nan().sum() == 0

    @pytest.mark.unit
    def test_columns_cast_to_the_dtypes_of_the_model(self, polars_io, tmp_path):
        io = polars_io(tmp_path)
        frame = pl.DataFrame({"order_id": pl.Series([1, 2], dtype=pl.Int32), "price": [1, 2], "status": pl.Series(["open", "closed"], dtype=pl.Categorical)})

        cast = io._apply_schema(frame)  # pylint: disable=protected-access

        assert cast.schema == pl.Schema({"order_id": pl.Int64, "price": pl.Float64, "status": pl.String})
        assert cast.to_dicts() == [{"order_id": 1, "price": 1.0, "status": "open"}, {"order_id": 2, "price": 2.0, "status": "closed"}]

    @pytest.mark.unit
    @pytest.mark.parametrize("lazy", [False, True])
    def test_a_string_column_parses_to_a_datetime_with_its_time_zone(self, polars_io, tmp_path, lazy):
        io = polars_io(tmp_path, Events)
        frame = pl.DataFrame({"at": ["2020-01-01T10:00:00+00:00", "2021-06-01T00:00:00+00:00"], "count": [1, None]})

        cast = io._apply_schema(frame.lazy() if lazy else frame)  # pylint: disable=protected-access
        cast = cast.collect() if lazy else cast

        assert cast.schema["at"] == pl.Datetime("us", "UTC")
        assert cast["at"].dt.year().to_list() == [2020, 2021]
        assert cast["count"].to_list() == [1, None]

    @pytest.mark.unit
    def test_a_datetime_in_another_time_zone_is_cast_to_the_time_zone_of_the_model(self, polars_io, tmp_path):
        io = polars_io(tmp_path, Events)
        frame = pl.DataFrame({"at": pl.Series([datetime(2020, 1, 1)]).dt.replace_time_zone("Europe/London"), "count": [1]})

        cast = io._apply_schema(frame)  # pylint: disable=protected-access

        assert cast.schema["at"] == pl.Datetime("us", "UTC")

    @pytest.mark.unit
    def test_a_float_nan_casts_to_a_null_of_an_integer_column(self, polars_io, tmp_path):
        io = polars_io(tmp_path, Events)
        frame = pl.DataFrame({"at": [datetime(2020, 1, 1), datetime(2020, 1, 2)], "count": [1.0, float("nan")]}).with_columns(pl.col("at").dt.replace_time_zone("UTC"))

        cast = io._apply_schema(frame)  # pylint: disable=protected-access

        assert cast["count"].to_list() == [1, None]
        assert cast["count"].to_list() == pd.Series([1.0, float("nan")]).astype("Int64").replace({pd.NA: None}).tolist()

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "values, dtype",
        [
            (["1", "x"], pl.String),
            ([300, 1], pl.Int64),
        ],
    )
    def test_a_value_that_does_not_fit_the_dtype_raises(self, polars_io, tmp_path, values, dtype):
        class Small(DataModel, dataset="small"):
            n: pl.Int8

        io = polars_io(tmp_path, Small)

        with pytest.raises(ColumnsDataTypeError):
            io._apply_schema(pl.DataFrame({"n": pl.Series(values, dtype=dtype)}))  # pylint: disable=protected-access

    @pytest.mark.unit
    @pytest.mark.parametrize("values", [["a", "z"], ["a", None, "q"]])
    def test_a_value_that_is_not_a_category_of_an_enum_raises(self, polars_io, tmp_path, values):
        class Labels(DataModel, dataset="labels"):
            label: Annotated[pl.Enum, ["a", "b"]] = Field(nullable=True)

        io = polars_io(tmp_path, Labels)

        with pytest.raises(ColumnsDataTypeError):
            io._apply_schema(pl.DataFrame({"label": values}))  # pylint: disable=protected-access

    @pytest.mark.unit
    def test_a_string_that_is_not_a_datetime_raises(self, polars_io, tmp_path):
        io = polars_io(tmp_path, Events)

        with pytest.raises(ColumnsDataTypeError):
            io._apply_schema(pl.DataFrame({"at": ["not a date"], "count": [1]}))  # pylint: disable=protected-access

    @pytest.mark.unit
    def test_a_column_that_does_not_cast_raises_and_logs_its_name(self, polars_io, tmp_path, caplog):
        io = polars_io(tmp_path)
        frame = pl.DataFrame({"order_id": ["1", "x"], "price": [1.0, 2.0], "status": ["open", "open"]})

        with caplog.at_level(logging.ERROR, logger="dynamicio.core"), pytest.raises(ColumnsDataTypeError):
            io._apply_schema(frame)  # pylint: disable=protected-access

        assert "Tried casting column ORDERS['order_id'] to 'Int64' from 'String', but failed" in caplog.text

    @pytest.mark.unit
    def test_a_lazy_frame_that_does_not_cast_raises_when_it_is_validated(self, polars_io, tmp_path, caplog):
        io = polars_io(tmp_path)
        frame = pl.LazyFrame({"order_id": ["1", "x"], "price": [1.0, 2.0], "status": ["open", "open"]})

        cast = io._apply_schema(frame)  # pylint: disable=protected-access

        with caplog.at_level(logging.ERROR, logger="dynamicio.core"), pytest.raises(ColumnsDataTypeError):
            io.validate_from_schema(cast)
        assert "a cast in its plan failed" in caplog.text

    @pytest.mark.unit
    def test_a_lazy_frame_that_does_not_cast_raises_when_a_dataframe_check_is_made(self, polars_io, tmp_path, caplog):
        class Checked(DataModel, dataset="checked"):
            n: pl.Int8 = Field(nullable=False)

            @pa_polars.dataframe_check
            def small(cls, data):  # pylint: disable=no-self-argument
                return data.lazyframe.select(pl.col("n") < 5)

        io = polars_io(tmp_path, Checked)
        cast = io._apply_schema(pl.LazyFrame({"n": ["1", "x"]}))  # pylint: disable=protected-access

        with caplog.at_level(logging.ERROR, logger="dynamicio.core"), pytest.raises(ColumnsDataTypeError):
            io.validate_from_schema(cast)
        assert "The validations of CHECKED could not collect the frame: a cast in its plan failed" in caplog.text

    @pytest.mark.unit
    def test_a_lazy_frame_that_does_not_cast_raises_when_its_metrics_are_logged(self, polars_io, tmp_path, caplog):
        io = polars_io(tmp_path)
        cast = io._apply_schema(pl.LazyFrame({"order_id": ["1", "x"], "price": [1.0, 2.0], "status": ["open", "open"]}))  # pylint: disable=protected-access

        with caplog.at_level(logging.ERROR, logger="dynamicio.core"), pytest.raises(ColumnsDataTypeError), patch.object(metrics, "log_metric") as log_metric:
            io.log_metrics_from_schema(cast)
        assert "The metrics of ORDERS could not collect the frame: a cast in its plan failed" in caplog.text
        log_metric.assert_not_called()

    @pytest.mark.unit
    def test_a_schema_column_that_is_not_in_the_frame_raises(self, polars_io, tmp_path, caplog):
        io = polars_io(tmp_path)

        with caplog.at_level(logging.ERROR, logger="dynamicio.core"), pytest.raises(ColumnsDataTypeError):
            io._apply_schema(pl.DataFrame({"order_id": [1], "price": [1.0]}))  # pylint: disable=protected-access

        assert "The columns ['status'] of the schema of ORDERS are not in the frame" in caplog.text

    @pytest.mark.unit
    def test_a_column_option_that_is_not_in_the_schema_raises(self, polars_io, tmp_path, orders, caplog):
        io = polars_io(tmp_path, columns=["order_id", "nope"])

        with caplog.at_level(logging.ERROR, logger="dynamicio.core"), pytest.raises(ColumnsDataTypeError):
            io._apply_schema(orders)  # pylint: disable=protected-access

        assert "The columns passed: ['nope'] do not belong to the schema" in caplog.text

    @pytest.mark.unit
    def test_a_column_option_casts_only_those_columns(self, polars_io, tmp_path):
        io = polars_io(tmp_path, columns=["order_id"])
        frame = pl.DataFrame({"order_id": pl.Series([1], dtype=pl.Int8)})

        assert io._apply_schema(frame).schema == pl.Schema({"order_id": pl.Int64})  # pylint: disable=protected-access

    @pytest.mark.unit
    @pytest.mark.parametrize("show", [True, False])
    def test_show_casting_warnings_logs_each_cast_column(self, polars_io, tmp_path, show, caplog):
        io = polars_io(tmp_path, show_casting_warnings=show)
        frame = pl.DataFrame({"order_id": pl.Series([1], dtype=pl.Int8), "price": [1.0], "status": ["open"]})

        with caplog.at_level(logging.INFO, logger="dynamicio.core"):
            io._apply_schema(frame)  # pylint: disable=protected-access

        message = "Expected: 'Int64' dtype for ORDERS['order_id'], found 'Int8'"
        assert (message in caplog.text) is show
        assert "price" not in caplog.text

    @pytest.mark.unit
    def test_the_frame_of_the_caller_does_not_change(self, polars_io, tmp_path, orders):
        io = polars_io(tmp_path)

        io._apply_schema(orders)  # pylint: disable=protected-access

        assert math.isnan(orders["price"][1])


class TestCastStrings:
    """The casts of the strings that the pandas CSV and JSON writers give for durations and booleans."""

    @pytest.mark.unit
    @pytest.mark.parametrize("lazy", [False, True])
    def test_a_string_column_parses_to_a_duration_as_pandas_to_timedelta_does(self, polars_io, tmp_path, lazy):
        class Waits(DataModel, dataset="waits"):
            wait: Annotated[pl.Duration, "ns"] = Field(nullable=True)

        io = polars_io(tmp_path, Waits)
        values = ["0 days 00:00:00.001000", "1 days 02:00:00", "0 days 00:00:00.000000001", None]
        frame = pl.DataFrame({"wait": values})

        cast = io._apply_schema(frame.lazy() if lazy else frame)  # pylint: disable=protected-access
        cast = cast.collect() if lazy else cast

        assert cast.schema["wait"] == pl.Duration("ns")
        assert cast["wait"].to_pandas().tolist()[:3] == pd.to_timedelta(pd.Series(values[:3])).tolist()
        assert cast["wait"].to_list()[3] is None

    @pytest.mark.unit
    @pytest.mark.parametrize("lazy", [False, True])
    def test_a_string_that_is_not_a_duration_raises(self, polars_io, tmp_path, lazy):
        class Waits(DataModel, dataset="waits"):
            wait: Annotated[pl.Duration, "ns"]

        io = polars_io(tmp_path, Waits)
        frame = pl.DataFrame({"wait": ["soon"]})

        # An eager cast raises at once. A lazy cast raises the polars error when the caller collects the frame.
        with pytest.raises(pl.exceptions.InvalidOperationError if lazy else ColumnsDataTypeError):
            cast = io._apply_schema(frame.lazy() if lazy else frame)  # pylint: disable=protected-access
            if lazy:
                cast.collect()

    @pytest.mark.unit
    @pytest.mark.parametrize("lazy", [False, True])
    def test_a_string_column_parses_to_a_boolean_as_pandas_writes_it(self, polars_io, tmp_path, lazy):
        class Flags(DataModel, dataset="flags"):
            flag: bool = Field(nullable=True)

        io = polars_io(tmp_path, Flags)
        frame = pl.DataFrame({"flag": ["True", "False", "true", "false", "TRUE", "FALSE", None]})

        cast = io._apply_schema(frame.lazy() if lazy else frame)  # pylint: disable=protected-access
        cast = cast.collect() if lazy else cast

        assert cast["flag"].to_list() == [True, False, True, False, True, False, None]

    @pytest.mark.unit
    @pytest.mark.parametrize("lazy", [False, True])
    def test_a_string_that_is_not_a_boolean_raises(self, polars_io, tmp_path, lazy):
        class Flags(DataModel, dataset="flags"):
            flag: bool

        io = polars_io(tmp_path, Flags)
        frame = pl.DataFrame({"flag": ["True", "yes"]})

        # An eager cast raises at once. A lazy cast raises the polars error when the caller collects the frame.
        with pytest.raises(pl.exceptions.InvalidOperationError if lazy else ColumnsDataTypeError):
            cast = io._apply_schema(frame.lazy() if lazy else frame)  # pylint: disable=protected-access
            if lazy:
                cast.collect()


class TestValidate:
    @pytest.mark.unit
    @pytest.mark.parametrize("lazy", [False, True])
    def test_a_valid_frame_passes(self, polars_io, tmp_path, orders, lazy):
        io = polars_io(tmp_path)
        cast = io._apply_schema(orders.lazy() if lazy else orders)  # pylint: disable=protected-access

        assert io.validate_from_schema(cast) is io

    @pytest.mark.unit
    def test_failures_are_one_error_by_validation_name_with_the_pandas_messages(self, polars_io, tmp_path):
        # Given the same failing data on both engines
        data = {"order_id": [1, 1, -2, 4], "price": [None, None, None, 200.0], "status": ["open", "lost", "open", "open"]}
        pandas_frame = pd.DataFrame(data)
        expected = TestValidate.pandas_failures(tmp_path, pandas_frame)

        # When
        io = polars_io(tmp_path)
        with pytest.raises(SchemaValidationError) as error:
            io.validate_from_schema(io._apply_schema(pl.DataFrame(data)))  # pylint: disable=protected-access

        # Then
        assert error.value.message == expected

    @staticmethod
    def pandas_failures(tmp_path, frame: pd.DataFrame) -> Optional[dict]:
        EngineSetting.reset()
        io = EngineCheckIO.make(tmp_path, PandasOrders)
        try:
            io.validate_from_schema(io._apply_schema(frame))  # pylint: disable=protected-access
        except SchemaValidationError as error:
            return error.message
        finally:
            EngineSetting.reset()
        return None

    @pytest.mark.unit
    def test_the_validations_and_checks_run_in_one_collect_all(self, polars_io, tmp_path, orders):
        io = polars_io(tmp_path)
        cast = io._apply_schema(orders.lazy())  # pylint: disable=protected-access

        with patch.object(pl, "collect_all", wraps=pl.collect_all) as collect_all:
            io.validate_from_schema(cast)

        collect_all.assert_called_once()

    @pytest.mark.unit
    def test_apply_false_skips_a_validation(self, polars_io, tmp_path):
        io = polars_io(tmp_path)
        for validation in io.schema_validations["order_id"]:
            validation.apply = validation.name != "has_unique_values"

        io.validate_from_schema(io._apply_schema(pl.DataFrame({"order_id": [1, 1], "price": [1.0, 2.0], "status": ["open", "open"]})))  # pylint: disable=protected-access

    @pytest.mark.unit
    def test_a_pandera_check_runs_on_the_polars_frame(self, polars_io, tmp_path):
        class Codes(DataModel, dataset="codes"):
            code: str = pa_polars.Field(str_matches=r"^[A-Z]{3}$")

        io = polars_io(tmp_path, Codes)

        with pytest.raises(SchemaValidationError) as error:
            io.validate_from_schema(pl.DataFrame({"code": ["ABC", "abc", "TOOLONG", None]}))

        # A Pandera field is not nullable by default, so the null fails `has_no_null_values` and is not a failure case of `str_matches`
        assert error.value.message == {"has_no_null_values": "CODES[code] has 1 nulls", "str_matches": "Values ['abc', 'TOOLONG'] for CODES[code] fail the check str_matches!"}

    @pytest.mark.unit
    def test_a_custom_column_check_runs_on_the_polars_frame(self, polars_io, tmp_path):
        class Even(DataModel, dataset="even"):
            n: int

            @pa_polars.check("n", name="is_even")
            @classmethod
            def is_even(cls, data):
                return data.lazyframe.select(pl.col(data.key) % 2 == 0)

        io = polars_io(tmp_path, Even)

        with pytest.raises(SchemaValidationError) as error:
            io.validate_from_schema(pl.DataFrame({"n": [2, 3, 5]}))

        assert error.value.message == {"is_even": "Values [3, 5] for EVEN[n] fail the check is_even!"}

    @pytest.mark.unit
    @pytest.mark.parametrize("values, expected", [([2, 4], None), ([2, 3], {"is_small": "Values [False] for SMALL[n] fail the check is_small!"})])
    def test_a_column_check_that_returns_one_bool_passes_or_fails(self, polars_io, tmp_path, values, expected):
        class Small(DataModel, dataset="small"):
            n: int

            @pa_polars.check("n", name="is_small")
            @classmethod
            def is_small(cls, data):
                return data.lazyframe.select(pl.col(data.key).sum() < 7).collect().item()

        io = polars_io(tmp_path, Small)

        if expected is None:
            io.validate_from_schema(pl.DataFrame({"n": values}))
            return
        with pytest.raises(SchemaValidationError) as error:
            io.validate_from_schema(pl.DataFrame({"n": values + [5]}))
        assert error.value.message == expected

    @pytest.mark.unit
    def test_a_dataframe_check_that_returns_one_bool_passes_or_fails(self, polars_io, tmp_path):
        class Total(DataModel, dataset="total"):
            n: int

            @pa_polars.dataframe_check
            @classmethod
            def small_total(cls, data):
                return data.lazyframe.select(pl.col("n").sum()).collect().item() < 7

        io = polars_io(tmp_path, Total)

        io.validate_from_schema(pl.DataFrame({"n": [1, 2]}))
        with pytest.raises(SchemaValidationError) as error:
            io.validate_from_schema(pl.DataFrame({"n": [4, 5]}))
        assert error.value.message == {"small_total": "TOTAL: 1 failure(s) of small_total, for example [False]"}

    @pytest.mark.unit
    def test_two_dataframe_checks_with_one_name_both_run(self, polars_io, tmp_path):
        class Bounds(DataModel, dataset="bounds"):
            low: int
            high: int

            @pa_polars.dataframe_check(name="bounds")
            @classmethod
            def low_is_positive(cls, data):
                return data.lazyframe.select(pl.col("low") > 0)

            @pa_polars.dataframe_check(name="bounds")
            @classmethod
            def high_is_small(cls, data):
                return data.lazyframe.select(pl.col("high") < 10)

        io = polars_io(tmp_path, Bounds)

        with pytest.raises(SchemaValidationError) as error:
            io.validate_from_schema(pl.DataFrame({"low": [-1, 1], "high": [1, 2]}))
        assert error.value.message == {"bounds": "BOUNDS: 1 failure(s) of bounds, for example [-1, 1]"}

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "a, expected",
        [
            ([1, 1, 2], "GROUPS: 2 failure(s) of multiple_fields_uniqueness, for example [1, 1]"),
            ([1, 3, 2], "GROUPS: 4 failure(s) of multiple_fields_uniqueness, for example [1, 1, 5, 5]"),
        ],
    )
    def test_config_unique_with_groups_reports_the_first_group_with_repeated_rows(self, polars_io, tmp_path, a, expected):
        # The messages are those of the pandas engine for the same data
        class Groups(DataModel, dataset="groups"):
            a: int
            b: int
            c: int

            class Config:
                unique = [["a"], ["b", "c"]]

        io = polars_io(tmp_path, Groups)

        io.validate_from_schema(pl.DataFrame({"a": [1, 2, 3], "b": [1, 2, 1], "c": [5, 6, 7]}))
        with pytest.raises(SchemaValidationError) as error:
            io.validate_from_schema(pl.DataFrame({"a": a, "b": [1, 2, 1], "c": [5, 6, 5]}))
        assert error.value.message == {"multiple_fields_uniqueness": expected}

    @pytest.mark.unit
    def test_config_unique_reports_the_repeated_rows(self, polars_io, tmp_path):
        class Pairs(DataModel, dataset="pairs"):
            a: int
            b: str

            class Config:
                unique = ["a", "b"]

        io = polars_io(tmp_path, Pairs)

        with pytest.raises(SchemaValidationError) as error:
            io.validate_from_schema(pl.DataFrame({"a": [1, 1, 2, 1], "b": ["x", "x", "x", "y"]}))

        assert error.value.message == {"multiple_fields_uniqueness": "PAIRS: 4 failure(s) of multiple_fields_uniqueness, for example [1, 1, 'x', 'x']"}

    @pytest.mark.unit
    @pytest.mark.parametrize("report_duplicates, cells, examples", [("exclude_first", 4, [1, 1, "x", "x"]), ("exclude_last", 4, [1, 1, "x", "x"]), ("all", 6, [1, 1, 1, "x", "x"])])
    def test_report_duplicates_changes_the_rows_that_count(self, report_duplicates, cells, examples):
        # A model always has `report_duplicates="all"`: Pandera has no `Config` key for it, but a `DataFrameSchema` has one
        PolarsValidations = pytest.importorskip("dynamicio.schema.polars_validations").PolarsValidations  # pylint: disable=invalid-name
        frame = pl.LazyFrame({"a": [1, 1, 1, 2], "b": ["x", "x", "x", "y"]})

        pending = PolarsValidations.unique_rows("PAIRS", frame, [["a", "b"]], report_duplicates)
        result = pending.finish(pl.collect_all(list(pending.queries)))

        assert result.value == cells
        assert result.message.endswith(f"for example {examples}")

    @pytest.mark.unit
    def test_config_ordered_reports_the_columns_out_of_order(self, polars_io, tmp_path):
        class Ordered(DataModel, dataset="ordered"):
            a: int
            b: int
            c: int

            class Config:
                ordered = True

        io = polars_io(tmp_path, Ordered)

        io.validate_from_schema(pl.DataFrame({"a": [1], "b": [1], "c": [1]}))
        with pytest.raises(SchemaValidationError) as error:
            io.validate_from_schema(pl.DataFrame({"b": [1], "a": [1], "c": [1]}))

        assert error.value.message == {"column_ordered": "ORDERED: 2 failure(s) of column_ordered, for example ['b', 'a']"}

    @pytest.mark.unit
    def test_a_failing_dataframe_check_with_raise_warning_warns_as_on_pandas(self, polars_io, tmp_path):
        class Spans(DataModel, dataset="spans"):
            start: int
            end: int

            @pa_polars.dataframe_check(raise_warning=True)
            @classmethod
            def start_before_end(cls, data):
                return data.lazyframe.select(pl.col("start") <= pl.col("end"))

        io = polars_io(tmp_path, Spans)

        with pytest.warns(SchemaWarning, match=r"SPANS: 1 failure\(s\) of start_before_end, for example \[5, 4\]"):
            io.validate_from_schema(pl.DataFrame({"start": [1, 5], "end": [2, 4]}))

    @pytest.mark.unit
    def test_a_failing_dataframe_check_with_raise_warning_warns_on_pandas(self, tmp_path):
        class Spans(pa.DataFrameModel):
            __dataset_name__ = "spans"
            start: Series[int]
            end: Series[int]

            @pa.dataframe_check(raise_warning=True)
            @classmethod
            def start_before_end(cls, df):
                return df["start"] <= df["end"]

        io = EngineCheckIO.make(tmp_path, Spans)

        with pytest.warns(SchemaWarning):
            io.validate_from_schema(pd.DataFrame({"start": [1, 5], "end": [2, 4]}))

    @pytest.mark.unit
    def test_config_unique_converts_five_values_of_each_column_for_its_examples(self, polars_io, tmp_path):
        class Pairs(DataModel, dataset="pairs"):
            a: int
            b: int

            class Config:
                unique = ["a", "b"]

        io = polars_io(tmp_path, Pairs)
        frame = pl.DataFrame({"a": [1] * 1000, "b": [2] * 1000})

        with patch.object(pl.Series, "to_list", autospec=True, side_effect=pl.Series.to_list) as to_list, pytest.raises(SchemaValidationError) as error:
            io.validate_from_schema(frame)

        assert error.value.message == {"multiple_fields_uniqueness": "PAIRS: 2000 failure(s) of multiple_fields_uniqueness, for example [1, 1, 1, 1, 1]"}
        assert all(len(call.args[0]) <= 5 for call in to_list.call_args_list)

    @pytest.mark.unit
    def test_a_dataframe_check_reports_its_failing_rows(self, polars_io, tmp_path):
        class Spans(DataModel, dataset="spans"):
            start: int
            end: int

            @pa_polars.dataframe_check
            @classmethod
            def start_before_end(cls, data):
                return data.lazyframe.select(pl.col("start") <= pl.col("end"))

        io = polars_io(tmp_path, Spans)

        with pytest.raises(SchemaValidationError) as error:
            io.validate_from_schema(pl.DataFrame({"start": [1, 5, 9], "end": [2, 4, 3]}))

        assert error.value.message == {"start_before_end": "SPANS: 2 failure(s) of start_before_end, for example [5, 4, 9, 3]"}

    @pytest.mark.unit
    def test_column_and_dataframe_failures_are_in_one_error(self, polars_io, tmp_path):
        class Both(DataModel, dataset="both"):
            a: int = Field(nullable=False, gt=0)

            class Config:
                unique = ["a"]

        io = polars_io(tmp_path, Both)

        with pytest.raises(SchemaValidationError) as error:
            io.validate_from_schema(pl.DataFrame({"a": [-1, -1]}))

        assert set(error.value.message) == {"is_greater_than", "multiple_fields_uniqueness"}

    @pytest.mark.unit
    def test_an_empty_frame_passes_every_validation(self, polars_io, tmp_path):
        io = polars_io(tmp_path)
        frame = pl.DataFrame(schema={"order_id": pl.Int64, "price": pl.Float64, "status": pl.String})

        io.validate_from_schema(io._apply_schema(frame))  # pylint: disable=protected-access


class TestConfigUniqueName:
    @pytest.mark.unit
    def test_config_unique_with_one_name_is_one_group_of_one_column(self, polars_io, tmp_path):
        class One(DataModel, dataset="pairs"):
            a: int

            class Config:
                unique = "a"

        class Listed(DataModel, dataset="pairs"):
            a: int

            class Config:
                unique = ["a"]

        # Given
        frame = pl.DataFrame({"a": [1, 1, 2]})

        # When
        errors = []
        for model in (One, Listed):
            with pytest.raises(SchemaValidationError) as error:
                polars_io(tmp_path, model).validate_from_schema(frame)
            errors.append(error.value.message)

        # Then
        assert errors[0] == errors[1] == {"multiple_fields_uniqueness": "PAIRS: 2 failure(s) of multiple_fields_uniqueness, for example [1, 1]"}


class TestCheckRegistered:
    """A validation or a metric of the model with no polars implementation fails when the IO object is built, before a read loads the data."""

    @pytest.mark.unit
    def test_a_validation_without_a_polars_validator_raises_engine_resource_error(self, polars_io, tmp_path, monkeypatch):
        # Given
        monkeypatch.delitem(pytest.importorskip("dynamicio.schema.polars_validations").PolarsValidations.ALL, "has_unique_values")

        # When / Then
        with pytest.raises(EngineResourceError, match="no validation 'has_unique_values' \\(register it with PolarsValidations.validator\\)"):
            polars_io(tmp_path, apply_schema_validations=True)

    @pytest.mark.unit
    def test_a_metric_without_a_polars_metric_raises_engine_resource_error(self, polars_io, tmp_path, monkeypatch):
        # Given
        monkeypatch.delitem(pytest.importorskip("dynamicio.schema.polars_metrics").PolarsMetrics.ALL, "Min")

        # When / Then
        with pytest.raises(EngineResourceError, match="no metric 'Min' \\(register it with PolarsMetrics.metric\\)"):
            polars_io(tmp_path, log_schema_metrics=True)

    @pytest.mark.unit
    def test_an_io_object_that_does_not_validate_or_log_metrics_is_built(self, polars_io, tmp_path, monkeypatch):
        # Given
        monkeypatch.delitem(pytest.importorskip("dynamicio.schema.polars_metrics").PolarsMetrics.ALL, "Min")
        monkeypatch.delitem(pytest.importorskip("dynamicio.schema.polars_validations").PolarsValidations.ALL, "has_unique_values")

        # When
        io = polars_io(tmp_path)

        # Then
        assert io.schema_metrics


class TestLogMetrics:
    @pytest.mark.unit
    def test_the_metrics_are_the_lines_of_the_pandas_engine(self, polars_io, tmp_path):
        # Given
        data = {"order_id": [3, 1, 2], "price": [1.5, None, 4.5], "status": ["open", "closed", "open"]}
        with patch.object(metrics, "log_metric") as log_metric:
            EngineSetting.reset()
            pandas_io = EngineCheckIO.make(tmp_path, PandasOrders)
            pandas_io.log_metrics_from_schema(pandas_io._apply_schema(pd.DataFrame(data)))  # pylint: disable=protected-access
        expected = log_metric.call_args_list

        # When
        io = polars_io(tmp_path)
        with patch.object(metrics, "log_metric") as log_metric:
            io.log_metrics_from_schema(io._apply_schema(pl.DataFrame(data)))  # pylint: disable=protected-access

        # Then
        assert log_metric.call_args_list == expected

    @pytest.mark.unit
    @pytest.mark.parametrize("lazy", [False, True])
    def test_the_metrics_run_in_one_select(self, polars_io, tmp_path, orders, lazy):
        io = polars_io(tmp_path)
        cast = io._apply_schema(orders.lazy() if lazy else orders)  # pylint: disable=protected-access

        with patch.object(pl.LazyFrame, "collect", autospec=True, side_effect=pl.LazyFrame.collect) as collect, patch.object(metrics, "log_metric"):
            io.log_metrics_from_schema(cast)

        assert collect.call_count == 1

    @pytest.mark.unit
    def test_the_metrics_of_an_empty_frame_are_nan_or_zero(self, polars_io, tmp_path):
        io = polars_io(tmp_path)
        frame = pl.DataFrame(schema={"order_id": pl.Int64, "price": pl.Float64, "status": pl.String})

        with patch.object(metrics, "log_metric") as log_metric:
            io.log_metrics_from_schema(frame)

        values = {c.kwargs["metric"]: c.kwargs["value"] for c in log_metric.call_args_list}
        assert values["Counts"] == 0
        assert all(math.isnan(values[name]) for name in ("Min", "Max", "Mean", "Std"))

    @pytest.mark.unit
    def test_counts_per_label_logs_a_zero_for_an_enum_category_that_is_not_in_the_column(self, polars_io, tmp_path):
        # Given a pandas category column with the same categories
        expected = metrics.CountsPerLabel(dataset_name="LABELS", df=pd.DataFrame({"s": pd.Categorical(["open", "open", None], categories=["open", "closed"])}), column="s")

        class Labels(DataModel, dataset="labels"):
            s: Annotated[pl.Enum, ["open", "closed"]] = Field(nullable=True, metrics=[Metric.COUNTS_PER_LABEL])

        io = polars_io(tmp_path, Labels)

        # When
        with patch.object(metrics, "log_metric") as log_metric:
            io.log_metrics_from_schema(pl.DataFrame({"s": pl.Series(["open", "open", None], dtype=pl.Enum(["open", "closed"]))}))

        # Then
        assert {c.args[1]: c.args[3] for c in log_metric.call_args_list} == expected.calculate_metric() == {"s-open": 2, "s-closed": 0}

    @pytest.mark.unit
    @pytest.mark.parametrize("name", ["count", "count-count"])
    def test_counts_per_label_takes_a_column_with_the_name_of_the_count_field(self, polars_io, tmp_path, name):
        class Counted(DataModel, dataset="counted"):
            label: str = Field(nullable=False, alias=name, metrics=[Metric.COUNTS_PER_LABEL])

        io = polars_io(tmp_path, Counted)

        with patch.object(metrics, "log_metric") as log_metric:
            io.log_metrics_from_schema(pl.DataFrame({name: ["a", "b", "a"]}))

        assert {c.args[1]: c.args[3] for c in log_metric.call_args_list} == {f"{name}-a": 2, f"{name}-b": 1}

    @pytest.mark.unit
    def test_a_model_without_metrics_logs_nothing(self, polars_io, tmp_path):
        class Plain(DataModel, dataset="plain"):
            a: int

        io = polars_io(tmp_path, Plain)

        with patch.object(metrics, "log_metric") as log_metric:
            io.log_metrics_from_schema(pl.DataFrame({"a": [1]}))

        log_metric.assert_not_called()
