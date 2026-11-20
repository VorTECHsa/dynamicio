"""The validators of the polars engine: the same names, messages and values as ``dynamicio.schema.validations``.

A polars validator does not compute its result. It returns a ``PolarsValidations.Pending``: the lazy queries that compute the result, and a function that
turns their collected frames into a ``ValidationResult``. The engine collects the queries of all validators and of the Pandera checks in one
``pl.collect_all``, so a lazy frame is read once. The module needs polars, so only the polars engine imports it.

The engine casts a frame before it validates it, and the cast changes float NaN to null (decision 3 of ADR 0010). A null is then the only missing value,
as NaN and None are in pandas.
"""

# pylint: disable=too-many-positional-arguments, too-many-public-methods
import math
import operator
import warnings
from typing import Any, Callable, Collection, Dict, List, NamedTuple, Sequence, Tuple

import pandas as pd
import polars as pl
from pandera.errors import SchemaWarning

from dynamicio.schema.validations import ValidationResult


class PolarsValidations:
    """The polars validators, by the names that a schema uses. A consumer adds one with ``@PolarsValidations.validator``."""

    class Pending(NamedTuple):
        """The lazy queries of a validation, and the function that turns their collected frames into its result."""

        queries: Sequence[pl.LazyFrame]
        finish: Callable[[List[pl.DataFrame]], ValidationResult]

    ALL: Dict[str, Callable[..., "PolarsValidations.Pending"]] = {}
    BUILT_IN = (
        "has_unique_values",
        "has_no_null_values",
        "has_acceptable_percentage_of_nulls",
        "is_in",
        "is_greater_than",
        "is_greater_than_or_equal",
        "is_lower_than",
        "is_lower_than_or_equal",
        "is_between",
    )

    @classmethod
    def validator(cls, func: Callable[..., "PolarsValidations.Pending"]) -> Callable[..., "PolarsValidations.Pending"]:
        """Register a polars validator under the name of its function: ``func(dataset, frame, column, **options) -> Pending``."""
        name = func.__name__
        if name in cls.ALL:
            raise ValueError(f"a polars validator named {name} exists")
        cls.ALL[name] = func
        return func

    @staticmethod
    def python_values(series: pl.Series) -> List[Any]:
        """The values of a series as the pandas validators print them: a ``Datetime`` as a ``pd.Timestamp`` and a ``Duration`` as a ``pd.Timedelta``.

        ``to_list`` gives ``datetime`` values, which have microseconds, so a temporal series goes through pandas to keep its nanoseconds.
        """
        if isinstance(series.dtype, (pl.Datetime, pl.Duration)):
            return [None if value is pd.NaT else value for value in series.to_pandas()]
        return series.to_list()

    @staticmethod
    def has_unique_values(dataset: str, frame: pl.LazyFrame, column: str) -> "PolarsValidations.Pending":
        """The non-null values that repeat, most frequent first, as ``value_counts`` gives them in pandas.

        Values with the same count are in the order they first appear. A ``Categorical`` or ``Enum`` column is a pandas ``category`` with sorted
        categories, and pandas puts its values with the same count in the order of the categories.
        """
        categorical = isinstance(frame.collect_schema()[column], (pl.Categorical, pl.Enum))
        # The column is renamed first, so its name does not collide with the names of the row index and of the count
        duplicates = (
            frame.select(pl.col(column).alias("value"))
            .drop_nulls()
            .with_row_index("row")
            .group_by("value")
            .agg(pl.len().alias("count"), pl.col("row").first())
            .filter(pl.col("count") > 1)
            .sort(["count", pl.col("value").cast(pl.String) if categorical else "row"], descending=[True, False])
            .select("value")
        )

        def finish(frames: List[pl.DataFrame]) -> ValidationResult:
            values = PolarsValidations.python_values(frames[0].to_series())
            if not values:
                return ValidationResult(valid=True, message=f"{dataset}[{column}] has unique values", value=0)
            return ValidationResult(valid=False, message=f"Values {values} for {dataset}[{column}] are duplicated!", value=len(values))

        return PolarsValidations.Pending((duplicates,), finish)

    @staticmethod
    def has_no_null_values(dataset: str, frame: pl.LazyFrame, column: str) -> "PolarsValidations.Pending":
        """The number of nulls of a column."""

        def finish(frames: List[pl.DataFrame]) -> ValidationResult:
            nulls = frames[0].item()
            return ValidationResult(valid=nulls == 0, message=f"{dataset}[{column}] has {nulls} nulls", value=nulls)

        return PolarsValidations.Pending((frame.select(pl.col(column).null_count()),), finish)

    @staticmethod
    def has_acceptable_percentage_of_nulls(dataset: str, frame: pl.LazyFrame, column: str, threshold: float) -> "PolarsValidations.Pending":
        """The fraction of nulls of a column, below a threshold. A frame without rows has no nulls."""
        if threshold <= 0 or threshold >= 1:
            raise ValueError(f"Threshold value: {threshold} must be a value between 0 and 1.")

        def finish(frames: List[pl.DataFrame]) -> ValidationResult:
            nulls, rows = frames[0].row(0)
            percentage_of_nulls = 0.0 if rows == 0 else nulls / rows
            if percentage_of_nulls < threshold:
                return ValidationResult(valid=True, message=f"Percentage of nulls of for {dataset}[{column}] is {percentage_of_nulls}", value=percentage_of_nulls)
            return ValidationResult(
                valid=False,
                message=f"Percentage of nulls of for {dataset}[{column}] is {percentage_of_nulls} which exceeds threshold: {threshold}",
                value=percentage_of_nulls,
            )

        return PolarsValidations.Pending((frame.select(pl.col(column).null_count().alias("nulls"), pl.len().alias("rows")),), finish)

    @staticmethod
    def is_in(dataset: str, frame: pl.LazyFrame, column: str, categorical_values: Collection[Any], exact: bool = False) -> "PolarsValidations.Pending":
        """The non-null values of a column are a subset of the acceptable values, or exactly them when ``exact`` is True.

        As in pandas, the set of values ignores nulls, but the count of cells that are not acceptable includes the nulls that ``isin`` does not accept.
        """
        acceptable = set(categorical_values)
        values = [value for value in acceptable if not PolarsValidations.is_null(value)]
        dtype = frame.collect_schema()[column]
        null_is_acceptable = PolarsValidations.null_is_acceptable(dtype, acceptable)
        unique = frame.select(pl.col(column).drop_nulls().unique())
        not_acceptable = frame.select((~pl.col(column).is_in(PolarsValidations.candidates(values, dtype).implode()).fill_null(null_is_acceptable)).sum())

        def finish(frames: List[pl.DataFrame]) -> ValidationResult:
            # The values of `to_pandas` are the scalars that `unique()` gives in pandas: `pd.Timestamp` with its time zone, numpy integers
            unique_values = set(frames[0].to_series().to_pandas().unique())
            count_invalid = frames[1].item()
            not_acceptable_message = f"Values {unique_values - acceptable} for {dataset}[{column}] are not acceptable for {count_invalid} cells"
            if exact:
                if unique_values == acceptable:
                    return ValidationResult(valid=True, message=f"All acceptable categorical values for {dataset}[{column}] are present", value=0)
                if unique_values < acceptable:
                    missing = acceptable - unique_values
                    return ValidationResult(valid=False, message=f"Missing categorical values for {dataset}[{column}]: {missing}", value=len(missing))
                return ValidationResult(valid=False, message=not_acceptable_message, value=count_invalid)
            if unique_values.issubset(acceptable):
                return ValidationResult(valid=True, message=f"Categorical values for {dataset}[{column}] are acceptable", value=0)
            return ValidationResult(valid=False, message=not_acceptable_message, value=count_invalid)

        return PolarsValidations.Pending((unique, not_acceptable), finish)

    @classmethod
    def candidates(cls, values: Collection[Any], dtype: pl.DataType) -> pl.Series:
        """The acceptable values that a column of ``dtype`` can hold, as a series of ``dtype``.

        A value of another type, for example ``"unused"`` for an ``Int64`` column, matches no cell in pandas, so it is left out. A series of all the
        values would coerce them to one type, and polars refuses to compare an ``Int64`` column with a ``String`` series.
        """
        kept = []
        for value in values:
            try:
                kept.append(cls.candidate(value, dtype))
            except (TypeError, ValueError, OverflowError, pl.exceptions.PolarsError):
                continue
        return pl.concat(kept) if kept else pl.Series([], dtype=dtype)

    @staticmethod
    def candidate(value: Any, dtype: pl.DataType) -> pl.Series:
        """One acceptable value as a series of ``dtype``. It raises for a value that a column of ``dtype`` cannot hold.

        A ``pd.Timestamp`` or a ``pd.Timedelta`` goes through pandas to keep its nanoseconds. In pandas, a time zone-naive timestamp matches no cell of a
        time zone-aware column, and the reverse. A float with no fraction matches the equal integer, as ``1.0 == 1``.
        """
        if isinstance(value, pd.Timestamp) and isinstance(dtype, pl.Datetime):
            if (value.tz is None) != (dtype.time_zone is None):
                raise TypeError(f"{value!r} and a {dtype} column have different time zone kinds")
            return pl.from_pandas(pd.Series([value])).cast(dtype, strict=True)
        if isinstance(value, pd.Timedelta) and isinstance(dtype, pl.Duration):
            return pl.from_pandas(pd.Series([value])).cast(dtype, strict=True)
        if dtype.is_integer() and isinstance(value, float) and value.is_integer():
            value = int(value)
        series = pl.Series([value], dtype=dtype, strict=True)
        if series.dtype != dtype:  # polars keeps the type of a `datetime` for a `Date` series
            raise TypeError(f"{value!r} is not a {dtype} value")
        return series

    @staticmethod
    def is_null(value: Any) -> bool:
        """Whether a value is None or a float NaN."""
        return value is None or (isinstance(value, float) and math.isnan(value))

    @staticmethod
    def null_is_acceptable(dtype: pl.DataType, acceptable: Collection[Any]) -> bool:
        """Whether ``isin`` in pandas accepts a null of the pandas column of a polars dtype, for a set of acceptable values.

        A float column holds NaN, which only a NaN matches. An integer or Boolean column of a model holds ``<NA>``, which no value matches. A string,
        categorical or temporal column holds None, NaN or ``NaT``, and None or NaN matches each.
        """
        if dtype.is_float():
            return any(isinstance(value, float) and math.isnan(value) for value in acceptable)
        if dtype.is_integer() or dtype == pl.Boolean:
            return False
        return any(PolarsValidations.is_null(value) for value in acceptable)

    @staticmethod
    def bounded(frame: pl.LazyFrame, column: str, valid: pl.Expr, finish: Callable[[int, int], ValidationResult]) -> "PolarsValidations.Pending":
        """The non-null count of a column and the number of its non-null values that fail ``valid``, for the comparison validators. A null passes."""
        counts = frame.select(pl.col(column).count().alias("non_null"), (~valid).sum().alias("invalid"))
        return PolarsValidations.Pending((counts,), lambda frames: finish(*frames[0].row(0)))

    @staticmethod
    def compare(dataset: str, frame: pl.LazyFrame, column: str, threshold: Any, compare: Callable[[pl.Expr, Any], pl.Expr], word: str, opposite: str) -> "PolarsValidations.Pending":
        """A comparison of each non-null value of a column with a threshold, with the messages of the pandas comparison validators."""

        def finish(non_null: int, invalid: int) -> ValidationResult:
            if invalid == 0:
                return ValidationResult(valid=True, message=f"All values of {dataset}[{column}] are {word} {threshold}", value=0)
            return ValidationResult(valid=False, message=f"{invalid} cell values for {dataset}[{column}] are {opposite} {threshold}", value=invalid / non_null)

        return PolarsValidations.bounded(frame, column, compare(pl.col(column), threshold), finish)

    @staticmethod
    def is_greater_than(dataset: str, frame: pl.LazyFrame, column: str, threshold: float) -> "PolarsValidations.Pending":
        """Each non-null value is greater than the threshold."""
        return PolarsValidations.compare(dataset, frame, column, threshold, operator.gt, "above", "below")

    @staticmethod
    def is_greater_than_or_equal(dataset: str, frame: pl.LazyFrame, column: str, threshold: float) -> "PolarsValidations.Pending":
        """Each non-null value is greater than or equal to the threshold."""
        return PolarsValidations.compare(dataset, frame, column, threshold, operator.ge, "above", "below")

    @staticmethod
    def is_lower_than(dataset: str, frame: pl.LazyFrame, column: str, threshold: float) -> "PolarsValidations.Pending":
        """Each non-null value is lower than the threshold."""
        return PolarsValidations.compare(dataset, frame, column, threshold, operator.lt, "below", "above")

    @staticmethod
    def is_lower_than_or_equal(dataset: str, frame: pl.LazyFrame, column: str, threshold: float) -> "PolarsValidations.Pending":
        """Each non-null value is lower than or equal to the threshold."""
        return PolarsValidations.compare(dataset, frame, column, threshold, operator.le, "below", "above")

    @staticmethod
    def is_between(dataset: str, frame: pl.LazyFrame, column: str, lower: float, upper: float, include_left: bool = False, include_right: bool = False) -> "PolarsValidations.Pending":
        """Each non-null value is between the bounds. ``include_left`` and ``include_right`` include the bounds."""
        lower_bound = operator.ge if include_left else operator.gt
        upper_bound = operator.le if include_right else operator.lt

        def finish(non_null: int, invalid: int) -> ValidationResult:
            if invalid == 0:
                return ValidationResult(valid=True, message=f"All values of {dataset}[{column}] is between {lower} and {upper} thresholds", value=0)
            return ValidationResult(valid=False, message=f"{invalid} cell values for {dataset}[{column}] are either below {lower} or above {upper}", value=invalid / non_null)

        return PolarsValidations.bounded(frame, column, lower_bound(pl.col(column), lower) & upper_bound(pl.col(column), upper), finish)

    @staticmethod
    def pandera_check(dataset: str, frame: pl.LazyFrame, column: str, pandera_check: Any) -> "PolarsValidations.Pending":  # pylint: disable=redefined-outer-name
        """A Pandera ``Check`` of a column that dynamicio has no validator of its own for (``str_matches``, a custom ``@pa.check``).

        Not a registered validator: the engine calls it for the checks that ``schema.models`` compiles. The value is the number of failing values.
        """
        result = pandera_check(frame, column)

        def finish(frames: List[pl.DataFrame]) -> ValidationResult:
            passed, failing, first = PolarsValidations.check_frames(frames)
            if passed:
                return ValidationResult(valid=True, message=f"{dataset}[{column}] passes {pandera_check.name}", value=0)
            examples = PolarsValidations.python_values(first.to_series(0)) if first is not None else [False]
            return ValidationResult(valid=False, message=f"Values {examples} for {dataset}[{column}] fail the check {pandera_check.name}!", value=failing)

        return PolarsValidations.Pending(PolarsValidations.check_queries(result, result.failure_cases), finish)

    @staticmethod
    def check_queries(result: Any, failure_cases: Any) -> Sequence[pl.LazyFrame]:
        """The queries of a Pandera check result: whether it passed, and the count and the first rows of its failure cases.

        A check that returns one bool, not a bool for each row, has no failure cases: its only query is whether it passed.
        """
        if failure_cases is None:
            return (result.check_passed,)
        return (result.check_passed, failure_cases.select(pl.len()), failure_cases.head(5))

    @staticmethod
    def check_frames(frames: List[pl.DataFrame]) -> tuple:
        """``(passed, failing count, first failure cases)`` of the collected ``check_queries``. A check of one bool that fails has one failure and no rows."""
        passed = bool(frames[0].item())
        if len(frames) == 1:
            return passed, 0 if passed else 1, None
        return passed, frames[1].item(), frames[2]

    @staticmethod
    def dataframe_message(dataset: str, check: str, failures: int, examples: List[Any]) -> str:
        """The message of a failed dataframe-level check, as ``validations.dataframe_failures`` writes it."""
        return f"{dataset}: {failures} failure(s) of {check}, for example {examples}"

    @classmethod
    def dataframe_checks(cls, dataset: str, frame: pl.LazyFrame, columns: Sequence[str], dataframe_validator: Any) -> List[Tuple[str, "PolarsValidations.Pending"]]:
        """The dataframe-level part of a polars model (``@pa.dataframe_check``, ``Config.unique``, ``Config.ordered``), with the Pandera check name of each.

        The checks run as lazy queries, so they join the ``collect_all`` of the column validators. The names are the Pandera check names of pandas
        (``multiple_fields_uniqueness``, ``column_ordered``). Two checks can have one name: each is kept, and the engine joins their failures. The count of
        a dataframe check is its failing rows, and the examples are the values of the first failing rows.
        """
        checks = [(str(check.name), cls.dataframe_check(dataset, frame, check)) for check in dataframe_validator.checks]
        if dataframe_validator.unique:
            checks.append(("multiple_fields_uniqueness", cls.unique_rows(dataset, frame, cls.unique_groups(dataframe_validator.unique), dataframe_validator.report_duplicates)))
        if dataframe_validator.ordered:
            checks.append(("column_ordered", cls.ordered(dataset, columns, list(dataframe_validator.columns))))
        return checks

    @staticmethod
    def unique_groups(unique: Any) -> List[List[str]]:
        """The column groups of ``Config.unique``: a name or a list of names is one group, and a list of lists is a list of groups."""
        if isinstance(unique, str):
            return [[unique]]
        if all(isinstance(name, str) for name in unique):
            return [list(unique)]
        return [[group] if isinstance(group, str) else list(group) for group in unique]

    @classmethod
    def dataframe_check(cls, dataset: str, frame: pl.LazyFrame, check: Any) -> "PolarsValidations.Pending":
        """A ``@pa.dataframe_check``: the failing rows, without the ``check_output`` column of Pandera."""
        result = check(frame)
        failure_cases = None if result.failure_cases is None else result.failure_cases.select(pl.exclude("check_output"))

        def finish(frames: List[pl.DataFrame]) -> ValidationResult:
            passed, failing, first = cls.check_frames(frames)
            if passed:
                return ValidationResult(valid=True, message=f"{dataset} passes {check.name}", value=0)
            examples = [value for row in first.head(5).rows() for value in row][:5] if first is not None else [False]
            message = cls.dataframe_message(dataset, str(check.name), failing, examples)
            if check.raise_warning:
                # As `DataFrameSchema.validate` in pandas: a check with `raise_warning=True` warns and does not fail the validation
                warnings.warn(message, SchemaWarning)
                return ValidationResult(valid=True, message=message, value=failing)
            return ValidationResult(valid=False, message=message, value=failing)

        return PolarsValidations.Pending(cls.check_queries(result, failure_cases), finish)

    @classmethod
    def unique_rows(cls, dataset: str, frame: pl.LazyFrame, groups: List[List[str]], report_duplicates: str) -> "PolarsValidations.Pending":
        """``Config.unique``: the rows whose values in the columns of a group repeat. As in pandas, the count is of cells and the examples go column by column.

        Each group is a query. As in pandas, the result is the failure of the first group with repeated rows.
        """

        def duplicated_rows(group: List[str]) -> pl.LazyFrame:
            key = pl.struct(group)
            duplicated = {"exclude_first": key.is_duplicated() & ~key.is_first_distinct(), "exclude_last": key.is_duplicated() & ~key.is_last_distinct()}.get(
                report_duplicates, key.is_duplicated()
            )
            return frame.select(group).filter(duplicated)

        def finish(frames: List[pl.DataFrame]) -> ValidationResult:
            for group, failing in zip(groups, frames):
                if failing.height > 0:
                    examples = [value for name in group for value in failing[name].head(5).to_list()][:5]
                    cells = failing.height * len(group)
                    return ValidationResult(valid=False, message=cls.dataframe_message(dataset, "multiple_fields_uniqueness", cells, examples), value=cells)
            return ValidationResult(valid=True, message=f"{dataset} has unique rows in {groups}", value=0)

        return PolarsValidations.Pending(tuple(duplicated_rows(group) for group in groups), finish)

    @classmethod
    def ordered(cls, dataset: str, columns: Sequence[str], expected: List[str]) -> "PolarsValidations.Pending":
        """``Config.ordered``: the columns of the model in the frame are in the order of the model. It reads no data."""
        present = [name for name in columns if name in expected]
        in_order = [name for name in expected if name in present]
        out_of_order = [name for name, wanted in zip(present, in_order) if name != wanted]

        def finish(_: List[pl.DataFrame]) -> ValidationResult:
            if not out_of_order:
                return ValidationResult(valid=True, message=f"{dataset} has its columns in order", value=0)
            return ValidationResult(valid=False, message=cls.dataframe_message(dataset, "column_ordered", len(out_of_order), out_of_order[:5]), value=len(out_of_order))

        return PolarsValidations.Pending((), finish)


PolarsValidations.ALL.update({name: getattr(PolarsValidations, name) for name in PolarsValidations.BUILT_IN})
