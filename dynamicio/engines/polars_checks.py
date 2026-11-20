"""The parts of a validation and of a metric step of the polars engine that a ``collect_all`` of ``PolarsEngine`` or of ``PolarsSinks`` shares."""

import contextlib
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Tuple

from dynamicio.errors import ColumnsDataTypeError, SchemaValidationError

if TYPE_CHECKING:  # pragma: no cover
    from dynamicio.core import DynamicDataIO


class PolarsChecks:
    """Collect the queries of the validations and of the metrics, raise the failed validations and log the metric values. It does not import polars."""

    @staticmethod
    @contextlib.contextmanager
    def failed_casts(io: "DynamicDataIO", step: str) -> Iterator[None]:
        """Raise ``ColumnsDataTypeError`` when a step collects a plan with a cast that fails, and log the polars error.

        Raises:
            ColumnsDataTypeError: A cast in the plan failed when the step collected it.
        """
        import polars as pl  # pylint: disable=import-outside-toplevel

        from dynamicio import core  # pylint: disable=import-outside-toplevel

        try:
            yield
        except (pl.exceptions.InvalidOperationError, pl.exceptions.ComputeError):
            core.logger.exception(f"The {step} of {io.name} could not collect the frame: a cast in its plan failed")
            raise ColumnsDataTypeError() from None

    @staticmethod
    def validation_queries(pending: List[Tuple[str, Any]]) -> List[Any]:
        """The lazy queries of the pending validations, in their order."""
        return [query for _, check in pending for query in check.queries]

    @staticmethod
    def raise_failed_validations(pending: List[Tuple[str, Any]], collected: List[Any]) -> None:
        """Raise one ``SchemaValidationError`` for the pending validations that failed, from the collected frames of their queries.

        Raises:
            SchemaValidationError: One or more validations failed. The messages of each validation name are joined with ``"; "``, as on the pandas engine.
        """
        failed_validations: Dict[str, List[str]] = {}
        start = 0
        for name, check in pending:
            result = check.finish(collected[start : start + len(check.queries)])
            start += len(check.queries)
            if not result.valid:
                failed_validations.setdefault(name, []).append(result.message)
        if failed_validations:
            raise SchemaValidationError({name: "; ".join(messages) for name, messages in failed_validations.items()})

    @staticmethod
    def log_metric_values(io: "DynamicDataIO", wanted: List[Tuple[str, str]], values: Tuple[Any, ...], dtypes: Any) -> None:
        """Log the value of each ``(column, metric)`` pair, in the order of the schema."""
        from dynamicio.schema.polars_metrics import PolarsMetrics  # pylint: disable=import-outside-toplevel

        for (column, metric), value in zip(wanted, values):
            PolarsMetrics.log(io.name, column, metric, value, dtypes.get(column))
