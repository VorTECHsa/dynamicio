"""The lazy writes of the polars engine: ``sink()`` of a ``LazyFrame`` to a ``local``, an ``s3_file`` or an ``s3_path_prefix`` resource.

A sink runs the write, the validations and the metrics of the model in one ``pl.collect_all`` (change 11 of ADR 0010). The order of decision 12 still
applies to what the caller sees:

- The write goes to a temporary file first. The destination changes only after the validations pass, so a failed validation leaves the old file.
- The metrics are logged only after the validations pass, as ``write()`` logs them.

A parquet file on a ``local`` or ``s3_file`` resource streams: polars writes the rows to a temporary file, and pyarrow then copies its row groups to a
file with the arrow types, the pandas metadata and the write options of ``write()``. The file then gives the same frame to the pandas engine. A CSV or
JSON file and an ``s3_path_prefix`` resource collect the frame and go to the writer of ``write()``, so the bytes and the objects are the same.
"""

import os
import tempfile
from typing import TYPE_CHECKING, Any, Dict, List, MutableMapping

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from dynamicio.engines.polars_checks import PolarsChecks
from dynamicio.engines.polars_files import PolarsFiles
from dynamicio.engines.polars_parquet import PolarsParquet
from dynamicio.mixins.utils import get_file_type_value

if TYPE_CHECKING:  # pragma: no cover
    from dynamicio.core import DynamicDataIO
    from dynamicio.engines.polars_engine import PolarsEngine


class PolarsSinks:
    """The ``sink()`` of the file resources that the polars engine writes."""

    SOURCES = ("local", "s3_file", "s3_path_prefix")
    # The row group size of `pyarrow.parquet.write_table` when the resource sets none, and its largest row group size
    ROW_GROUP_ROWS = 1024 * 1024
    MAX_ROW_GROUP_ROWS = 64 * 1024 * 1024
    # The temporary file is read once, on the same host, so a fast codec is enough
    STAGE_COMPRESSION = "lz4"
    STAGE_PREFIX = ".dynamicio-sink-"

    @classmethod
    def sink(cls, engine: "PolarsEngine", io: "DynamicDataIO", frame: Any) -> None:
        """Write a frame with the casts, the validations and the metrics of the model in one ``collect_all``.

        Raises:
            ColumnsDataTypeError: A schema column is not in the frame, or a cast in the plan failed when the sink collected it.
            SchemaValidationError: A validation failed. The destination does not change.
            ValueError: The S3 key of a parquet file ends in ``/``, as on ``write()``.
        """
        lazy = engine.cast(io, engine.schema_columns(io, frame.lazy()))
        source, section = PolarsFiles.source_of(io), PolarsFiles.section_of(io)
        options = dict(io.options)
        if get_file_type_value(section.file_type) != "parquet" or source == "s3_path_prefix":
            collected = cls.collect(engine, io, lazy, [lazy])
            PolarsFiles.write(io, collected[0])
            return
        cls.sink_parquet(engine, io, lazy, options)

    @classmethod
    def sink_parquet(cls, engine: "PolarsEngine", io: "DynamicDataIO", lazy: pl.LazyFrame, options: MutableMapping[str, Any]) -> None:
        """Sink a parquet file of a ``local`` or ``s3_file`` resource to a temporary file, then move it into place or upload it."""
        source, target = PolarsFiles.source_of(io), PolarsFiles.target_of(io, options)
        # A local file is moved into place, so its temporary file is in the same directory, on the same file system
        directory = os.path.dirname(os.path.abspath(target)) if source == "local" else None
        with tempfile.TemporaryDirectory(prefix=cls.STAGE_PREFIX, dir=directory) as stage:
            staged, final = os.path.join(stage, "staged.parquet"), os.path.join(stage, "final.parquet")
            cls.rewrite(staged, final, cls.stage(engine, io, lazy, staged, options), options)
            if source == "local":
                os.replace(final, target)
                return
            PolarsFiles.upload(final, PolarsFiles.section_of(io).bucket, target)

    @classmethod
    def stage(cls, engine: "PolarsEngine", io: "DynamicDataIO", lazy: pl.LazyFrame, staged: str, options: MutableMapping[str, Any]) -> pa.Schema:
        """Sink the rows to the staged file, with the validations and the metrics in the same ``collect_all``, and return the arrow schema of the file.

        The same ``collect_all`` counts the rows, for the ``RangeIndex`` of the pandas metadata, and the categories of each ``Categorical`` column.
        """
        categorical = [column for column, dtype in lazy.collect_schema().items() if dtype == pl.Categorical]
        shape = lazy.select(pl.len(), *[pl.col(column).drop_nulls().n_unique() for column in categorical])
        collected = cls.collect(engine, io, lazy, [lazy.sink_parquet(staged, compression=cls.STAGE_COMPRESSION, lazy=True), shape])
        height, *counts = collected[1].row(0)
        return PolarsParquet.arrow_schema(io, lazy.collect_schema(), height, dict(zip(categorical, counts)), index=options.get("index") is not False)

    @classmethod
    def collect(cls, engine: "PolarsEngine", io: "DynamicDataIO", lazy: pl.LazyFrame, queries: List[pl.LazyFrame]) -> List[pl.DataFrame]:
        """Collect the queries of a sink with the validations and the metrics of the model, then raise the failed validations and log the metrics.

        Returns:
            The collected frames of ``queries``, in their order.

        Raises:
            ColumnsDataTypeError: A cast in the plan failed.
            SchemaValidationError: A validation failed. The metrics are then not logged.
        """
        with PolarsChecks.failed_casts(io, "sink"):
            pending = engine.pending_validations(io, lazy) if io.apply_schema_validations else []
            validations = PolarsChecks.validation_queries(pending)
            metrics = engine.metrics_query(io, lazy) if io.log_schema_metrics else None
            collected = pl.collect_all([*queries, *validations, *([metrics[1]] if metrics else [])])
        PolarsChecks.raise_failed_validations(pending, collected[len(queries) : len(queries) + len(validations)])
        if metrics:
            PolarsChecks.log_metric_values(io, metrics[0], collected[-1].row(0), lazy.collect_schema())
        return collected[: len(queries)]

    @classmethod
    def rewrite(cls, staged: str, final: str, schema: pa.Schema, options: MutableMapping[str, Any]) -> None:
        """Copy the rows of the staged file to a parquet file with the arrow types, the pandas metadata and the write options of ``write()``.

        The rows go in row groups of ``row_group_size`` rows, as ``pyarrow.parquet.write_table`` cuts a table, so only one row group is in memory at a time.
        A frame without rows gives one row group without rows, as ``write_table`` writes it.
        """
        arguments: Dict[str, Any] = PolarsParquet.parquet_arguments(options)
        rows = min(arguments.pop("row_group_size", None) or cls.ROW_GROUP_ROWS, cls.MAX_ROW_GROUP_ROWS)
        with pq.ParquetWriter(final, schema, **arguments) as writer:
            batches: List[pa.RecordBatch] = []
            buffered, written = 0, False
            for batch in pq.ParquetFile(staged).iter_batches(batch_size=rows):
                batches.append(batch)
                buffered += batch.num_rows
                if buffered >= rows:
                    table = pa.Table.from_batches(batches)
                    full = buffered - buffered % rows
                    writer.write_table(PolarsParquet.to_schema(table.slice(0, full), schema), row_group_size=rows)
                    written = True
                    batches, buffered = table.slice(full).to_batches(), buffered - full
            if buffered or not written:
                rest = pa.Table.from_batches(batches, schema=pq.read_schema(staged)) if batches else schema.empty_table()
                writer.write_table(PolarsParquet.to_schema(rest, schema), row_group_size=rows)
