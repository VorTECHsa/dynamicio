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
import stat
import tempfile
from typing import TYPE_CHECKING, Any, Dict, List, MutableMapping

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from dynamicio.engines.engine import Engine
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
        """Write a frame, or the parts of a frame, with the casts, the validations and the metrics of the model in one ``collect_all``.

        A list or a tuple of frames is the parts of one frame, in their order. Each part gets the schema columns and the casts of the model, and the
        sink then concatenates them. The parts can thus have other types of a column, for example ``Int32`` and ``Int64``, or a column of nulls only.
        The pandas metadata of the file comes from the model, so it has the types of the model and a ``RangeIndex`` of all the rows.

        Raises:
            ColumnsDataTypeError: A schema column is not in a part, or a cast in the plan failed when the sink collected it.
            SchemaValidationError: A validation failed. The destination does not change.
            ValueError: The S3 key of a parquet file ends in ``/``, as on ``write()``, or a list of parts is empty.
        """
        lazy = cls.combined(engine, io, frame)
        source, section = Engine.source_of(io), PolarsFiles.section_of(io)
        options = dict(io.options)
        if get_file_type_value(section.file_type) != "parquet" or source == "s3_path_prefix":
            collected = cls.collect(engine, io, lazy, [lazy])
            PolarsFiles.write(io, collected[0])
            return
        cls.sink_parquet(engine, io, lazy, options)

    @classmethod
    def combined(cls, engine: "PolarsEngine", io: "DynamicDataIO", frame: Any) -> pl.LazyFrame:
        """The lazy frame of a frame, or the concatenation of the parts of a frame, each with the schema columns and the casts of the model."""
        parts = list(frame) if isinstance(frame, (list, tuple)) else [frame]
        if not parts:
            raise ValueError(f"sink() of {io.name!r} got no parts")
        cast = [engine.cast(io, engine.schema_columns(io, part.lazy())) for part in parts]
        if len(cast) == 1:
            return cast[0]
        # A vertical concat needs the columns in the same positions, and each part keeps the column order of its input
        order = cast[0].collect_schema().names()
        return pl.concat([cast[0], *(part.select(order) for part in cast[1:])], how="vertical")

    @classmethod
    def sink_parquet(cls, engine: "PolarsEngine", io: "DynamicDataIO", lazy: pl.LazyFrame, options: MutableMapping[str, Any]) -> None:
        """Sink a parquet file of a ``local`` or ``s3_file`` resource to a temporary file, then move it into place or upload it."""
        source, target = Engine.source_of(io), PolarsFiles.target_of(io, options)
        # A local file is moved into place, so its temporary file is in the same directory, on the same file system
        directory = os.path.dirname(os.path.abspath(target)) if source == "local" else None
        with tempfile.TemporaryDirectory(prefix=cls.STAGE_PREFIX, dir=directory) as stage:
            staged, final = os.path.join(stage, "staged.parquet"), os.path.join(stage, "final.parquet")
            cls.rewrite(staged, final, cls.stage(engine, io, lazy, staged, options), options)
            if source == "local":
                cls.keep_mode(final, target)
                os.replace(final, target)
                return
            PolarsFiles.upload(final, PolarsFiles.section_of(io).bucket, target)

    @staticmethod
    def keep_mode(final: str, target: str) -> None:
        """Give the new file the mode of the file that it replaces. ``write()`` opens the old file and keeps its mode, so a file of the owner only stays so.
        A new file has the mode of the umask, as a file of ``write()``."""
        try:
            mode = stat.S_IMODE(os.stat(target).st_mode)
        except FileNotFoundError:
            return
        os.chmod(final, mode)

    @classmethod
    def stage(cls, engine: "PolarsEngine", io: "DynamicDataIO", lazy: pl.LazyFrame, staged: str, options: MutableMapping[str, Any]) -> pa.Schema:
        """Sink the rows to the staged file, with the validations and the metrics in the same ``collect_all``, and return the arrow schema of the file.

        The same ``collect_all`` counts the rows, for the ``RangeIndex`` of the pandas metadata, the categories of each ``Categorical`` column, and the
        nulls of each column, for the nullable pandas dtype of an integer or a boolean column and the ``null`` type of an ``object`` column of nulls.
        """
        schema = lazy.collect_schema()
        categorical = [column for column, dtype in schema.items() if dtype == pl.Categorical]
        # The nulls of an integer or a boolean column give its pandas dtype, and a column of nulls only has the `null` type if its pandas dtype is `object`
        nullable = list(schema.names())
        # Each aggregate has its own alias, so a column of the model cannot have the name of another aggregate
        shape = lazy.select(
            pl.len().alias("rows"),
            *[pl.col(column).drop_nulls().n_unique().alias(f"categories_{number}") for number, column in enumerate(categorical)],
            *[pl.col(column).null_count().alias(f"nulls_{number}") for number, column in enumerate(nullable)],
        )
        collected = cls.collect(engine, io, lazy, [lazy.sink_parquet(staged, compression=cls.STAGE_COMPRESSION, lazy=True), shape])
        height, *aggregates = collected[1].row(0)
        null_counts = dict(zip(nullable, aggregates[len(categorical) :]))
        return PolarsParquet.arrow_schema(io, schema, height, dict(zip(categorical, aggregates)), options, null_counts=null_counts)

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

        The staged file is read without ``pre_buffer``. With it, ``iter_batches`` kept the compressed column chunks of the row groups that it had read, so
        the memory grew with the size of the file: 2.8 GiB of arrow memory for a staged file of 12M rows, against 0.9 GiB without it (pyarrow 25.0.1).
        """
        arguments: Dict[str, Any] = PolarsParquet.parquet_arguments(options)
        rows = min(arguments.pop("row_group_size", None) or cls.ROW_GROUP_ROWS, cls.MAX_ROW_GROUP_ROWS)
        with pq.ParquetWriter(final, schema, **arguments) as writer:
            batches: List[pa.RecordBatch] = []
            buffered, written = 0, False
            for batch in pq.ParquetFile(staged, pre_buffer=False).iter_batches(batch_size=rows):
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
