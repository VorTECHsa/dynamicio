"""The lazy reads of the polars engine: ``scan()`` of parquet, CSV and JSON files on a local disk, in a local batch, in one S3 object and under an S3 prefix.

A scan gives a ``LazyFrame`` with the schema columns. ``DynamicDataIO.scan()`` then adds the casts of the model to its plan. Parquet and CSV files go
to the polars scanners, so a filter or a selection of the caller reads only the row groups and the columns that it needs (ADR 0004). polars has no lazy
reader of a JSON array, so a JSON scan reads the file with the eager reader of the engine.

A parquet prefix is one ``scan_parquet`` of all its objects that keeps ADR 0002 (decision 13 of ADR 0010):

- The engine reads the footer of each object. The type of each column is the widest type of the footers, and the scan upcasts the narrower objects.
- An object that lacks a column gets nulls in it, with the warning of the pandas engine. A column that no object has raises ``ValueError``.
"""

import collections
import os
from typing import TYPE_CHECKING, Any, Dict, List, MutableMapping, Optional

import polars as pl

from dynamicio.engines.polars_files import PolarsFiles
from dynamicio.engines.polars_s3 import PolarsS3
from dynamicio.errors import ColumnsDataTypeError, NoObjectsFoundError
from dynamicio.mixins import utils, with_s3
from dynamicio.mixins.utils import get_file_type_value

if TYPE_CHECKING:  # pragma: no cover
    from dynamicio.core import DynamicDataIO


class PolarsScans:
    """The ``scan()`` of the file resources: ``local``, ``local_batch``, ``s3_file`` and ``s3_path_prefix``."""

    SOURCES = ("local", "local_batch", "s3_file", "s3_path_prefix")
    # The time units of a Datetime, from the coarsest to the finest
    TIME_UNITS = ("ms", "us", "ns")
    WIDE_INTEGERS = (pl.Int128, pl.UInt128)

    @classmethod
    def scan(cls, io: "DynamicDataIO") -> pl.LazyFrame:
        """The lazy frame of the file or the files of a resource, with the schema columns that the files have.

        Raises:
            FileNotFoundError: A local file does not exist.
            NoObjectsFoundError: No local file matches a ``local_batch`` resource, or no object is under an S3 prefix.
            S3KeyNotFoundError: The key of a parquet object does not exist.
            ObjectNotFoundError: The key of a CSV or JSON object does not exist.
            ValueError: No object under a parquet prefix has a column of the schema.
            ColumnsDataTypeError: The objects under a parquet prefix have types of a column that do not upcast to one type.
        """
        source, section = PolarsFiles.source_of(io), PolarsFiles.section_of(io)
        file_type = get_file_type_value(section.file_type)
        options = dict(io.options)
        if source == "s3_path_prefix":
            return cls.scan_prefix(io, section, file_type, options)
        if file_type == "json":
            return PolarsFiles.read(io).lazy()
        if source == "local_batch":
            files = io._local_batch_files(options)  # pylint: disable=protected-access
            return pl.concat([cls.scan_file(file_type, file, io, options) for file in files], how="diagonal_relaxed")
        path = utils.resolve_template(section.file_path, options)
        if source == "local":
            if not os.path.exists(path):
                raise FileNotFoundError(f"No such file: {path}")
            return cls.scan_file(file_type, path, io, options)
        if path.endswith("/"):
            raise NotImplementedError(f"the polars engine does not read the objects under an S3 prefix yet: s3://{section.bucket}/{path}")
        PolarsS3.check_key(section.bucket, path, file_type)
        return cls.scan_file(file_type, f"s3://{section.bucket}/{path}", io, options, PolarsS3.arguments())

    @staticmethod
    def scan_file(file_type: str, path: str, io: "DynamicDataIO", options: MutableMapping[str, Any], arguments: Optional[Dict[str, Any]] = None) -> pl.LazyFrame:
        """The lazy frame of one parquet or CSV file, with the schema columns of the file, as the eager reader of the engine selects them."""
        arguments = arguments or {}
        if file_type == "parquet":
            frame = pl.scan_parquet(path, **arguments)
            names = frame.collect_schema().names()
            requested = options.get("columns")
            columns = requested if isinstance(requested, list) else list(io.schema.column_names)
            return frame.select([column for column in columns if column in names])
        return PolarsFiles.schema_columns(pl.scan_csv(path, **PolarsFiles.csv_arguments(io, options), **arguments), io)

    @classmethod
    def scan_prefix(cls, io: "DynamicDataIO", section: Any, file_type: str, options: MutableMapping[str, Any]) -> pl.LazyFrame:
        """The lazy frame of the objects under an S3 prefix, or under the prefix of a ``dynamic_file_path`` with its pattern, in listing order.

        A CSV or JSON object is read with the eager reader of the engine, as the pandas engine reads each object, and the frames are concatenated.
        """
        options.pop("no_disk_space", None)  # it changes only the hdf reads of the pandas engine
        if section.dynamic_file_path:
            url = utils.resolve_template(f"s3://{section.bucket}/{section.dynamic_file_path}", options)
            prefix, suffix = url.rsplit("/**/", 1)
            pattern: Optional[str] = f"**/{suffix}"
        else:
            url = prefix = utils.resolve_template(f"s3://{section.bucket}/{section.path_prefix}", options)
            pattern = None
        if file_type != "parquet":
            frames = with_s3.s3_read_down(prefix, lambda fobj: PolarsFiles.decode(file_type, fobj, io, options), include_pattern=pattern)
            if not frames:
                raise NoObjectsFoundError(f"No objects found under {url}")
            return pl.concat([frame for frame in frames if frame.height] or frames, how="diagonal_relaxed").lazy()
        keys = PolarsS3.keys(prefix, pattern)
        if not keys:
            raise NoObjectsFoundError(f"No objects found under {url}")
        with_s3.logger.info(f"[s3] Scanning {len(keys)} objects under {url}")
        return cls.scan_objects(io, [f"s3://{section.bucket}/{key}" for key in keys], url, options)

    @classmethod
    def scan_objects(cls, io: "DynamicDataIO", urls: List[str], source: str, options: MutableMapping[str, Any]) -> pl.LazyFrame:
        """One ``scan_parquet`` of the objects of a prefix, with an explicit schema from their footers (ADR 0002 and decision 13 of ADR 0010).

        Raises:
            ValueError: No object has a column of the schema.
            ColumnsDataTypeError: The footers have types of a column that do not upcast to one type.
        """
        footers = PolarsS3.footers(urls)
        requested = list(options.get("columns") or io.schema.column_names)
        absent = collections.Counter(column for footer in footers for column in requested if column not in footer)
        if absent:
            with_s3.logger.warning(f"[s3] Some objects under {source} lack columns that are filled with nulls: {dict(sorted(absent.items()))} (objects lacking each column)")
        never_present = [column for column in requested if absent[column] == len(footers)]
        if never_present:
            raise ValueError(f"Columns {never_present} are missing from every parquet object under {source}")
        schema = {column: cls.widest(io, column, [footer[column] for footer in footers if column in footer]) for column in requested}
        cast_options = pl.ScanCastOptions(integer_cast=["upcast", "allow-float"], float_cast="upcast", datetime_cast="upcast", categorical_to_string="allow")
        return pl.scan_parquet(urls, schema=schema, missing_columns="insert", extra_columns="ignore", cast_options=cast_options, hive_partitioning=False, **PolarsS3.arguments())

    @classmethod
    def widest(cls, io: "DynamicDataIO", column: str, dtypes: List[Any]) -> Any:
        """The type of a column in the footers of several objects, as ``pd.concat`` gives it for their frames.

        - A ``Null`` column, which pyarrow writes for a column of nulls only, takes the type of the other objects.
        - Integers give the integer type that holds them all, and integers and floats give ``Float64``.
        - ``String`` and ``Categorical`` give ``String``.
        - ``Datetime`` columns with one time zone give the finest time unit.

        Raises:
            ColumnsDataTypeError: The types do not upcast to one type, for example ``String`` and ``Int64``, or ``Int64`` and ``UInt64``.
        """
        kinds = list(dict.fromkeys(dtype for dtype in dtypes if dtype != pl.Null))
        if not kinds:
            return pl.Null
        if len(kinds) == 1:
            return kinds[0]
        if all(kind.is_integer() for kind in kinds):
            widest = pl.concat([pl.DataFrame(schema={column: kind}) for kind in kinds], how="vertical_relaxed").schema[column]
            if widest not in cls.WIDE_INTEGERS:
                return widest
        elif all(kind.is_numeric() for kind in kinds):
            return pl.Float64
        elif all(kind in (pl.String, pl.Categorical) for kind in kinds):
            return pl.String
        elif all(isinstance(kind, pl.Datetime) for kind in kinds) and len({kind.time_zone for kind in kinds}) == 1:
            unit = max((kind.time_unit for kind in kinds), key=cls.TIME_UNITS.index)
            return pl.Datetime(unit, kinds[0].time_zone)
        from dynamicio import core  # pylint: disable=import-outside-toplevel

        core.logger.error(f"The objects of {io.name} have the types {[str(kind) for kind in kinds]} for the column '{column}', which do not upcast to one type")
        raise ColumnsDataTypeError()
