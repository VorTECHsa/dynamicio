"""The eager file I/O of the polars engine: parquet, CSV and JSON files on a local disk, in a local batch and in one S3 object. ``PolarsScans`` reads
an S3 prefix.

A file that the polars engine writes gives the same frame to the pandas engine of 11.1.0 (rule 2 of ADR 0010):

- The CSV and JSON writers change the frame to pandas, with the pandas dtypes of the model, and call the pandas writers of 11.1.0. The bytes are then equal.
- The parquet writer calls ``pyarrow.parquet.write_table``, as ``DataFrame.to_parquet`` does, with the pandas metadata of the model. A pandas reader
  then gives the pandas dtypes of the model, for example ``Int64`` and ``boolean`` columns with nulls.

The readers are polars readers. The engine casts their frames to the model after the read, as for each read. The module needs polars, so only the polars
engine imports it.
"""

import io as std_io
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, MutableMapping, Union

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from pandas._libs.parsers import STR_NA_VALUES  # pylint: disable=no-name-in-module

from dynamicio.engines.engine import Engine
from dynamicio.engines.polars_options import PolarsOptions
from dynamicio.engines.polars_parquet import PolarsParquet
from dynamicio.engines.polars_s3 import PolarsS3
from dynamicio.errors import EngineResourceError
from dynamicio.formats.json.index import JsonIndex
from dynamicio.mixins import utils, with_s3
from dynamicio.mixins.utils import get_file_type_value
from dynamicio.schema.polars_dtypes import DtypeMap
from dynamicio.transport.s3.upload import S3Upload

if TYPE_CHECKING:  # pragma: no cover
    from dynamicio.core import DynamicDataIO

Source = Union[str, std_io.BytesIO]
Frame = Union[pl.DataFrame, pl.LazyFrame]


class PolarsFiles:
    """The polars readers and writers of the file resources: ``local``, ``local_batch`` and ``s3_file``."""

    SOURCES = ("local", "local_batch", "s3_file")
    # The resources that the engine checks and reads: an `s3_path_prefix` read is a scan of its objects
    READ_SOURCES = (*SOURCES, "s3_path_prefix")
    DURATION_UNITS = {"s": "seconds", "ms": "milliseconds", "us": "microseconds", "ns": "nanoseconds"}
    # The pandas nullable dtype of each arrow integer type. The default conversion gives float64 to an integer column with nulls, which loses digits above 2**53
    NULLABLE_INTEGERS = {
        pa.int8(): pd.Int8Dtype(),
        pa.int16(): pd.Int16Dtype(),
        pa.int32(): pd.Int32Dtype(),
        pa.int64(): pd.Int64Dtype(),
        pa.uint8(): pd.UInt8Dtype(),
        pa.uint16(): pd.UInt16Dtype(),
        pa.uint32(): pd.UInt32Dtype(),
        pa.uint64(): pd.UInt64Dtype(),
    }

    @classmethod
    def section_of(cls, io: "DynamicDataIO") -> Any:
        """The ``local`` or ``s3`` section of the resource definition of an IO object."""
        return io.sources_config.s3 if Engine.source_of(io) in ("s3_file", "s3_path_prefix") else io.sources_config.local

    @classmethod
    def paths_of(cls, io: "DynamicDataIO") -> List[str]:
        """The path templates of a resource: their fields are options of the path, not options of the reader or the writer."""
        section = cls.section_of(io)
        return [path for path in (getattr(section, name, None) for name in ("file_path", "path_prefix", "dynamic_file_path")) if path]

    @classmethod
    def check(cls, io: "DynamicDataIO") -> None:
        """Refuse the file type and the options of a file resource that the polars engine cannot read or write. Other resources are not checked.

        Raises:
            EngineResourceError: The file type or an option has no polars reader and writer, or the ``index_column`` of a JSON resource is not a column of
                the model.
        """
        source = Engine.source_of(io)
        if source not in cls.READ_SOURCES:
            return
        fields = {field for path in cls.paths_of(io) for field in utils.get_string_template_field_names(path)}
        options = {name: value for name, value in io.options.items() if name not in fields}
        file_type = get_file_type_value(cls.section_of(io).file_type)
        PolarsOptions.check(file_type, source, options)
        cls.check_lists(io, file_type)
        index_column = options.get("index_column")
        if index_column is not None and index_column not in io.schema.columns:
            raise EngineResourceError(f"the index_column {index_column!r} of {io.name} is not a column of its model: the columns are {list(io.schema.columns)}")

    @staticmethod
    def check_lists(io: "DynamicDataIO", file_type: str) -> None:
        """Refuse a CSV resource of a model with a ``List`` column: a CSV cell holds text, and the pandas writer gives the ``repr`` of the list.

        Raises:
            EngineResourceError: The model has a ``List`` column and the file type is CSV.
        """
        if file_type != "csv" or io._model is None:  # pylint: disable=protected-access
            return
        lists = [name for name, dtype in DtypeMap.of_model(io._model).items() if isinstance(dtype, pl.List)]  # pylint: disable=protected-access
        if lists:
            raise EngineResourceError(f"a CSV file cannot hold the List columns {lists} of {io.name}. Use a parquet or a JSON file.")

    @classmethod
    def read(cls, io: "DynamicDataIO") -> pl.DataFrame:
        """Read the file or the files of a resource into one frame with the schema columns that the files have.

        Raises:
            FileNotFoundError: A local file does not exist.
            NoObjectsFoundError: No local file matches a ``local_batch`` resource.
            S3KeyNotFoundError: The key of a parquet object does not exist.
            ObjectNotFoundError: The key of a CSV or JSON object does not exist.
        """
        source, section = Engine.source_of(io), cls.section_of(io)
        file_type = get_file_type_value(section.file_type)
        options = dict(io.options)
        if source == "local_batch":
            files = io._local_batch_files(options)  # pylint: disable=protected-access
            return pl.concat([cls.decode(file_type, file, io, options) for file in files], how="diagonal_relaxed")
        path = utils.resolve_template(section.file_path, options)
        if source == "local":
            return cls.decode(file_type, path, io, options)
        return cls.decode(file_type, cls.download(section.bucket, path, file_type), io, options)

    @staticmethod
    def download(bucket: str, key: str, file_type: str) -> std_io.BytesIO:
        """Download one S3 object into memory, and raise the dynamicio error of the pandas engine for a missing key."""
        s3_path = f"s3://{bucket}/{key}"
        if key.endswith("/"):
            raise NotImplementedError(f"the polars engine does not read the objects under an S3 prefix yet: {s3_path}")
        with_s3.logger.info(f"[s3] Started downloading: {s3_path}")
        try:
            return with_s3._download_to_memory(bucket, key)  # pylint: disable=protected-access
        except ClientError as error:
            PolarsS3.raise_missing(error, s3_path, file_type)

    @classmethod
    def decode(cls, file_type: str, source: Source, io: "DynamicDataIO", options: MutableMapping[str, Any]) -> pl.DataFrame:
        """Read one file, or one object in memory, with the polars reader of its file type."""
        return getattr(cls, f"read_{file_type}")(source, io, options)

    @staticmethod
    def schema_columns(frame: Frame, io: "DynamicDataIO") -> Frame:
        """The columns of a frame or a lazy frame that the schema declares, in the order of the file, as ``usecols`` gives them on pandas."""
        return frame.select([column for column in frame.collect_schema().names() if column in io.schema.column_names])

    @staticmethod
    def read_parquet(source: Source, io: "DynamicDataIO", options: MutableMapping[str, Any]) -> pl.DataFrame:
        """Read the schema columns of a parquet file, in the order of the ``columns`` option or of the schema, as on pandas."""
        names = pq.read_schema(source).names
        if isinstance(source, std_io.BytesIO):
            source.seek(0)
        requested = options.get("columns")
        columns = requested if isinstance(requested, list) else list(io.schema.column_names)
        return pl.read_parquet(source, columns=[column for column in columns if column in names])

    @classmethod
    def read_csv(cls, source: Source, io: "DynamicDataIO", options: MutableMapping[str, Any]) -> pl.DataFrame:
        """Read a CSV file with the arguments of ``csv_arguments``."""
        return cls.schema_columns(pl.read_csv(source, **cls.csv_arguments(io, options)), io)

    @staticmethod
    def csv_arguments(io: "DynamicDataIO", options: MutableMapping[str, Any]) -> Dict[str, Any]:
        """The arguments of ``read_csv`` and ``scan_csv``: the null strings of ``pandas.read_csv``, and the columns that are not numbers or booleans as strings.

        ``names`` gives the names of the columns of the file, as on pandas: without ``header``, the first line is data, and ``header: 0`` replaces the
        names of the header line. polars raises ``SchemaError`` if the file has
        another number of columns. pandas then makes an index of the first columns, or adds columns of nulls.

        A string column keeps its text, for example ``007``, and the cast then parses the temporal columns. The schema of the numbers comes from all rows,
        so a float on the last row of an integer-looking column does not fail the read. A scan therefore reads the file once for the schema.
        """
        arguments = PolarsOptions.read_arguments("csv", options)
        if "new_columns" in arguments and options.get("header", "infer") == "infer":
            # pandas reads the first line as data when `names` is set without `header`
            arguments["has_header"] = False
        if not arguments.pop("has_header", True):
            if "new_columns" not in arguments:
                # pandas cannot read it either: `usecols` names columns that a headerless file without `names` does not have
                raise EngineResourceError(
                    "the polars engine reads a CSV file without a header line only with the names option, as the pandas engine does. "
                    "Set names to one name for each column of the file, or remove header: false from the read resource."
                )
            arguments["has_header"] = False
        extra = arguments.pop("null_values", None)
        extra = [extra] if isinstance(extra, str) else list(extra or [])
        dtypes = DtypeMap.of_model(io._model)  # pylint: disable=protected-access
        overrides = {column: pl.String for column, dtype in dtypes.items() if not (dtype.is_numeric() or dtype == pl.Boolean)}
        return {**arguments, "schema_overrides": overrides, "infer_schema_length": None, "null_values": sorted(STR_NA_VALUES) + extra}

    @classmethod
    def read_json(cls, source: Source, io: "DynamicDataIO", options: MutableMapping[str, Any]) -> pl.DataFrame:
        """Read a JSON array of records, or JSON lines with ``lines: true``. An integer under a temporal column of the model is an epoch, as pandas writes it.

        A JSON object with orient index or columns becomes records, with its keys in the column ``index_column``. A key is a string, so the engine casts
        the keys to a numeric dtype of the model, as ``pandas.read_json`` converts the keys of the index. A file without records, as ``DataFrame.to_json``
        writes a frame without rows, is a frame without rows that has the columns of the model. polars decompresses a file of records itself, and the
        engine decompresses a JSON object with the ``compression`` option or the suffix of the file, as ``DataFrame.to_json`` compresses it.
        """
        data = source.getvalue() if isinstance(source, std_io.BytesIO) else Path(source).read_bytes()
        dtypes = DtypeMap.of_model(io._model)  # pylint: disable=protected-access
        if data.strip() in (b"", b"[]", b"{}"):
            return pl.DataFrame(schema=dtypes)
        index_column = options.get("index_column")
        if index_column is not None:
            data = JsonIndex.decompressed(data, cls.target_of(io, options), options.get("compression", "infer"))
            data = JsonIndex.records(data, PolarsOptions.json_orient(Engine.source_of(io), options), index_column)
            if data.strip() == b"[]":  # orient columns writes a frame without rows as an empty object for each column
                return pl.DataFrame(schema=dtypes)
        reader = pl.read_ndjson if options.get("lines", False) else pl.read_json
        frame = cls.schema_columns(reader(std_io.BytesIO(data), infer_schema_length=None), io)
        if index_column is not None and dtypes[index_column].is_numeric():
            frame = frame.with_columns(pl.col(index_column).cast(dtypes[index_column]))
        unit = options.get("date_unit", "ms")
        epochs = [
            cls.from_epoch(column, dtypes[column], unit) for column, dtype in frame.schema.items() if dtype.is_integer() and isinstance(dtypes[column], (pl.Datetime, pl.Duration))
        ]
        return frame.with_columns(epochs) if epochs else frame

    @classmethod
    def from_epoch(cls, column: str, expected: Any, unit: str) -> pl.Expr:
        """The ``Datetime`` or ``Duration`` of an integer that ``DataFrame.to_json`` writes for a timestamp or a timedelta, in ``date_unit``."""
        if isinstance(expected, pl.Duration):
            return pl.duration(**{cls.DURATION_UNITS[unit]: pl.col(column)}, time_unit="ns").alias(column)
        expression = pl.from_epoch(pl.col(column), time_unit=unit)
        if expected.time_zone is not None:  # an epoch is a UTC instant
            expression = expression.dt.replace_time_zone("UTC").dt.convert_time_zone(expected.time_zone)
        return expression.alias(column)

    @classmethod
    def write(cls, io: "DynamicDataIO", frame: pl.DataFrame) -> None:
        """Write a frame that has the polars dtypes of the model.

        A CSV or JSON frame, and the frame of an ``s3_path_prefix`` resource, go to the pandas writer of their source. The prefix writer of the pandas
        engine thus writes the same objects and deletes the same old objects for each ``mode``. A ``local_batch`` resource has no writer on either engine.
        """
        source, section = Engine.source_of(io), cls.section_of(io)
        file_type = get_file_type_value(section.file_type)
        if file_type != "parquet" or source in ("local_batch", "s3_path_prefix"):
            getattr(io, f"_write_to_{source}")(cls.to_pandas(io, frame))
            return
        options = dict(io.options)
        table = PolarsParquet.arrow_table(io, frame, options)
        target = cls.target_of(io, options)
        if source == "local":
            pq.write_table(table, target, **PolarsParquet.parquet_arguments(options))
            return
        buffer = std_io.BytesIO()
        pq.write_table(table, buffer, **PolarsParquet.parquet_arguments(options))
        cls.upload(buffer, section.bucket, target)

    @classmethod
    def target_of(cls, io: "DynamicDataIO", options: MutableMapping[str, Any]) -> str:
        """The local path or the S3 key of a ``local`` or ``s3_file`` resource.

        Raises:
            ValueError: The S3 key of a parquet file ends in ``/``, as on the pandas engine.
        """
        source, section = Engine.source_of(io), cls.section_of(io)
        path = utils.resolve_template(section.file_path, options)
        if source == "s3_file" and path.endswith("/"):
            raise ValueError("[s3-parquet] Parquet output path must be a file, not a directory (e.g., 's3://bucket/data.parquet').")
        return path

    @staticmethod
    def upload(source: Union[str, std_io.BytesIO], bucket: str, key: str) -> None:
        """Upload a parquet file or buffer to an S3 key, with the log lines of the pandas engine."""
        s3_path = f"s3://{bucket}/{key}"
        with_s3.logger.info(f"[s3] Started uploading: {s3_path}")
        S3Upload.upload(source, bucket, key)
        with_s3.logger.info(f"[s3] Finished uploading: {s3_path}")

    @staticmethod
    def pandas_dtypes(io: "DynamicDataIO", frame: pl.DataFrame) -> Dict[str, str]:
        """The pandas dtype of each column of a frame, from the model: the ``pandas_dtype`` metadata or the reverse map of decision 14 of ADR 0010.

        An integer or a boolean column with a null takes the nullable pandas dtype, as decision 14 states.
        """
        return {column: PolarsParquet.pandas_dtype(io, column, frame.get_column(column).null_count() > 0) for column in frame.columns}

    @classmethod
    def to_pandas(cls, io: "DynamicDataIO", frame: pl.DataFrame) -> pd.DataFrame:
        """The pandas frame of a polars frame, with the pandas dtypes of the model and a ``RangeIndex``, for the pandas writers.

        An integer column goes through a pandas nullable integer dtype, so a large integer next to a null keeps its value.
        """
        return frame.to_pandas(types_mapper=cls.NULLABLE_INTEGERS.get).astype(cls.pandas_dtypes(io, frame))

    @classmethod
    def to_pandas_chunks(cls, io: "DynamicDataIO", frame: pl.DataFrame, rows: int) -> Iterator[pd.DataFrame]:
        """The pandas frames of the chunks of ``rows`` rows of a frame, one at a time. A frame without rows gives one chunk without rows, with the columns."""
        for offset in range(0, max(frame.height, 1), rows):
            yield cls.to_pandas(io, frame.slice(offset, rows))
