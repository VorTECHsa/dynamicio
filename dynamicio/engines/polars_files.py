"""The eager file I/O of the polars engine: parquet, CSV and JSON files on a local disk, in a local batch and in one S3 object.

A file that the polars engine writes gives the same frame to the pandas engine of 11.1.0 (rule 2 of ADR 0010):

- The CSV and JSON writers change the frame to pandas, with the pandas dtypes of the model, and call the pandas writers of 11.1.0. The bytes are then equal.
- The parquet writer calls ``pyarrow.parquet.write_table``, as ``DataFrame.to_parquet`` does, with the pandas metadata of the model. A pandas reader
  then gives the pandas dtypes of the model, for example ``Int64`` and ``boolean`` columns with nulls.

The readers are polars readers. The engine casts their frames to the model after the read, as for each read. The module needs polars, so only the polars
engine imports it.
"""

import io as std_io
import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, MutableMapping, Tuple, Union

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from pandas._libs.parsers import STR_NA_VALUES  # pylint: disable=no-name-in-module

from dynamicio.engines.polars_options import PolarsOptions
from dynamicio.errors import EngineResourceError, ObjectNotFoundError, S3KeyNotFoundError
from dynamicio.mixins import utils, with_s3
from dynamicio.mixins.utils import get_file_type_value
from dynamicio.schema.polars_dtypes import DtypeMap

if TYPE_CHECKING:  # pragma: no cover
    from dynamicio.core import DynamicDataIO

Source = Union[str, std_io.BytesIO]


class PolarsFiles:
    """The polars readers and writers of the file resources: ``local``, ``local_batch`` and ``s3_file``."""

    SOURCES = ("local", "local_batch", "s3_file")
    # The codes of a pandas categorical: int8 below 127 categories, int16 below 32767 and int32 below 2**31 - 1, as `coerce_indexer_dtype` gives them
    CODE_TYPES = ((2**7 - 1, pa.int8()), (2**15 - 1, pa.int16()), (2**31 - 1, pa.int32()))
    # The pandas metadata of the columns index of a frame with string column names, as `DataFrame.to_parquet` writes it
    COLUMN_INDEXES = [{"name": None, "field_name": None, "pandas_type": "unicode", "numpy_type": "object", "metadata": {"encoding": "UTF-8"}}]
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

    @staticmethod
    def source_of(io: "DynamicDataIO") -> str:
        """The backend of an IO object."""
        return io.sources_config.data_backend_type.value

    @classmethod
    def section_of(cls, io: "DynamicDataIO") -> Any:
        """The ``local`` or ``s3`` section of the resource definition of an IO object."""
        return io.sources_config.s3 if cls.source_of(io) == "s3_file" else io.sources_config.local

    @classmethod
    def paths_of(cls, io: "DynamicDataIO") -> List[str]:
        """The path templates of a resource: their fields are options of the path, not options of the reader or the writer."""
        section = cls.section_of(io)
        return [path for path in (getattr(section, name, None) for name in ("file_path", "path_prefix", "dynamic_file_path")) if path]

    @classmethod
    def check(cls, io: "DynamicDataIO") -> None:
        """Refuse the file type and the options of a file resource that the polars engine cannot read or write. Other resources are not checked.

        Raises:
            EngineResourceError: The file type or an option has no polars reader and writer.
        """
        source = cls.source_of(io)
        if source not in cls.SOURCES:
            return
        fields = {field for path in cls.paths_of(io) for field in utils.get_string_template_field_names(path)}
        options = {name: value for name, value in io.options.items() if name not in fields}
        PolarsOptions.check(get_file_type_value(cls.section_of(io).file_type), source, options)

    @classmethod
    def read(cls, io: "DynamicDataIO") -> pl.DataFrame:
        """Read the file or the files of a resource into one frame with the schema columns that the files have.

        Raises:
            FileNotFoundError: A local file does not exist.
            NoObjectsFoundError: No local file matches a ``local_batch`` resource.
            S3KeyNotFoundError: The key of a parquet object does not exist.
            ObjectNotFoundError: The key of a CSV or JSON object does not exist.
        """
        source, section = cls.source_of(io), cls.section_of(io)
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
            if not S3KeyNotFoundError.is_missing_key(error):
                raise
            if file_type == "parquet":
                raise S3KeyNotFoundError(s3_path, error) from error
            raise ObjectNotFoundError(f"No object found at {s3_path}") from error

    @classmethod
    def decode(cls, file_type: str, source: Source, io: "DynamicDataIO", options: MutableMapping[str, Any]) -> pl.DataFrame:
        """Read one file, or one object in memory, with the polars reader of its file type."""
        return getattr(cls, f"read_{file_type}")(source, io, options)

    @staticmethod
    def schema_columns(frame: pl.DataFrame, io: "DynamicDataIO") -> pl.DataFrame:
        """The columns of a frame that the schema declares, in the order of the file, as ``usecols`` gives them on pandas."""
        return frame.select([column for column in frame.columns if column in io.schema.column_names])

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
        """Read a CSV file with the null strings of ``pandas.read_csv``, and the columns that are not numbers or booleans as strings.

        A string column keeps its text, for example ``007``, and the cast then parses the temporal columns. The schema of the numbers comes from all rows,
        so a float on the last row of an integer-looking column does not fail the read.
        """
        arguments = PolarsOptions.read_arguments("csv", options)
        if not arguments.pop("has_header", True):
            # pandas cannot read it either: `usecols` names columns that a headerless file does not have
            raise EngineResourceError("the polars engine reads CSV files with a header line, as the pandas engine does. Remove header: false from the read resource.")
        extra = arguments.pop("null_values", None)
        extra = [extra] if isinstance(extra, str) else list(extra or [])
        dtypes = DtypeMap.of_model(io._model)  # pylint: disable=protected-access
        overrides = {column: pl.String for column, dtype in dtypes.items() if not (dtype.is_numeric() or dtype == pl.Boolean)}
        frame = pl.read_csv(source, schema_overrides=overrides, infer_schema_length=None, null_values=sorted(STR_NA_VALUES) + extra, **arguments)
        return cls.schema_columns(frame, io)

    @classmethod
    def read_json(cls, source: Source, io: "DynamicDataIO", options: MutableMapping[str, Any]) -> pl.DataFrame:
        """Read a JSON array of records, or JSON lines with ``lines: true``. An integer under a temporal column of the model is an epoch, as pandas writes it.

        A file without records, as ``DataFrame.to_json`` writes a frame without rows, is a frame without rows that has the columns of the model.
        """
        data = source.getvalue() if isinstance(source, std_io.BytesIO) else Path(source).read_bytes()
        dtypes = DtypeMap.of_model(io._model)  # pylint: disable=protected-access
        if data.strip() in (b"", b"[]"):
            return pl.DataFrame(schema=dtypes)
        reader = pl.read_ndjson if options.get("lines", False) else pl.read_json
        frame = cls.schema_columns(reader(std_io.BytesIO(data), infer_schema_length=None), io)
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

        A CSV or JSON frame goes to the pandas writer of its source. A ``local_batch`` resource has no writer on either engine.
        """
        source, section = cls.source_of(io), cls.section_of(io)
        file_type = get_file_type_value(section.file_type)
        if file_type != "parquet" or source == "local_batch":
            getattr(io, f"_write_to_{source}")(cls.to_pandas(io, frame))
            return
        options = dict(io.options)
        path = utils.resolve_template(section.file_path, options)
        arguments = {name: value for name, value in PolarsOptions.write_options("parquet", options).items() if name not in ("engine", "index")}
        table = cls.arrow_table(io, frame, index=options.get("index") is not False)
        if source == "local":
            pq.write_table(table, path, **arguments)
            return
        if path.endswith("/"):
            raise ValueError("[s3-parquet] Parquet output path must be a file, not a directory (e.g., 's3://bucket/data.parquet').")
        s3_path = f"s3://{section.bucket}/{path}"
        with_s3.logger.info(f"[s3] Started uploading: {s3_path}")
        buffer = std_io.BytesIO()
        pq.write_table(table, buffer, **arguments)
        with_s3._upload_from_memory(buffer, section.bucket, path)  # pylint: disable=protected-access
        with_s3.logger.info(f"[s3] Finished uploading: {s3_path}")

    @staticmethod
    def pandas_dtypes(io: "DynamicDataIO", frame: pl.DataFrame) -> Dict[str, str]:
        """The pandas dtype of each column of a frame, from the model: the ``pandas_dtype`` metadata or the reverse map of decision 14 of ADR 0010."""
        return {column: io.schema.columns[column].data_type for column in frame.columns}

    @classmethod
    def to_pandas(cls, io: "DynamicDataIO", frame: pl.DataFrame) -> pd.DataFrame:
        """The pandas frame of a polars frame, with the pandas dtypes of the model and a ``RangeIndex``, for the pandas writers.

        An integer column goes through a pandas nullable integer dtype, so a large integer next to a null keeps its value.
        """
        return frame.to_pandas(types_mapper=cls.NULLABLE_INTEGERS.get).astype(cls.pandas_dtypes(io, frame))

    @classmethod
    def arrow_table(cls, io: "DynamicDataIO", frame: pl.DataFrame, index: bool = True) -> pa.Table:
        """The arrow table of a frame, with the arrow types and the pandas metadata that ``DataFrame.to_parquet`` gives for the pandas dtypes of the model.

        pyarrow makes the types and the metadata from an empty pandas frame with these dtypes. An empty ``object`` column has the ``null`` type there, and
        an empty ``category`` column has no categories, so the engine gives them their string types and the number of categories of the frame.

        ``index=False`` gives the metadata of ``DataFrame.to_parquet(index=False)``, without an index. Otherwise the metadata has the ``RangeIndex`` of
        the frame, as ``DataFrame.to_parquet`` writes by default.
        """
        empty = pd.DataFrame({column: pd.Series([], dtype=dtype) for column, dtype in cls.pandas_dtypes(io, frame).items()})
        schema = pa.Schema.from_pandas(empty, preserve_index=False)
        metadata = json.loads(schema.metadata[b"pandas"])
        if index:
            metadata["index_columns"] = [{"kind": "range", "name": None, "start": 0, "stop": frame.height, "step": 1}]
            metadata["column_indexes"] = cls.COLUMN_INDEXES
        fields = []
        for field, column in zip(schema, metadata["columns"]):
            kind = field.type
            if pa.types.is_null(kind):
                kind = pa.string()
                column["pandas_type"] = "unicode"
            elif pa.types.is_dictionary(kind):
                kind, column["numpy_type"], column["metadata"]["num_categories"] = cls.categorical(frame, field.name)
            fields.append(pa.field(field.name, kind))
        target = pa.schema(fields, metadata={b"pandas": json.dumps(metadata).encode()})
        table = frame.to_arrow()
        return pa.Table.from_arrays([table.column(field.name).cast(field.type) for field in target], schema=target)

    @classmethod
    def categorical(cls, frame: pl.DataFrame, column: str) -> Tuple[pa.DataType, str, int]:
        """The arrow dictionary type, the pandas code dtype and the number of categories of a ``Categorical`` or ``Enum`` column."""
        dtype = frame.schema[column]
        count = len(dtype.categories) if isinstance(dtype, pl.Enum) else frame.get_column(column).drop_nulls().n_unique()
        index = next((kind for limit, kind in cls.CODE_TYPES if count < limit), pa.int64())
        return pa.dictionary(index, pa.string()), str(index), count
