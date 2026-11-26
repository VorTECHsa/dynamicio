"""The parquet footer of the polars engine: the arrow types and the pandas metadata that ``DataFrame.to_parquet`` gives for the pandas dtypes of a model.

A parquet file of the polars engine then gives the same frame to the pandas engine of 11.1.0 (rule 2 of ADR 0010). ``PolarsFiles`` and ``PolarsSinks``
write with this footer.
"""

import json
from typing import TYPE_CHECKING, Any, Dict, MutableMapping, Tuple

import pandas as pd
import polars as pl
import pyarrow as pa

from dynamicio.engines.polars_options import PolarsOptions
from dynamicio.errors import EngineResourceError
from dynamicio.formats.parquet.index import ParquetIndex
from dynamicio.schema.polars_dtypes import DtypeMap

if TYPE_CHECKING:  # pragma: no cover
    from dynamicio.core import DynamicDataIO


class PolarsParquet:
    """The arrow schema, the arrow table and the ``pyarrow.parquet`` arguments of a parquet write of the polars engine."""

    # The codes of a pandas categorical: int8 below 127 categories, int16 below 32767 and int32 below 2**31 - 1, as `coerce_indexer_dtype` gives them
    CODE_TYPES = ((2**7 - 1, pa.int8()), (2**15 - 1, pa.int16()), (2**31 - 1, pa.int32()))
    # The pandas metadata of the columns index of a frame with string column names, as `DataFrame.to_parquet` writes it
    COLUMN_INDEXES = [{"name": None, "field_name": None, "pandas_type": "unicode", "numpy_type": "object", "metadata": {"encoding": "UTF-8"}}]

    @staticmethod
    def parquet_arguments(options: MutableMapping[str, Any]) -> Dict[str, Any]:
        """The arguments of ``pyarrow.parquet.write_table`` from the parquet write options of a resource."""
        return {name: value for name, value in PolarsOptions.write_options("parquet", options).items() if name not in ("engine", "index", "metadata", ParquetIndex.OPTION)}

    @classmethod
    def arrow_table(cls, io: "DynamicDataIO", frame: pl.DataFrame, options: MutableMapping[str, Any]) -> pa.Table:
        """The arrow table of a frame, with the arrow types and the pandas metadata that ``DataFrame.to_parquet`` gives for the pandas dtypes of the model."""
        counts = {column: frame.get_column(column).null_count() for column in frame.columns}
        target = cls.arrow_schema(io, frame.schema, frame.height, cls.category_counts(frame), options, null_counts=counts)
        return cls.to_schema(frame.to_arrow(), target)

    @staticmethod
    def pandas_dtype(io: "DynamicDataIO", column: str, has_nulls: bool) -> str:
        """The pandas dtype of a column of the model, with the nullable dtype of an integer or a boolean column that has a null (decision 14 of ADR 0010)."""
        dtype = io.schema.columns[column].data_type
        return DtypeMap.holding_nulls(dtype) if has_nulls else dtype

    @staticmethod
    def to_schema(table: pa.Table, target: pa.Schema) -> pa.Table:
        """A table with the columns, the types and the metadata of a target schema."""
        # pyarrow has no cast to the null type, so a column of nulls takes the nulls of its length
        columns = [pa.nulls(table.num_rows) if pa.types.is_null(field.type) else table.column(field.name).cast(field.type) for field in target]
        return pa.Table.from_arrays(columns, schema=target)

    @staticmethod
    def category_counts(frame: pl.DataFrame) -> Dict[str, int]:
        """The number of categories of each ``Categorical`` column of a frame: its values that are not null."""
        return {column: frame.get_column(column).drop_nulls().n_unique() for column, dtype in frame.schema.items() if dtype == pl.Categorical}

    @classmethod
    def arrow_schema(cls, io: "DynamicDataIO", schema: pl.Schema, height: int, counts: Dict[str, int], options: MutableMapping[str, Any], *, null_counts: Dict[str, int]) -> pa.Schema:
        """The arrow types and the pandas metadata that ``DataFrame.to_parquet`` gives for the pandas dtypes of the model.

        pyarrow makes the types and the metadata from an empty pandas frame with these dtypes. An empty ``object`` column has the ``null`` type there, and
        an empty ``category`` column has no categories, so the engine gives them their string, list or ``bool`` types and the number of categories of the
        frame.

        ``index=False`` gives the metadata of ``DataFrame.to_parquet(index=False)``, without an index. ``index_columns`` gives the fields and the
        metadata of the index of ``set_index`` (``formats.parquet.index.ParquetIndex``), after the other columns. Otherwise the metadata has the ``RangeIndex`` of the
        frame, as ``DataFrame.to_parquet`` writes by default.

        An ``object`` column with rows that are all null has the ``null`` type and the pandas type ``empty``, as ``DataFrame.to_parquet`` writes it.

        Args:
            io: The IO object, for the pandas dtypes of its model.
            schema: The polars schema of the frame, with the columns in the order of the file.
            height: The number of rows of the frame, for the ``RangeIndex``.
            counts: The number of categories of each ``Categorical`` column. An ``Enum`` column has the number of its categories.
            options: The write options of the resource, for ``index`` and ``index_columns``.
            null_counts: The number of nulls of each column. An integer or a boolean column with a null has the nullable pandas dtype in the metadata.

        Raises:
            EngineResourceError: ``index_columns`` is not valid, or names a column that is not in the frame or that has another name than its field.
        """
        options = dict(options)
        try:
            names = ParquetIndex.pop(options)
            dtypes = {column: cls.pandas_dtype(io, column, null_counts.get(column, 0) > 0) for column in schema.names()}
            empty = pd.DataFrame({column: pd.Series([], dtype=dtype) for column, dtype in dtypes.items()})
            arrow = pa.Schema.from_pandas(empty if names is None else ParquetIndex.set_index(empty, names), preserve_index=names is not None)
        except ValueError as error:
            raise EngineResourceError(f"{io.name}: {error}") from error
        metadata = json.loads(arrow.metadata[b"pandas"])
        if names is None and options.get("index") is not False:
            metadata["index_columns"] = [{"kind": "range", "name": None, "start": 0, "stop": height, "step": 1}]
            metadata["column_indexes"] = cls.COLUMN_INDEXES
        nulls = {column for column, count in null_counts.items() if height and count == height}
        fields = [cls.arrow_field(field, column, schema[field.name], counts, field.name in nulls) for field, column in zip(arrow, metadata["columns"])]
        return pa.schema(fields, metadata={**{key.encode(): value.encode() for key, value in (options.get("metadata") or {}).items()}, b"pandas": json.dumps(metadata).encode()})

    @classmethod
    def arrow_field(cls, field: pa.Field, column: Dict[str, Any], dtype: Any, counts: Dict[str, int], all_null: bool = False) -> pa.Field:
        """The arrow field of a column of the file, from the field of an empty pandas frame. The pandas metadata ``column`` changes in place.

        ``all_null`` is true for a column with rows that are all null. An ``object`` column of that kind keeps the ``null`` type of the empty frame.
        """
        kind = field.type
        if pa.types.is_null(kind) and all_null:
            column["pandas_type"] = "empty"
        elif pa.types.is_null(kind) and dtype == pl.Boolean:
            kind = pa.bool_()
            column["pandas_type"] = "bool"
        elif pa.types.is_null(kind) and isinstance(dtype, (pl.List, pl.Datetime)):
            kind = cls.list_type(dtype) if isinstance(dtype, pl.List) else pl.Series([], dtype=dtype).to_arrow().type
            column["pandas_type"] = pa.pandas_compat.get_logical_type(kind)
        elif pa.types.is_null(kind):
            kind = pa.string()
            column["pandas_type"] = "unicode"
        elif pa.types.is_dictionary(kind):
            count = len(dtype.categories) if isinstance(dtype, pl.Enum) else counts[field.name]
            kind, column["numpy_type"], column["metadata"]["num_categories"] = cls.categorical(count)
        return pa.field(field.name, kind)

    @staticmethod
    def list_type(dtype: pl.List) -> pa.DataType:
        """The arrow type that ``DataFrame.to_parquet`` gives a pandas column of lists with the items of ``dtype``: a ``list<item>`` of ``string``, not of ``large_string``."""
        item = pl.Series([], dtype=dtype.inner).to_arrow(compat_level=pl.CompatLevel.oldest()).type
        return pa.list_(pa.string() if pa.types.is_large_string(item) else item)

    @classmethod
    def categorical(cls, count: int) -> Tuple[pa.DataType, str, int]:
        """The arrow dictionary type, the pandas code dtype and the number of categories of a categorical column with ``count`` categories."""
        index = next((kind for limit, kind in cls.CODE_TYPES if count < limit), pa.int64())
        return pa.dictionary(index, pa.string()), str(index), count
