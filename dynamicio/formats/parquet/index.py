"""The ``index_columns`` option of a parquet write, on the two engines."""

import logging
from typing import Any, Dict, Mapping, MutableMapping, Optional

logger = logging.getLogger(__name__)


class ParquetIndex:
    """The `index_columns` option of a parquet write: the columns of the frame that the file holds as its pandas index.

    `DataFrame.to_parquet` writes the index after the columns, and the pandas metadata names its levels. pyarrow names the field of a level after
    the level, or `__index_level_<n>__` if the level has no name or a column of the frame has its name. With `index_columns`, a write takes the
    index from these columns, so the file is that of the frame with `set_index(columns)` and these level names. The option is a list, in the
    order of the levels. A column name gives a level with the name of its column. A mapping of one column name to a level name, or to None for a
    level without a name, gives that level name. The column of each level must have the name of its field, so `[{"__index_level_0__": null}]`
    writes the unnamed index of a frame. A read is the same as without the option: the pandas engine gives the index, and the polars engine gives
    the fields as columns.
    """

    OPTION = "index_columns"

    @classmethod
    def pop(cls, options: MutableMapping[str, Any]) -> Optional[Dict[str, Optional[str]]]:
        """Remove `index_columns` from the options of a parquet writer, and return the level name of each column, in the order of the levels.

        Raises:
            ValueError: `index_columns` is not a list of column names and of mappings of one column name to a level name, or `index` is false.
        """
        value = options.pop(cls.OPTION, None)
        if value is None:
            return None
        levels = (
            [(item, item) if isinstance(item, str) else next(iter(item.items())) if isinstance(item, Mapping) and len(item) == 1 else (None, None) for item in value]
            if isinstance(value, (list, tuple))
            else []
        )
        names = dict(levels)
        valid = levels and len(names) == len(levels) and all(isinstance(column, str) and (name is None or isinstance(name, str)) for column, name in levels)
        if not valid:
            raise ValueError(f"`{cls.OPTION}` must be a list of column names and of mappings of one column name to an index name, without a column twice, got {value!r}")
        if options.get("index") is False:
            raise ValueError(f"`{cls.OPTION}` writes an index, so the index option cannot be false")
        return names

    @classmethod
    def set_index(cls, df: Any, names: Dict[str, Optional[str]]) -> Any:
        """The pandas frame `df` with the columns of `names` as its index, and the level names of `names`.

        Raises:
            ValueError: A column of `names` is not a column of `df`, or pyarrow gives the field of a level a name other than its column.
        """
        missing = [column for column in names if column not in df.columns]
        if missing:
            raise ValueError(f"`{cls.OPTION}` names {missing}, which are not columns of the frame: the columns are {list(df.columns)}")
        indexed = df.set_index(list(names))
        indexed.index.names = list(names.values())
        fields = [f"__index_level_{level}__" if name is None or name in indexed.columns else name for level, name in enumerate(names.values())]
        for column, field in zip(names, fields):
            if column != field:
                raise ValueError(f"the file holds the index level of column {column!r} in the field {field!r}. Name the column {field!r}, as the file does.")
        return indexed
