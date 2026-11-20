"""Mixin utility functions."""

# pylint: disable=no-member, protected-access, too-few-public-methods

import inspect
import json
import logging
import string
import threading
from contextlib import contextmanager
from enum import Enum
from functools import wraps
from typing import Any, Callable, Collection, Dict, Iterable, Mapping, MutableMapping, Optional, Union

logger = logging.getLogger(__name__)


def allow_options(options: Union[Iterable[str], Callable]):
    """Decorator to filter **kwargs passed to a function, allowing only valid ones.

    Options are filtered per reader or writer: a key that is not in `options` is dropped with a warning only.
    A new dynamicio option (one that the target library does not accept, e.g. `truncate_and_append`) must be
    added to the list of each reader or writer that reads it, as in `[*args_of(pd.HDFStore.put), "pickle_protocol"]`.

    Args:
        options: A list of valid options or a callable that returns a list of valid options.

    Returns:
        Callable: A decorator that filters **kwargs passed to the function.
    """

    def _filter_out_irrelevant_options(kwargs: Mapping, valid_options: Iterable):
        filtered_options = {}
        invalid_options = {}
        for key, val in kwargs.items():
            if key in valid_options:
                filtered_options[key] = val
            else:
                invalid_options[key] = val
        if invalid_options:
            logger.warning(f"Options {invalid_options} were not used because they are not supported by this operation. " f"Review your kwargs!")
        return filtered_options

    def read_with_valid_options(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            valid = args_of(options) if callable(options) else set(options)
            return func(*args, **_filter_out_irrelevant_options(kwargs, valid))

        return wrapper

    return read_with_valid_options


def args_of(*funcs: Callable) -> set[str]:
    """Retrieve a set of accepted keyword arguments from one or more functions.

    Args:
        *funcs: The functions to inspect.

    Returns:
        set[str]: A set of accepted keyword arguments.
    """
    allowed_args = set()
    for func in funcs:
        sig = inspect.signature(func)
        allowed_args.update(sig.parameters.keys())
    return allowed_args


def get_string_template_field_names(s: str) -> Collection[str]:  # pylint: disable=C0103
    """Given a string `s`, it parses the string to identify any template fields and returns the names of those fields.

     If `s` is not a string template, the returned `Collection` is empty.

    Args:
        s: A string which is either a template, e.g. /path/to/file/{replace_me}.h5 or just a path /path/to/file/dont_replace_me.h5

    Returns:
        Collection[str]

    Example:
        >>> get_string_template_field_names("abc{def}{efg}")
        ["def", "efg"]
        >>> get_string_template_field_names("{0}-{1}")
        ["0", "1"]
        >>> get_string_template_field_names("hello world")
        []
    """
    # string.Formatter.parse returns a 4-tuple of:
    # `literal_text`, `field_name`, `form_at_spec`, `conversion`
    # More info here https://docs.python.org/3.8/library/string.html#string.Formatter.parse
    field_names = [group[1] for group in string.Formatter().parse(s) if group[1] is not None]

    return field_names


def resolve_template(path: str, options: MutableMapping[str, Any]) -> str:  # pylint: disable=C0103
    """Given a string `path`, it attempts to replace all templates fields with values provided in `options`.

    If `path` is not a string template, `path` is returned.

    Args:
        path: A string which is either a template, e.g. /path/to/file/{replace_me}.h5 or just a path /path/to/file/dont_replace_me.h5
        options: A dynamic name for the "replace_me" field in the templated string. e.g. {"replace_me": "name_of_file"}

    Returns:
        str: Returns a static path replaced with the value in the options mapping.

    Raises:
        ValueError: if any template fields in s are not named using valid Python identifiers or if a given template field cannot be resolved in `options`
    """
    fields = get_string_template_field_names(path)

    if len(fields) == 0:
        return path

    if not all(field.isidentifier() for field in fields):
        raise ValueError(f"Expected valid Python identifiers, found {fields}")

    if not all(field in options for field in fields):
        raise ValueError(f"Expected values for all fields in {fields}, found {list(options.keys())}")

    path = path.format(**{field: options[field] for field in fields})
    for field in fields:
        options.pop(field)

    return path


_PICKLE_PROTOCOL_LOCK = threading.RLock()


@contextmanager
def pickle_protocol(protocol: Optional[int]):
    """Downgrade to the provided pickle protocol within the context manager.

    The lock serializes the callers of this context manager only. `pickle.HIGHEST_PROTOCOL` is process-wide, so another thread that pickles while the
    context is open also gets the downgraded protocol. Lock order: take this lock before `with_local.hdf_lock`, never the other way, or two writers deadlock.

    Args:
        protocol: The number of the protocol HIGHEST_PROTOCOL to downgrade to. Defaults to 4, which covers python 3.4 and higher.
    """
    import pickle  # pylint: disable=import-outside-toplevel

    with _PICKLE_PROTOCOL_LOCK:  # `HIGHEST_PROTOCOL` is global: two threads that overlap here would restore each other's value, and one would remain
        previous = pickle.HIGHEST_PROTOCOL
        try:
            pickle.HIGHEST_PROTOCOL = protocol or 4
            yield
        finally:
            pickle.HIGHEST_PROTOCOL = previous


def get_file_type_value(file_type: Union[str, Enum]) -> str:
    """Get the value of the file type.

    Args:
        file_type: The file type, which can be a string or an Enum.

    Returns:
        str: The value of the file type.
    """
    return file_type.value if isinstance(file_type, Enum) else file_type


class JsonIndex:
    """The `index_column` option of a JSON resource: the column that holds the row labels of a JSON object with orient `index` or `columns`.

    `DataFrame.to_json` writes the index as the keys of these orients, and `pd.read_json` reads the keys back as the index. With `index_column`,
    a read gives the keys as a column of that name, and a write takes the keys from that column. The file is that of the frame with
    `set_index(index_column)`. The polars engine requires the option for these orients, because a polars frame has no index.
    """

    OPTION = "index_column"
    ORIENTS = ("index", "columns")

    @classmethod
    def pop(cls, options: MutableMapping[str, Any], orient: Optional[str], lines: bool = False) -> Optional[str]:
        """Remove `index_column` from the options of a JSON reader or writer, and return it.

        Args:
            options: The options of the reader or the writer. The method removes `index_column` from them.
            orient: The orient of the file. None is the `columns` default of `pd.read_json` and `DataFrame.to_json`.
            lines: Whether the file holds JSON lines.

        Raises:
            ValueError: `index_column` is not a string, or the orient is not `index` or `columns`, or `lines` is true.
        """
        index_column = options.pop(cls.OPTION, None)
        if index_column is None:
            return None
        if not isinstance(index_column, str):
            raise ValueError(f"`{cls.OPTION}` must be the name of a column, got {index_column!r}")
        if (orient or "columns") not in cls.ORIENTS or lines:
            raise ValueError(f"`{cls.OPTION}` applies to a JSON object with orient index or columns only, not to orient {orient!r} with lines={lines!r}")
        return index_column

    @staticmethod
    def records(data: bytes, orient: str, index_column: str) -> bytes:
        """The JSON records of a JSON object with orient `index` or `columns`. The key of each row goes in the column `index_column`, first.

        Orient `columns` holds one object for each column. The rows are the keys of these objects, in the order of their first use, and a key that a
        column does not have gives a null, as on pandas.

        Raises:
            ValueError: The JSON is not an object of objects, or the object has a column of the name `index_column`, as `reset_index(names=index_column)`
                raises on pandas. The key of a row would otherwise replace the value of that column.
        """
        document = json.loads(data)
        if not isinstance(document, dict) or not all(isinstance(values, dict) for values in document.values()):
            raise ValueError(f"a JSON file of orient {orient!r} must hold an object of objects, and this one holds a {type(document).__name__} of other values")
        if orient == "columns":
            if index_column in document:
                raise ValueError(f"cannot insert {index_column}, already exists: the JSON object has a column of the name of `{JsonIndex.OPTION}`")
            keys = dict.fromkeys(key for values in document.values() for key in values)
            document = {key: {column: values.get(key) for column, values in document.items()} for key in keys}
        elif any(index_column in row for row in document.values()):
            raise ValueError(f"cannot insert {index_column}, already exists: the JSON object has a column of the name of `{JsonIndex.OPTION}`")
        return json.dumps([{index_column: key, **row} for key, row in document.items()]).encode()


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
