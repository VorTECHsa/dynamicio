"""Mixin utility functions."""

import inspect
import string
from contextlib import contextmanager
from enum import Enum
from functools import wraps
from typing import Any, Callable, Collection, Iterable, Mapping, MutableMapping, Optional, Union

from magic_logger import logger


def allow_options(options: Union[Iterable[str], Callable]):
    """Decorator to filter **kwargs passed to a function, allowing only valid ones.

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


def args_of(*funcs) -> set[str]:
    """Retrieve a set of accepted keyword arguments from one or more functions.

    Args:
        funcs: A list of functions to inspect.

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


@contextmanager
def pickle_protocol(protocol: Optional[int]):
    """Downgrade to the provided pickle protocol within the context manager.

    Args:
        protocol: The number of the protocol HIGHEST_PROTOCOL to downgrade to. Defaults to 4, which covers python 3.4 and higher.
    """
    import pickle  # pylint: disable=import-outside-toplevel

    previous = pickle.HIGHEST_PROTOCOL
    try:
        pickle.HIGHEST_PROTOCOL = 4
        if protocol:
            pickle.HIGHEST_PROTOCOL = protocol
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
