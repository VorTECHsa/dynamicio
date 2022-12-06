"""Mixin utility functions"""
# pylint: disable=no-member, protected-access, too-few-public-methods

import inspect
import string
from contextlib import contextmanager
from functools import wraps
from types import FunctionType, MethodType
from typing import Any, Collection, Iterable, Mapping, MutableMapping, Optional, Union

from magic_logger import logger


def allow_options(options: Union[Iterable, FunctionType, MethodType]):
    """Validate **options for a decorated reader function.

    Args:
        options: A set of valid options for a reader (e.g. `pandas.read_parquet` or `pandas.read_csv`)

    Returns:
        read_with_valid_options: The input function called with modified options.
    """

    def _filter_out_irrelevant_options(kwargs: Mapping, valid_options: Iterable):
        filtered_options = {}
        invalid_options = {}
        for key_arg in kwargs.keys():
            if key_arg in valid_options:
                filtered_options[key_arg] = kwargs[key_arg]
            else:
                invalid_options[key_arg] = kwargs[key_arg]
        if len(invalid_options) > 0:
            logger.warning(
                f"Options {invalid_options} were not used because they were not supported by the read or write method configured for this source. "
                "Check if you expected any of those to have been used by the operation!"
            )
        return filtered_options

    def read_with_valid_options(func):
        @wraps(func)
        def _(*args, **kwargs):
            if callable(options):
                return func(*args, **_filter_out_irrelevant_options(kwargs, args_of(options)))
            return func(*args, **_filter_out_irrelevant_options(kwargs, options))

        return _

    return read_with_valid_options


def args_of(func):
    """Retrieve allowed options for a given function.

    Args:
        func: A function like, e.g., pd.read_csv

    Returns:
        A set of allowed options
    """
    return set(inspect.signature(func).parameters.keys())


def get_string_template_field_names(s: str) -> Collection[str]:  # pylint: disable=C0103
    """Given a string `s`, it parses the string to identify any template fields and returns the names of those fields.

     If `s` is not a string template, the returned `Collection` is empty.

    Args:
        s:

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
        ValueError: if any template fields in s are not named using valid Python identifiers
        ValueError: if a given template field cannot be resolved in `options`
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
