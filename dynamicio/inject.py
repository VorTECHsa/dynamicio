"""Injects dynamic values into a string."""

import re
from pathlib import Path
from typing import Any, Dict, TypeVar

double_bracket_matcher = re.compile(r"""(.*)(\[\[\s*(\S+)\s*]])(.*)""")
curly_braces_matcher = re.compile(r"(.*)(\{\s*(\S+)\s*\})(.*)")


class InjectionError(Exception):
    """Raised when a string has any dynamic values in the form of "{DYNAMIC_VAR}" or "[[ DYNAMIC_VAR ]]"."""


Injectable = TypeVar("Injectable", str, Path)


def inject(value: Injectable, **kwargs) -> Injectable:
    """Parse a string and replace any "{DYNAMIC_VAR}" and "[[ DYNAMIC_VAR ]]" with the respective values in the kwargs.

    case-insensitive.
    Args:
        value: A string with dynamic values in the form of "{DYNAMIC_VAR}" or "[[ DYNAMIC_VAR ]]".
        kwargs: A mapping of values to replace in the path.

    Returns:
        str: String with all dynamic values replaced.
    """
    to_inject = str(value)
    injected = _inject_with_matcher(to_inject, double_bracket_matcher, **kwargs)
    injected = _inject_with_matcher(injected, curly_braces_matcher, **kwargs)
    return type(value)(injected)


def check_injections(value: Injectable) -> None:
    """Raise if a string has any dynamic values in the form of "{DYNAMIC_VAR}" or "[[ DYNAMIC_VAR ]]"."""
    to_check: str = str(value)
    while _ := double_bracket_matcher.search(to_check):
        raise InjectionError(f'Path is not fully injected: "{to_check!r}"')
    while _ := curly_braces_matcher.search(to_check):
        raise InjectionError(f'Path is not fully injected: "{to_check!r}"')


def _inject_with_matcher(value: str, matcher, **kwargs) -> str:
    """Replaces any matching dynamic values.

    Args:
        path: A string with dynamic values.
        matcher: A regex matcher to find the dynamic values.
        kwargs: A mapping of values to replace in the path.

    Returns:
        str: The path with the dynamic values replaced with the respective values in the kwargs.
    """
    kwargs_lower = {k.lower(): v for k, v in kwargs.items()}  # case-insensitive

    replacements: Dict[str, Any] = {}

    temp_suffix_value = ""

    while result := matcher.search(value):
        str_to_replace = result.group(3).lower()  # we want to be case-insensitive
        replacement = kwargs_lower.get(str_to_replace, None)

        if replacement is None:
            suffix = matcher.sub("\\g<2>\\g<4>", value)
            temp_suffix_value = f"{suffix}{temp_suffix_value}"
            value = matcher.sub("\\g<1>", value)
        else:
            replacements[str_to_replace] = replacement

            # finds the first match and replaces it
            value = matcher.sub(f"\\g<1>{replacement}\\g<4>", value)

    value = f"{value}{temp_suffix_value}"

    return value
