"""Injects dynamic values into a string."""

import re
import string
from typing import Any, Dict

double_bracket_matcher = re.compile(r"(.*)(\[\[\s*(\S+)\s*]])(.*)")
curly_braces_matcher = re.compile(r"(.*)(\{\s*(\S+)\s*\})(.*)")


def inject(value: str, **kwargs) -> str:
    """Parse a string and replace any "{DYNAMIC_VAR}" and "[[ DYNAMIC_VAR ]]" with the respective values in the kwargs.

    case-insensitive.
    Args:
        value: A string with dynamic values in the form of "{DYNAMIC_VAR}" or "[[ DYNAMIC_VAR ]]".
        kwargs: A mapping of values to replace in the path.

    Returns:
        str: String with all dynamic values replaced.
    """
    value = _inject_square_bracket_vars(value, **kwargs)
    value = _inject_curly_braces_vars(value, **kwargs)
    return value


def check_injections(value: str) -> None:
    """Raise if a string has any dynamic values in the form of "{DYNAMIC_VAR}" or "[[ DYNAMIC_VAR ]]"."""
    _check_square_bracket_injections(value)
    _check_curly_braces_injections(value)


def _check_square_bracket_injections(value: str) -> None:
    while _ := double_bracket_matcher.match(value):
        raise ValueError(f'Path is not fully injected: "{value}"')


def _check_curly_braces_injections(value: str) -> None:
    fields = [group[1] for group in string.Formatter().parse(value) if group[1] is not None]
    if len(fields) > 0:
        raise ValueError(f'Path is not fully injected: "{value}"')


def _inject_square_bracket_vars(value: str, **kwargs) -> str:
    """Inject dynamic values in the form of "[[ DYNAMIC_VAR ]]". case-insensitive.

    Args:
        value: A string with dynamic values in the form of "[[ DYNAMIC_VAR ]]".
        kwargs: Any kwargs to inject into the string.

    Returns:
        str: String with all dynamic values replaced.
    """
    return _inject_with_matcher(value, double_bracket_matcher, **kwargs)


def _inject_curly_braces_vars(value: str, **kwargs) -> str:
    """Parse a string and replace any "{DYNAMIC_VAR}" with the respective values in the kwargs. case-insensitive.

    Args:
        path: A string with dynamic values in the form of "{DYNAMIC_VAR}".
        kwargs: A mapping of values to replace in the path.

    Returns:
        str: The path with the dynamic values replaced with the respective values in the kwargs.
    """
    return _inject_with_matcher(value, curly_braces_matcher, **kwargs)


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

    while result := matcher.match(value):
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
