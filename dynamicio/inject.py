"""Injects dynamic values into a string."""

import re
import string
from typing import Any, Dict

dynamic_data_matcher = re.compile(r"(.*)(\[\[\s*(\S+)\s*]])(.*)")


def inject(value: str, **kwargs) -> str:
    """Parse a string and replace any "{DYNAMIC_VAR}" and "[[ DYNAMIC_VAR ]]" with the respective values in the kwargs.

    case-insensitive.
    Args:
        value: A string with dynamic values in the form of "{DYNAMIC_VAR}" or "[[ DYNAMIC_VAR ]]".
        kwargs: A mapping of values to replace in the path.

    Returns:
        str: String with all dynamic values replaced.
    """
    value = inject_square_bracket_vars(value, **kwargs)
    value = inject_curly_braces_vars(value, **kwargs)
    return value


def inject_square_bracket_vars(value: str, **kwargs) -> str:
    """Include dynamic values in the form of "[[ DYNAMIC_VAR ]]". case-insensitive.

    Args:
        value: A string with dynamic values in the form of "[[ DYNAMIC_VAR ]]".
        kwargs: Any kwargs to inject into the string.

    Returns:
        str: String with all dynamic values replaced.
    """
    kwargs_lower = {k.lower(): v for k, v in kwargs.items()}  # case-insensitive

    original_value = value  # for error message

    replacements: Dict[str, Any] = {}

    while result := dynamic_data_matcher.match(value):
        str_to_replace = result.group(3).lower()  # we want to be case-insensitive
        replacement = kwargs_lower.get(str_to_replace, None)

        replacements[str_to_replace] = replacement

        # finds the first match and replaces it
        value = dynamic_data_matcher.sub(f"\\g<1>{replacement}\\g<4>", value)

    if any(replacement is None for replacement in replacements.values()):
        raise ValueError(
            f'Expected [] values for all dynamic values: in "{original_value}"'
            f", given injections: {kwargs_lower}, values missing: {[k for k, v in replacements.items() if v is None]}"
        )

    return value


def inject_curly_braces_vars(value: str, **kwargs) -> str:
    """Parse a string and replace any "{DYNAMIC_VAR}" with the respective values in the kwargs. case-insensitive.

    Args:
        path: A string with dynamic values in the form of "{DYNAMIC_VAR}".
        kwargs: A mapping of values to replace in the path.

    Returns:
        str: The path with the dynamic values replaced with the respective values in the kwargs.
    """
    # string.Formatter.parse returns a 4-tuple of:
    # `literal_text`, `field_name`, `form_at_spec`, `conversion`
    # More info here https://docs.python.org/3.8/library/string.html#string.Formatter.parse
    fields = [group[1] for group in string.Formatter().parse(value) if group[1] is not None]

    kwargs_lower = {k.lower(): v for k, v in kwargs.items()}  # case-insensitive
    value_lower = value.format(**{field: f"{{{field.lower()}}}" for field in fields})  # make field names lowercase
    fields_lower = [field.lower() for field in fields]

    if not all(field in kwargs_lower for field in fields_lower):
        raise ValueError(
            f'Expected {{}} values for all dynamic values in: "{value}"'
            f", given injections: {kwargs_lower}, values missing: {[field for field in fields if field not in kwargs_lower]}"
        )

    path = value_lower.format(**{field: kwargs_lower[field] for field in fields_lower})

    return path
