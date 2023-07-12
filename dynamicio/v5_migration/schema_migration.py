# pylint: skip-file
# noqa


from __future__ import annotations

import abc
import re
from dataclasses import dataclass
from string import ascii_lowercase, digits
from typing import Any

import yaml

schema_import_str = """from datetime import datetime

import pandera as pa
from pandera import SchemaModel
from pandera.typing import Series

"""

_numpy_type_to_pandera_mapping = {
    r"object": "str",
    r"float.*": "float",
    r"int.*": "int",
    r"datetime.*": "datetime",
    r"bool": "bool",
}


def is_schema_dict(yaml_schema: dict) -> bool:
    if "columns" not in yaml_schema:
        return False
    for name, info in yaml_schema["columns"].items():
        if not isinstance(name, str):
            return False
        if "type" not in info:
            return False
    return True


def convert_single_schema_file(yaml_contents: dict) -> str:
    name = yaml_contents["name"]
    columns = _collect_columns(yaml_contents)

    schema_class = SchemaClass(name=name, columns=columns)

    return schema_class.render_template()


class Validation(abc.ABC):
    @abc.abstractmethod
    def render_own_template(self) -> str:
        raise NotImplementedError()


@dataclass
class HasNoNulls(Validation):
    template: str = "{condition}"

    @staticmethod
    def is_matched(validation_name: str) -> bool:
        return validation_name == "has_no_null_values"

    @classmethod
    def parse_from_dict(cls, candidate: dict[str, Any]) -> "HasNoNulls":
        return cls()

    def render_own_template(self) -> str:
        return self.template.format(condition="nullable=False")


@dataclass
class IsIn(Validation):
    categories: list[str]
    template: str = "isin=[{categories}]"

    @staticmethod
    def is_matched(validation_name: str) -> bool:
        return validation_name == "is_in"

    @classmethod
    def parse_from_dict(cls, candidate: dict[str, Any]) -> "IsIn":
        return cls(categories=candidate["options"]["categorical_values"])

    def render_own_template(self) -> str:
        return self.template.format(categories=",".join(f'"{cat}"' for cat in self.categories))


_supported_validations = [
    HasNoNulls,
    IsIn,
]


@dataclass
class Column:
    name: str
    data_type: str
    validations: list[Validation]
    template_python_compatible = "{name}: Series[{data_type}] = pa.Field({options})"
    _allowed_chars: list[str] = ascii_lowercase + digits + "_"

    @property
    def is_python_normalized(self) -> bool:
        assert len(self.name) >= 1, "Column name cannot be empty"

        s = self.name

        is_lowercase = s == s.lower()
        is_starts_with_alpha = s[0].isalpha()

        is_only_alpha_num_and_underscore = all([(c in self._allowed_chars) for c in s])

        return is_lowercase and is_starts_with_alpha and is_only_alpha_num_and_underscore

    def _python_normalize(self) -> str:
        normalized_name = self.name

        # Lowercase the name
        normalized_name = normalized_name.lower()

        normalized_name_tmp = list(normalized_name)

        # Replace all non-allowed characters (including spaces) with underscores
        for idx, c in enumerate(list(normalized_name_tmp)):
            if c not in self._allowed_chars:
                normalized_name_tmp[idx] = "_"
        normalized_name = "".join(normalized_name_tmp)

        # Make sure the name doesn't begin with a number
        if normalized_name[0] in digits:
            normalized_name = normalized_name[1:]

        return normalized_name

    def render_template(self) -> str:
        options = self._render_options()

        if self.is_python_normalized:
            return self.template_python_compatible.format(
                name=self.name, data_type=self.data_type, options=",".join(options)
            )
        else:
            options.append(f'alias="{self.name}"')

            return self.template_python_compatible.format(
                name=self._python_normalize(), data_type=self.data_type, options=",".join(options)
            )

    def _render_options(self) -> list[str]:
        options = []

        for v in self.validations:
            options.append(v.render_own_template())

        # We default to all fields being nullable unless otherwise specified by the validations
        if "nullable=False" not in options:
            options.append("nullable=True")

        return options


@dataclass
class SchemaClass:
    name: str
    columns: list[Column]

    template = """
class {class_name}(SchemaModel):
{columns}

    class Config:
        coerce = True
        strict = "filter"
    """

    def _python_normalize(self) -> str:
        normalized_class_name = ""

        for word in self.name.split("_"):
            normalized_class_name += word.lower().capitalize()

        return normalized_class_name

    def render_template(self) -> str:
        rendered_columns = "\n".join(["    " + col.render_template() for col in self.columns])

        return self.template.format(
            class_name=f"{self._python_normalize()}Schema",
            columns=rendered_columns,
        )


def _collect_columns(yaml_schema) -> list[Column]:
    columns = []
    for col_name, col_info in yaml_schema["columns"].items():
        parsed_numpy_dtype = col_info["type"]
        parsed_validations = []

        for candidate_type in _numpy_type_to_pandera_mapping:
            if re.search(candidate_type, parsed_numpy_dtype) is not None:
                derived_pandera_type = _numpy_type_to_pandera_mapping[candidate_type]

        for validation_name, validation_body in col_info["validations"].items():
            for candidate_validation in _supported_validations:
                if candidate_validation.is_matched(validation_name):
                    parsed_validations.append(candidate_validation.parse_from_dict(validation_body))

        assert derived_pandera_type is not None, "Could not match the numpy dtype to pandera type"

        columns.append(
            Column(
                name=col_name,
                data_type=derived_pandera_type,
                validations=parsed_validations,
            )
        )

    return columns


# DEBUG
if __name__ == "__main__":
    with open("/Users/arturkrochin/Projects/dynamicio/dynamicio/v5_migration/example.yaml", "r") as f:
        schema = yaml.safe_load(f)
    print(convert_single_schema_file(schema))
