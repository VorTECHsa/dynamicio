# pylint: skip-file
# noqa


from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from string import ascii_lowercase, digits

import yaml

schema_import_str = ""

_numpy_type_to_pandera_mapping = {
    r"object": "str",
    r"float.*": "float",
    r"int.*": "int",
    r"datetime.*": "datetime",
    r"bool": "bool",
}


def convert_single_schema_file(file_path: Path) -> str:
    yaml_schema = yaml.safe_load(file_path)

    name = yaml_schema["name"]
    columns = _collect_columns(yaml_schema)

    schema_class = SchemaClass(name=name, columns=columns)

    return schema_class.render_template()


@dataclass
class Column:
    name: str
    data_type: str

    _allowed_chars: list[str] = ascii_lowercase + digits + "_"

    template_python_compatible = "{name}: Series[{data_type}] = pa.Field(nullable=True)"
    template_python_incompatible = (
        '{python_normalized_name}: Series[{data_type}] = pa.Field(alias="{name}", nullable=True)'
    )

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
        if self.is_python_normalized:
            return self.template_python_compatible.format(name=self.name, data_type=self.data_type)
        else:
            return self.template_python_incompatible.format(
                python_normalized_name=self._python_normalize(), name=self.name, data_type=self.data_type
            )


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
        rendered_columns = "\n".join(["\t" + col.render_template() for col in self.columns])

        return self.template.format(
            class_name=f"{self._python_normalize()}Schema",
            columns=rendered_columns,
        )


def _collect_columns(yaml_schema) -> list[Column]:
    columns = []
    for col_name, col_info in yaml_schema["columns"].items():
        parsed_numpy_dtype = col_info["type"]

        for candidate_type in _numpy_type_to_pandera_mapping:
            if re.search(candidate_type, parsed_numpy_dtype) is not None:
                derived_pandera_type = _numpy_type_to_pandera_mapping[candidate_type]

        assert derived_pandera_type is not None, "Could not match the numpy dtype to pandera type"

        columns.append(
            Column(
                name=col_name,
                data_type=derived_pandera_type,
            )
        )

    return columns
