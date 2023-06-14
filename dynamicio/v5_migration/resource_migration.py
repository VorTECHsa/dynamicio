# pylint: skip-file
# noqa
# type: ignore


from __future__ import annotations

from dynamicio.v5_migration.resource_templates import (
    Kafka,
    KeyedResourceTemplate,
    LocalParquetFileType,
    Postgres,
    S3ParquetFileType,
)


def is_resource_dict(candidate_dict: dict) -> bool:
    """Checks if a dict is a resource dict."""
    for value in candidate_dict.values():
        if not isinstance(value, dict) or "schema" not in value:
            return False
    return True


def parse_resource_configs(parsed_yaml_entry: dict[str, str]) -> list:
    """Parses a single resource config dict."""
    resource_configs = []

    for key, val in parsed_yaml_entry.items():
        if key == "schema":
            continue

        for resource_type in [S3ParquetFileType, LocalParquetFileType, Kafka, Postgres]:
            if resource_type.is_dict_parseable(val):  # type: ignore
                resource_configs.append(resource_type.from_dict(val, key.lower()))  # type: ignore

    return resource_configs


def convert_resource_dict(parsed_yaml: dict) -> list[KeyedResourceTemplate]:
    """Converts a single resource dict to a list of keyed resource templates."""
    keyed_templates = []
    for resource_key, resource_value in parsed_yaml.items():
        resource_name = f"{resource_key.lower()}_resource"

        keyed_template = KeyedResourceTemplate(
            resources=parse_resource_configs(resource_value),
            resource_name=f"{resource_name}",
        )
        keyed_templates.append(keyed_template)

    return keyed_templates


resources_import_str = (
    "from dynamicio import KeyedResource, ParquetResource, S3ParquetResource, KafkaResource, PostgresResource\n\n"
)


def convert_single_resource_file(file_contents: dict) -> str:
    """Converts a single resource file (yaml) to valid python code without imports."""

    result = convert_resource_dict(file_contents)

    return "\n".join([resource.render_template() for resource in result])
