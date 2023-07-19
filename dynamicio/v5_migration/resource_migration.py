# pylint: skip-file
# noqa
# type: ignore


from __future__ import annotations

from copy import deepcopy

from rich import print as rich_print

from dynamicio.v5_migration.resource_templates import (
    KafkaTemplate,
    LocalTemplate,
    PostgresTemplate,
    ReadyTemplate,
    S3Template,
)


def is_resource_dict(candidate_dict: dict) -> bool:
    """Checks if a dict is a resource dict."""
    # make lower case
    check_dict = {
        k.lower(): {kk.lower(): vv for kk, vv in v.items()} if isinstance(v, dict) else v
        for k, v in deepcopy(candidate_dict).items()
    }
    for key, value in check_dict.items():
        if not isinstance(value, dict):
            return False
        if "cloud" not in value:
            rich_print(f"[red]No cloud key in {key} resource - not parsing as resource.[/red]")
            return False
    return True


def convert_single_resource_file(file_contents: dict) -> str:
    """Converts a single resource file (yaml) to valid python code without imports."""

    result = convert_resource_dict(file_contents)

    return "\n".join([resource.render_template() for resource in result])


def convert_resource_dict(parsed_yaml: dict) -> list[ReadyTemplate]:
    """Converts a single resource dict to a list of keyed resource templates."""
    ready_templates = []
    for resource_key, resource_dict in parsed_yaml.items():
        resource_name = f"{resource_key.lower()}_resource"
        has_parsed = False
        for resource_type in [S3Template, LocalTemplate, KafkaTemplate, PostgresTemplate]:
            if resource_type.is_dict_parseable(resource_dict):  # type: ignore
                try:
                    ready_template = resource_type.from_dict(resource_dict, resource_name)
                    ready_templates.append(ready_template)  # type: ignore
                    has_parsed = True
                except Exception as e:
                    print(e)
                    has_parsed = False
        if not has_parsed:
            rich_print(f"Could not parse resource [red]{resource_key}[/red]")

    return ready_templates


def parse_resource_configs(parsed_yaml_entry: dict[str, str]) -> list:
    """Parses a single resource config dict."""
    resource_configs = []

    for key, val in parsed_yaml_entry.items():
        if key == "schema":
            continue

        for resource_type in [S3Template, LocalTemplate, KafkaTemplate, PostgresTemplate]:
            if resource_type.is_dict_parseable(val):  # type: ignore
                resource_configs.append(resource_type.from_dict(val, key.lower()))  # type: ignore

    return resource_configs


resources_import_str = (
    "from dynamicio import "
    "ParquetResource, CsvResource, JsonResource, HdfResource,"
    "S3ParquetResource, S3CsvResource, S3JsonResource, S3HdfResource, "
    "KafkaResource, PostgresResource\n"
)
