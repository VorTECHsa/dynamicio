"""The resource definitions that the polars engine refuses or reads differently."""

import os
from typing import Any, Dict, List, Mapping, Optional, Tuple

import yaml

from dynamicio.engines.polars_options import PolarsOptions
from dynamicio.engines.polars_postgres import PolarsPostgres
from dynamicio.errors import EngineResourceError
from dynamicio.mixins.utils import get_string_template_field_names


class ResourceReview:
    """Lists the resource definitions that the polars engine refuses, and the CSV definitions without an ``index`` option.

    It reads the YAML files and changes none of them. It cannot see the options that the code passes to an IO object.
    """

    FILE_SOURCES = {"local": "local", "local_batch": "local", "s3": "s3", "s3_file": "s3", "s3_path_prefix": "s3"}

    @staticmethod
    def definitions(root: str) -> Dict[str, Dict[str, Any]]:
        """The resource definitions under ``root``, by the path of their YAML file. A file that is not a mapping of resources is left out."""
        found = {}
        for folder, _, names in os.walk(root):
            for name in sorted(names):
                if not name.endswith((".yaml", ".yml")):
                    continue
                file_path = os.path.join(folder, name)
                try:
                    with open(file_path, encoding="utf-8") as handle:
                        content = yaml.safe_load(handle)
                except (yaml.YAMLError, UnicodeDecodeError):
                    continue
                if isinstance(content, dict):
                    resources = {key: value for key, value in content.items() if isinstance(value, dict) and any(isinstance(env, dict) and "type" in env for env in value.values())}
                    if resources:
                        found[file_path] = resources
        return dict(sorted(found.items()))

    @classmethod
    def problem_of(cls, environment: Mapping[str, Any]) -> Tuple[Optional[str], bool]:
        """The reason that the polars engine refuses one environment of a resource, or None, and whether it is a CSV without an ``index`` option."""
        source, options = environment.get("type"), dict(environment.get("options") or {})
        if source in cls.FILE_SOURCES:
            section = environment.get(cls.FILE_SOURCES[source]) or {}
            file_type = str(section.get("file_type", "")).lower()
            paths = [str(section.get(key)) for key in ("file_path", "path_prefix", "dynamic_file_path") if section.get(key)]
            fields = {field for path in paths for field in get_string_template_field_names(path)}
            try:
                PolarsOptions.check(file_type, "s3_file" if source == "s3" else source, {name: value for name, value in options.items() if name not in fields})
            except EngineResourceError as error:
                return str(error), False
            return None, file_type == "csv" and "index" not in options
        if source == "kafka" and "key_column" not in options:
            return "the polars engine needs the option key_column: a polars frame has no index to give the message keys.", False
        refused = sorted(PolarsPostgres.PANDAS_READ_OPTIONS & set(options)) if source == "postgres" else []
        return (f"the polars engine has no Postgres option {', '.join(refused)}.", False) if refused else (None, False)

    @classmethod
    def review(cls, root: str) -> Dict[str, List[str]]:
        """``{"refused": [...], "csv_index": [...]}``: one ``file: RESOURCE.ENVIRONMENT`` line for each finding."""
        refused, csv_index = [], []
        for file_path, resources in cls.definitions(root).items():
            relative = os.path.relpath(file_path, root)
            for resource, environments in resources.items():
                for environment_name, environment in environments.items():
                    if not isinstance(environment, dict) or "type" not in environment:
                        continue
                    problem, no_index = cls.problem_of(environment)
                    if problem:
                        refused.append(f"{relative}: {resource}.{environment_name}: {problem}")
                    if no_index:
                        csv_index.append(f"{relative}: {resource}.{environment_name}")
        return {"refused": refused, "csv_index": csv_index}
