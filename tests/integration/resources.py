# pylint: disable=missing-module-docstring, missing-function-docstring
import json
import types
from pathlib import Path
from typing import Optional

# Application Imports
from dynamicio.config import IOConfig


class Resources:
    """Builds the CLOUD resource definitions that point at a floci stack."""

    @staticmethod
    def build(directory: Path, body: str):
        path = directory / "resources.yaml"
        path.write_text(f"R:\n  CLOUD:\n{body}")
        return IOConfig(path_to_source_yaml=str(path), env_identifier="CLOUD", dynamic_vars=types.ModuleType("no_dynamic_vars")).get(source_key="R")

    @staticmethod
    def s3_file(directory: Path, bucket: str, key: str, file_type: str, options: Optional[dict] = None, type_name: str = "s3_file"):
        """An S3 object resource. ``type_name="s3"`` gives the deprecated alias of ``s3_file``."""
        # A JSON object is a YAML flow mapping
        options_line = f"    options: {json.dumps(options)}\n" if options else ""
        return Resources.build(directory, f'    type: "{type_name}"\n    s3:\n      bucket: "{bucket}"\n      file_path: "{key}"\n      file_type: "{file_type}"\n{options_line}')

    @staticmethod
    def s3_path_prefix(directory: Path, bucket: str, prefix: str, file_type: str = "parquet", *, options: Optional[dict] = None, path_key: str = "path_prefix"):
        """A prefix resource. ``path_key="dynamic_file_path"`` makes ``prefix`` a dynamic file path with ``/**/`` in it."""
        options_line = f"    options: {json.dumps(options)}\n" if options else ""
        return Resources.build(directory, f'    type: "s3_path_prefix"\n    s3:\n      bucket: "{bucket}"\n      {path_key}: "{prefix}"\n      file_type: "{file_type}"\n{options_line}')

    @staticmethod
    def postgres(directory: Path, stack):
        return Resources.build(
            directory,
            f'    type: "postgres"\n    postgres:\n      db_host: "{stack.db_host}"\n      db_port: "{stack.db_port}"\n      db_name: "{stack.db_name}"\n'
            f'      db_user: "{stack.db_user}"\n      db_password: "{stack.db_password}"\n',
        )

    @staticmethod
    def kafka(directory: Path, stack, topic: str):
        return Resources.build(directory, f'    type: "kafka"\n    kafka:\n      kafka_server: "{stack.kafka_bootstrap}"\n      kafka_topic: "{topic}"\n')
