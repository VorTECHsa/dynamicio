"""Implements the `IOConfig` class, generating objects used as a configuration parameter for the instantiation of`src.utils.dynamicio.dataio.DynamicDataIO` objects.

The `IOConfig` object, essentially parses a yaml file that contains a set of input sources that will be processed by a
task, converting filtering and converting them into dictionaries.

For example, suppose an `input.yaml` file, containing:

    READ_FROM_S3_CSV:
      LOCAL:
        type: "local"
        local:
          file_path: "[[ TEST_RESOURCES ]]/data/input/some_csv_to_read.csv"
          file_type: "csv"
      CLOUD:
        type: "s3"
        s3:
          bucket: "[[ MOCK_BUCKET ]]"
          file_path: "[[ MOCK_KEY ]]"
          file_type: "csv"

would be loaded with:

    input_sources_config = IOConfig(
            "path_to/input.yaml",
            env_identifier="CLOUD",
            dynamic_vars=config_module
        )

and:

    input_sources_config.config

would return:

        {
            "READ_FROM_S3_CSV": {
                "LOCAL": {
                    "type": "local",
                    "local": {
                        "file_path": f"{test_global_vars.TEST_RESOURCES}/data/input/some_csv_to_read.csv",
                        "file_type": "csv",
                    },
                },
                "CLOUD": {
                    "type": "s3",
                    "s3": {
                        "bucket": "mock-bucket",
                        "file_path": "mock-key",
                        "file_type": "csv"
                    }
                },
            }
        }
"""
__all__ = ["IOConfig", "SafeDynamicResourceLoader", "SafeDynamicSchemaLoader"]

import re
from types import ModuleType
from typing import Any, List, MutableMapping

import pydantic
import yaml
from magic_logger import logger

from dynamicio.config.pydantic import BindingsYaml, IOEnvironment


class SafeDynamicResourceLoader(yaml.SafeLoader):
    """Implements a dynamic yaml loader that parses yaml files and replaces strings that map to [[ DYNAMIC_VAR ]].

    Dynamic variables defined in a provided module object.
    """

    module = None
    dynamic_data_matcher = re.compile(r"(.*)(\[\[\s*(\S+)\s*]])(.*)")

    @classmethod
    def with_module(cls, module: ModuleType):
        """Creates a dynamic subclass of SafeDynamicLoader with the `data_module` attribute set to `module`.

        Args:
            module: A global vars module with all the dynamic values defined in it.

        Returns:
            type
        """
        return type(f"{cls.__name__}_{module.__name__}", (cls,), {"module": module})

    def dyn_str_constructor(self, node: yaml.nodes.ScalarNode) -> str:
        """Responsible for the switching of one or more "[[ DYNAMIC_VAR ]]" strings with the respective attributes value in a given module.

        Args:
            node: Parsed item whose dynamic values that map to the "[[ DYNAMIC_VAR ]]" convention
                are replaced with the respective attributes in te provided module.

        Returns:
            Constructed `str` or numerical.
        """
        value = node.value

        while result := self.dynamic_data_matcher.match(value):
            ref = result.group(3)
            replacement = getattr(self.module, ref)

            value = self.dynamic_data_matcher.sub(f"\\g<1>{replacement}\\g<4>", value)

        return value


class SafeDynamicSchemaLoader(yaml.SafeLoader):
    """Implements a dynamic yaml loader that parses yaml files and replaces strings that map to [[ DYNAMIC_VAR ]].

    Dynamic variables defined in a provided module object.
    """

    module = None
    dynamic_data_matcher = re.compile(r"(.*)(\[\[\s*(\S+)\s*]])(.*)")

    @classmethod
    def with_module(cls, module: ModuleType):
        """Creates a dynamic subclass of SafeDynamicLoader with the `data_module` attribute set to `module`.

        Args:
            module: A global vars module with all the dynamic values defined in it.

        Returns:
            type
        """
        return type(f"{cls.__name__}_{module.__name__}", (cls,), {"module": module})

    def dyn_value_constructor(self, node: yaml.nodes.ScalarNode) -> Any:
        """Responsible for the switching of one or more "[[ DYNAMIC_VAR ]]" strings with the respective attributes value in a given module.

        Args:
            node: Parsed item whose dynamic values that map to the "[[ DYNAMIC_VAR ]]" convention
                are replaced with the respective attributes in te provided module.

        Returns:
            Constructed `str` or numerical.
        """
        value = node.value

        while result := self.dynamic_data_matcher.match(value):
            ref = result.group(3)
            replacement = getattr(self.module, ref)

            value = self.dynamic_data_matcher.sub(f"\\g<1>{replacement}\\g<4>", value)

            try:
                value = float(value)
                return value
            except ValueError:
                pass

        return value


class IOConfig:
    """Generates an object that returns a sub-dictionary of the elements of that yaml file.

    The file serves as a config for setting up DynamicDataIO objects. Requires a resources yaml file,
    an ENVIRONMENT value {CLOUD or LOCAL} and a vars module.

    Example:
        input_sources_config = IOConfig(
            "path_to/input.yaml",
            env_identifier="CLOUD",
            dynamic_vars=config_module
        )
    """

    YAML_TAG = "tag:yaml.org,2002:str"
    SafeDynamicResourceLoader.add_constructor(YAML_TAG, SafeDynamicResourceLoader.dyn_str_constructor)
    SafeDynamicSchemaLoader.add_constructor(YAML_TAG, SafeDynamicSchemaLoader.dyn_value_constructor)

    path_to_source_yaml: str
    env_identifier: str
    config: BindingsYaml

    def __init__(self, path_to_source_yaml: str, env_identifier: str, dynamic_vars: ModuleType):
        """Class constructor.

        Args:
            path_to_source_yaml: Absolute file path to yaml file containing source definitions
            env_identifier: "LOCAL" or "CLOUD".
            dynamic_vars: module containing values for dynamic values that the source yaml
                may reference.
        """
        self.path_to_source_yaml = path_to_source_yaml
        self.env_identifier = env_identifier
        self.dynamic_vars = dynamic_vars
        self.config = self._parse_sources_config()

    def _parse_sources_config(self) -> BindingsYaml:
        """Parses the yaml input and return a dictionary.

        Returns:
            A dictionary with the list of all file paths pointing to various input sources as those
            are defined in their respective data/*.yaml files.
        """
        used_file_inputs = [self.path_to_source_yaml]
        with open(self.path_to_source_yaml, "r") as stream:  # pylint: disable=unspecified-encoding]
            logger.debug(f"Parsing {self.path_to_source_yaml}...")
            data = yaml.load(stream, SafeDynamicResourceLoader.with_module(self.dynamic_vars))

        # Load any file_path's found in schema definitions
        for io_binding in data.values():
            if isinstance(io_binding, MutableMapping) and io_binding.get("schema", {}).get("file_path"):
                file_path = io_binding["schema"]["file_path"]
                used_file_inputs.append(file_path)
                # schema has `file_path`` in it
                with open(file_path, "r", encoding="utf8") as stream:
                    io_binding["schema"] = yaml.load(stream, SafeDynamicSchemaLoader.with_module(self.dynamic_vars))

        try:
            config = BindingsYaml(bindings=data)
            config.update_config_refs()
        except pydantic.ValidationError:
            logger.exception(f"Error loading {data=!r}, {used_file_inputs=!r}")
            raise
        return config

    @property
    def sources(self) -> List[str]:
        """Class property for easy access to a list of sources.

        Returns:
            All top level names of the available resources for the used resources yaml config.
        """
        return list(self.config.bindings.keys())

    def get(self, source_key: str) -> IOEnvironment:
        """A getter.

        Args:
            source_key: The name of the resource for which we want to create a config.

        Returns:
            A dictionary with the necessary fields for loading the data from a source.

        Example:

            Given:

                VOYAGE_DATA:
                  LOCAL:
                    type: "local"
                    local:
                      file_path: "[[ TEST_RESOURCES ]]/data/processed/voyage_data.parquet"
                      file_type: "parquet"
                  CLOUD:
                    type: "kafka"
                    KAFKA:
                      KAFKA_SERVER: "[[ KAFKA_SERVER ]]"
                      KAFKA_TOPIC: "[[ KAFKA_TOPIC ]]"

            If you do:

                input_sources_config = IOConfig(
                    "path_to/input.yaml",
                    env_identifier="CLOUD",
                    dynamic_vars=globals
                )
                voyage_data_cloud_mapping = input_config.get(source_key="VOYAGE_DATA")

            then `voyage_data_cloud_mapping` is:

                "KAFKA": {
                    "KAFKA_SERVER": "mock-kafka-server",
                    "KAFKA_TOPIC": "mock-kafka-topic"
                }
        """
        return self.config.bindings[source_key].get_binding_for_environment(self.env_identifier)
