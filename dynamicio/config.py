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
__all__ = ["IOConfig", "SafeDynamicLoader"]

import re
from types import ModuleType
from typing import List, Mapping

import yaml
from magic_logger import logger


class SafeDynamicLoader(yaml.SafeLoader):
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
        """Responsible for the switching of one or more "[[ DYNAMIC_VAR ]]" strings with the respective attributes value in a give module.

        Args:
            node: Parsed item whose dynamic values that map to the "[[ DYNAMIC_VAR ]]" convention
                are replaced with the respective attributes in te provided module.

        Returns:
            Constructed `str`
        """
        value = node.value

        while result := self.dynamic_data_matcher.match(value):
            ref = result.group(3)
            replacement = getattr(self.module, ref)

            value = self.dynamic_data_matcher.sub(f"\\g<1>{replacement}\\g<4>", value)

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
    SafeDynamicLoader.add_constructor(YAML_TAG, SafeDynamicLoader.dyn_str_constructor)

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

    def _parse_sources_config(self) -> Mapping:
        """Parses the yaml input and return a dictionary.

        Returns:
            A dictionary with the list of all file paths pointing to various input sources as those
            are defined in their respective data/*.yaml files.
        """
        with open(self.path_to_source_yaml, "r") as stream:  # pylint: disable=unspecified-encoding]
            logger.debug(f"Parsing {self.path_to_source_yaml}...")
            return yaml.load(stream, SafeDynamicLoader.with_module(self.dynamic_vars))

    @property
    def sources(self) -> List[str]:
        """Class property for easy access to a list of sources.

        Returns:
            All top level names of the available resources for the used resources yaml config.
        """
        return list(self.config.keys())

    def get(self, source_key: str) -> Mapping:
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
        source_config = self.config[source_key][self.env_identifier]
        if self.config[source_key].get("schema"):
            schema_definition = self._get_schema_definition(source_key)
            source_config["schema"] = self._get_schema(schema_definition)
            source_config["validations"] = self._get_validations(schema_definition)
            source_config["metrics"] = self._get_metrics(schema_definition)
        return source_config

    def _get_schema_definition(self, source_key: str) -> Mapping:
        """Retrieves the schema definition from a resource definition.

        Returns:
            The schema definition provided for a resource definition.
        """
        schema_file_path = self.config[source_key].get("schema")["file_path"]
        with open(schema_file_path, "r") as stream:  # pylint: disable=unspecified-encoding]
            logger.debug(f"Parsing schema: {schema_file_path}...")
            return yaml.load(stream, Loader=yaml.SafeLoader)

    @staticmethod
    def _get_schema(schema_definition: Mapping) -> Mapping:
        """Retrieve the schema from a schema definition.

        Args:
            schema_definition:

        Returns:
            The column types in the schema definition.
        """
        _schema = {}
        for column in schema_definition["columns"].keys():
            _schema[column] = schema_definition["columns"][column]["type"]
        return _schema

    @staticmethod
    def _get_validations(schema_definition: Mapping) -> Mapping:
        """Returns all validations for each column in a schema definition.

        Args:
            schema_definition: A dictionary with all columns in a dataset characterised by validations and metrics

        Returns:
            The validations applied to each column in the schema definition.
        """
        _validations = {}
        for column in schema_definition["columns"].keys():
            _validations[column] = schema_definition["columns"][column]["validations"]
        return _validations

    @staticmethod
    def _get_metrics(schema_definition):
        """Returns all metrics for each column in a schema definition.

        Args:
            schema_definition: A dictionary with all columns in a dataset characterised by validations and metrics

        Returns:
            The metrics applied to each column in the schema definition.
        """
        _metrics = {}
        for column in schema_definition["columns"].keys():
            _metrics[column] = schema_definition["columns"][column]["metrics"]
        return _metrics
