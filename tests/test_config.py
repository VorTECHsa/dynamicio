# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, R0801
import io
import os

import pytest
import yaml

from dynamicio.config.io_config import IOConfig, SafeDynamicResourceLoader
from tests import constants


class TestIOConfig:
    @pytest.mark.unit
    def test_config_io_parser_returns_a_transformed_dict_version_of_the_yaml_input_with_dynamic_values_replaced(self, expected_input_yaml_dict):
        # Given
        input_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/test_input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        )

        # When
        yaml_dict = input_config.config.dict()
        # Then
        assert yaml_dict == expected_input_yaml_dict

    @pytest.mark.unit
    def test_config_io_sources_returns_all_available_sources(self):
        # Given
        input_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/test_input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        )

        # When
        sources = list(input_config.config.bindings.keys())

        # Then
        assert sources == [
            "READ_FROM_S3_CSV_ALT",
            "READ_FROM_S3_CSV",
            "READ_FROM_S3_JSON",
            "READ_FROM_S3_HDF",
            "READ_FROM_S3_PARQUET",
            "READ_FROM_POSTGRES",
            "READ_FROM_KAFKA",
            "TEMPLATED_FILE_PATH",
            "READ_FROM_PARQUET_TEMPLATED",
            "REPLACE_SCHEMA_WITH_DYN_VARS",
        ]

    @pytest.mark.unit
    def test_get_for_config_io_set_for_a_local_env_returns_a_local_mapping_for_a_given_key(self, expected_s3_csv_local_mapping):
        # Given
        input_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/test_input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        )

        # When
        s3_csv_local_mapping = input_config.config.bindings["READ_FROM_S3_CSV"].dict()

        # Then
        assert s3_csv_local_mapping == expected_s3_csv_local_mapping

    @pytest.mark.unit
    def test_get_for_config_io_set_for_a_cloud_env_returns_a_cloud_mapping_for_an_postgres_key(self, expected_postgres_cloud_mapping):
        # Given
        input_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/test_input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        )

        # When
        postgres_cloud_mapping = input_config.get(source_key="READ_FROM_POSTGRES").dict()

        # Then
        assert postgres_cloud_mapping == expected_postgres_cloud_mapping


class TestSafeDynamicLoader:  # pylint: disable=R0903
    @pytest.mark.unit
    def test_replaces_all_resource_template_instances(self):
        file_contents = 'abc: "[[ VALUE_1 ]]/[[ VALUE_2 ]]"'

        class MockEnvironmentModule:  # pylint: disable=R0903
            VALUE_1 = "abc"
            VALUE_2 = "def"

        result = yaml.load(io.StringIO(file_contents), SafeDynamicResourceLoader.with_module(MockEnvironmentModule))

        assert result == {"abc": "abc/def"}


class TestSchemaBlockInResourceDefinition:
    @pytest.mark.unit
    def test_a_schema_block_is_rejected_with_a_pointer_to_the_migration(self, tmp_path):
        # Given
        definitions = tmp_path / "definitions.yaml"
        definitions.write_text('ORDERS:\n  LOCAL:\n    type: "local"\n    local:\n      file_path: "x.csv"\n      file_type: "csv"\n  schema:\n    file_path: "orders.yaml"\n')

        # When / Then
        with pytest.raises(Exception, match="dynamicio migrate") as error:
            IOConfig(str(definitions), env_identifier="LOCAL", dynamic_vars=constants)
        assert "DataFrameModel" in str(error.value)

    @pytest.mark.unit
    def test_a_resource_definition_without_a_schema_block_loads(self):
        # When
        config = IOConfig(os.path.join(constants.TEST_RESOURCES, "definitions/test_input.yaml"), env_identifier="LOCAL", dynamic_vars=constants)

        # Then
        assert "READ_FROM_S3_CSV" in config.config.bindings
