# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, R0801
import io
import os

import pytest
import yaml

from dynamicio.config import IOConfig, SafeDynamicLoader
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
        yaml_dict = input_config._parse_sources_config()  # pylint: disable=protected-access

        # Then
        assert yaml_dict == expected_input_yaml_dict

    @pytest.mark.unit
    def test_config_io_get_schema_definition_returns_a_schema_definition_from_a_source_config(self, expected_schema_definition):
        # Given
        input_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/test_input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        )

        # When
        schema_definition = input_config._get_schema_definition("READ_FROM_S3_CSV")  # pylint: disable=protected-access

        # Then
        assert schema_definition == expected_schema_definition

    @pytest.mark.unit
    def test_config_io_get_schema_returns_a_schema_from_a_schema_definition(self, input_schema_definition, expected_schema):
        # Given
        schema_definition = input_schema_definition

        # When
        schema = IOConfig._get_schema(schema_definition)  # pylint: disable=protected-access

        # Then
        assert schema == expected_schema

    @pytest.mark.unit
    def test_config_io_get_schema_returns_all_validations_from_a_schema_definition(self, input_schema_definition, expected_validations):
        # Given
        schema_definition = input_schema_definition

        # When
        validations = IOConfig._get_validations(schema_definition)  # pylint: disable=protected-access

        # Then
        assert validations == expected_validations

    @pytest.mark.unit
    def test_config_io_get_schema_returns_all_metrics_from_a_schema_definition(self, input_schema_definition, expected_metrics):
        # Given
        schema_definition = input_schema_definition

        # When
        metrics = IOConfig._get_metrics(schema_definition)  # pylint: disable=protected-access

        # Then
        assert metrics == expected_metrics

    @pytest.mark.unit
    def test_config_io_sources_returns_all_available_sources(self, expected_input_sources):
        # Given
        input_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/test_input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        )

        # When
        sources = input_config.sources

        # Then
        assert sources == expected_input_sources

    @pytest.mark.unit
    def test_get_for_config_io_set_for_a_local_env_returns_a_local_mapping_for_a_given_key(self, expected_s3_csv_local_mapping):
        # Given
        input_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/test_input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        )

        # When
        s3_csv_local_mapping = input_config.get(source_key="READ_FROM_S3_CSV")

        # Then
        assert s3_csv_local_mapping == expected_s3_csv_local_mapping

    @pytest.mark.unit
    def test_get_for_config_io_set_for_a_cloud_env_returns_a_cloud_mapping_for_an_s3_csv_key(self, expected_s3_csv_cloud_mapping):
        # Given
        input_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/test_input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        )

        # When
        s3_csv_cloud_mapping = input_config.get(source_key="READ_FROM_S3_CSV")

        # Then
        assert s3_csv_cloud_mapping == expected_s3_csv_cloud_mapping

    @pytest.mark.unit
    def test_get_for_config_io_set_for_a_cloud_env_returns_a_cloud_mapping_for_an_postgres_key(self, expected_postgres_cloud_mapping):
        # Given
        input_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/test_input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        )

        # When
        postgres_cloud_mapping = input_config.get(source_key="READ_FROM_POSTGRES")

        # Then
        assert postgres_cloud_mapping == expected_postgres_cloud_mapping


class TestSafeDynamicLoader:  # pylint: disable=R0903
    @pytest.mark.unit
    def test_replaces_all_template_instances(self):
        file_contents = 'abc: "[[ VALUE_1 ]]/[[ VALUE_2 ]]"'

        class MockEnvironmentModule:  # pylint: disable=R0903
            VALUE_1 = "abc"
            VALUE_2 = "def"

        result = yaml.load(io.StringIO(file_contents), SafeDynamicLoader.with_module(MockEnvironmentModule))

        assert result == {"abc": "abc/def"}
