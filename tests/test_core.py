# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, too-many-public-methods, R0801
import asyncio
import logging
import os
import time
from typing import Mapping, Tuple
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

import dynamicio
from dynamicio.config import IOConfig
from dynamicio.core import CASTING_WARNING_MSG, DynamicDataIO
from dynamicio.errors import ColumnsDataTypeError, SchemaNotFoundError, SchemaValidationError
from dynamicio.mixins import WithS3File
from tests import constants
from tests.mocking.io import (
    CsvWithSomeBool,
    HdfWithSomeBool,
    JsonWithSomeBool,
    ParquetWithCustomValidate,
    ParquetWithSomeBool,
    ReadMockS3CsvIO,
    ReadS3CsvIO,
    ReadS3DataWithFalseTypes,
    ReadS3IO,
    ReadS3ParquetIO,
    WriteS3CsvIO,
    WriteS3CsvWithSchema,
    WriteS3ParquetExternalIO,
)


@pytest.fixture(autouse=True, scope="module")
def propagate_logger():
    # We need this because otherwise caplog can't capture the logs
    logging.getLogger("dynamicio.metrics").propagate = True
    yield
    logging.getLogger("dynamicio.metrics").propagate = False


class TestCoreIO:
    @pytest.mark.unit
    def test_abstract_class_dynamic_data_io_cant_be_used_for_object_instantiation(self):
        # Given
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When/Then
        with pytest.raises(TypeError):
            DynamicDataIO(source_config=s3_csv_local_config)

    @pytest.mark.unit
    def test_objects_of_dynamic_data_io_subclasses_cant_be_instantiated_in_the_absence_of_a_non_empty_schema(
        self,
    ):
        # Given
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When/Then
        with pytest.raises(AssertionError):

            class AbsentSchemaIO(DynamicDataIO):
                pass

            AbsentSchemaIO(source_config=s3_csv_local_config)

    @pytest.mark.unit
    def test_objects_of_s3io_subclasses_cant_be_instantiated_in_the_presence_of_a_empty_dict_schema(
        self,
    ):
        # Given
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # Given/When/Then
        with pytest.raises(ValueError):

            class EmptySchemaIO(WithS3File, DynamicDataIO):
                dataset_name = "EmptySchema"
                schema = {}

            EmptySchemaIO(source_config=s3_csv_local_config)

    @pytest.mark.unit
    def test_objects_of_dynamic_data_io_subclasses_cant_be_instantiated_in_the_presence_of_a_schema_eq_to_none(
        self,
    ):
        # Given
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When/Then
        with pytest.raises(ValueError):

            class NoneSchemaIO(WithS3File, DynamicDataIO):
                dataset_name = "NoneSchema"
                schema = None

            NoneSchemaIO(source_config=s3_csv_local_config)

    @pytest.mark.unit
    def test_dynamic_data_io_object_instantiation_is_only_possible_for_subclasses(self):
        # Given
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When
        s3_csv_io = ReadS3CsvIO(source_config=s3_csv_local_config)

        # Then
        assert isinstance(s3_csv_io, ReadS3CsvIO) and isinstance(s3_csv_io, DynamicDataIO)

    @pytest.mark.unit
    def test_subclasses_of_dynamic_data_io_need_to_define_a_schema(self):
        # Given/When/Then
        with pytest.raises(AssertionError):

            class S3CsvIONoSchema(DynamicDataIO):  # pylint: disable=unused-variable
                pass

    @pytest.mark.unit
    def test_subclasses_of_dynamic_data_io_need_to_define_a_static_validate_function(self):
        # Given
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When/Then
        with pytest.raises(AssertionError):

            class CMVolumesIONoValidationFunction(DynamicDataIO):
                schema = {"foo": "int64"}

            CMVolumesIONoValidationFunction(source_config=s3_csv_local_config)

    @pytest.mark.unit
    def test_subclasses_of_dynamic_data_io_need_to_implement_private_reader_for_new_source_types(
        self,
    ):
        # Given
        athena_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/external.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_ATHENA")

        # When
        with pytest.raises(AssertionError):
            ReadS3IO(source_config=athena_cloud_config)

    @pytest.mark.unit
    def test_key_error_is_thrown_for_missing_schema_if_unified_io_subclass_assigns_schema_from_file_but_file_is_missing(
        self,
    ):
        # Given
        read_mock_s3_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/external.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_MOCK_S3_CSV")

        # When
        with pytest.raises(SchemaNotFoundError):
            ReadMockS3CsvIO(source_config=read_mock_s3_cloud_config)

    @pytest.mark.integration
    def test_schema_validations_are_applied_for_an_io_class_with_a_schema_definition(self, valid_dataframe):
        # Given
        df = valid_dataframe
        s3_csv_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")
        io_instance = ReadS3CsvIO(source_config=s3_csv_cloud_config)

        # When
        return_value = io_instance.validate_from_schema(df)

        # Then
        assert io_instance == return_value

    @pytest.mark.integration
    def test_log_metrics_from_schema_are_applied_for_an_io_class_with_a_schema_definition(self, caplog, valid_dataframe):
        # Given
        df = valid_dataframe
        s3_csv_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")
        io_instance = ReadS3CsvIO(source_config=s3_csv_cloud_config)

        # When
        with caplog.at_level(logging.INFO):
            print()
            return_value = io_instance.log_metrics_from_schema(df)

        # Then
        assert (
            io_instance is return_value
            and (len(caplog.records) == 10)
            and (getattr(caplog.records[0], "message") == '{"message": "METRIC", "dataset": "READ_FROM_S3_CSV", "column": "id", "metric": "UniqueCounts", "value": 4.0}')
            and (getattr(caplog.records[1], "message") == '{"message": "METRIC", "dataset": "READ_FROM_S3_CSV", "column": "id", "metric": "Counts", "value": 4.0}')
            and (getattr(caplog.records[2], "message") == '{"message": "METRIC", "dataset": "READ_FROM_S3_CSV", "column": "foo_name-class_a", "metric": "CountsPerLabel", "value": 2.0}')
            and (getattr(caplog.records[3], "message") == '{"message": "METRIC", "dataset": "READ_FROM_S3_CSV", "column": "foo_name-class_b", "metric": "CountsPerLabel", "value": 1.0}')
            and (getattr(caplog.records[4], "message") == '{"message": "METRIC", "dataset": "READ_FROM_S3_CSV", "column": "foo_name-class_c", "metric": "CountsPerLabel", "value": 1.0}')
            and (getattr(caplog.records[5], "message") == '{"message": "METRIC", "dataset": "READ_FROM_S3_CSV", "column": "bar", "metric": "Min", "value": 1500.0}')
            and (getattr(caplog.records[6], "message") == '{"message": "METRIC", "dataset": "READ_FROM_S3_CSV", "column": "bar", "metric": "Max", "value": 1500.0}')
            and (getattr(caplog.records[7], "message") == '{"message": "METRIC", "dataset": "READ_FROM_S3_CSV", "column": "bar", "metric": "Mean", "value": 1500.0}')
            and (getattr(caplog.records[8], "message") == '{"message": "METRIC", "dataset": "READ_FROM_S3_CSV", "column": "bar", "metric": "Std", "value": 0.0}')
            and (getattr(caplog.records[9], "message") == '{"message": "METRIC", "dataset": "READ_FROM_S3_CSV", "column": "bar", "metric": "Variance", "value": 0.0}')
        )

    @pytest.mark.integration
    def test_schema_validations_errors_are_thrown_for_each_validation_if_df_does_not_map_to_schema_definition(self, invalid_dataframe):
        # Given
        df = invalid_dataframe
        s3_csv_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When
        with pytest.raises(SchemaValidationError):
            ReadS3CsvIO(source_config=s3_csv_cloud_config).validate_from_schema(df)

    @pytest.mark.integration
    def test_schema_validations_exception_message_is_a_dict_with_all_violated_validations(self, invalid_dataframe, expected_messages):
        # Given
        df = invalid_dataframe
        s3_csv_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When
        try:
            ReadS3CsvIO(source_config=s3_csv_cloud_config).validate_from_schema(df)
        except SchemaValidationError as _exception:
            # Then
            assert _exception.message.keys() == expected_messages  # pylint: disable=no-member

    @pytest.mark.integration
    def test_local_writers_only_write_out_castable_columns_according_to_the_io_schema_case_float64_to_int64_id(self, dataset_with_more_columns_than_dictated_in_schema):

        # Given
        # Note col_1 will be interpreted with type float64
        input_df = dataset_with_more_columns_than_dictated_in_schema

        s3_parquet_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/external.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_PARQUET")

        # When
        # class WriteS3ParquetExternalIO(UnifiedIO):
        #     schema = {
        #         'bar': 'int64',
        #         'event_type': 'object',
        #         'id': 'int64',
        #         'end_odometer': 'int64',
        #         'foo_name': 'object',
        #     }
        write_s3_io = WriteS3ParquetExternalIO(source_config=s3_parquet_local_config)
        write_s3_io.write(input_df)

        # # Then
        try:
            output_df = pd.read_parquet(s3_parquet_local_config.local.file_path)
            assert output_df.columns.to_list() == [
                "id",
                "foo_name",
                "bar",
                "end_odometer",
                "event_type",
            ]
        finally:
            os.remove(s3_parquet_local_config.local.file_path)

    @pytest.mark.unit
    @patch.object(dynamicio.core.DynamicDataIO, "validate_from_schema")
    def test_schema_validations_are_not_applied_on_read_if_validate_flag_is_false(self, mock_validate_from_schema):
        # Given
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When
        # ReadS3CsvIO(source_config=s3_csv_cloud_config, apply_schema_validations=False).read()
        ReadS3CsvIO(source_config=s3_csv_local_config).read()  # False is the default value

        # Then
        mock_validate_from_schema.assert_not_called()

    @pytest.mark.unit
    @patch.object(dynamicio.core.DynamicDataIO, "validate_from_schema")
    def test_schema_validations_are_automatically_applied_on_read_if_validate_flag_is_true(self, mock_validate_from_schema):
        # Given
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When
        ReadS3CsvIO(source_config=s3_csv_local_config, apply_schema_validations=True).read()

        # Then
        mock_validate_from_schema.assert_called()

    @pytest.mark.unit
    @patch.object(dynamicio.core.DynamicDataIO, "validate_from_schema")
    def test_schema_validations_are_automatically_applied_on_write_if_validate_flag_is_true(self, mock_validate_from_schema, valid_dataframe):
        # Given
        df = valid_dataframe
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/external.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_CSV")

        # When
        WriteS3CsvWithSchema(source_config=s3_csv_local_config, apply_schema_validations=True).write(df)

        # Then
        try:
            mock_validate_from_schema.assert_called()
        finally:
            os.remove(s3_csv_local_config.local.file_path)

    @pytest.mark.unit
    @patch.object(dynamicio.core.DynamicDataIO, "validate_from_schema")
    def test_schema_validations_are_not_applied_on_write_if_validate_flag_is_false(self, mock_validate_from_schema, valid_dataframe):
        # Given
        df = valid_dataframe
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/external.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_CSV")

        # When
        # WriteS3CsvWithSchema(source_config=s3_csv_cloud_config, apply_schema_validations=False).write(df)
        WriteS3CsvWithSchema(source_config=s3_csv_local_config).write(df)  # False is the default value

        # Then
        try:
            mock_validate_from_schema.assert_not_called()
        finally:
            os.remove(s3_csv_local_config.local.file_path)

    @pytest.mark.unit
    @patch.object(dynamicio.core.DynamicDataIO, "log_metrics_from_schema")
    def test_schema_metrics_are_not_logged_on_read_if_metrics_flag_is_false(self, mock_log_metrics_from_schema):
        # Given
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When
        # ReadS3CsvIO(source_config=s3_csv_cloud_config, log_schema_metrics=False).read()
        ReadS3CsvIO(source_config=s3_csv_local_config).read()  # False is the default value

        # Then
        mock_log_metrics_from_schema.assert_not_called()

    @pytest.mark.unit
    @patch.object(dynamicio.core.DynamicDataIO, "log_metrics_from_schema")
    def test_schema_metrics_are_automatically_logged_on_read_if_validate_flag_is_true(self, mock_log_metrics_from_schema):
        # Given
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When
        ReadS3CsvIO(source_config=s3_csv_local_config, log_schema_metrics=True).read()

        # Then
        mock_log_metrics_from_schema.assert_called()

    @pytest.mark.unit
    @patch.object(dynamicio.core.DynamicDataIO, "log_metrics_from_schema")
    def test_schema_metrics_are_automatically_logged_on_write_if_metrics_flag_is_true(self, mock_log_metrics_from_schema, valid_dataframe):
        # Given
        df = valid_dataframe
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/external.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_CSV")

        # When
        WriteS3CsvWithSchema(source_config=s3_csv_local_config, log_schema_metrics=True).write(df)

        # Then
        try:
            mock_log_metrics_from_schema.assert_called()
        finally:
            os.remove(s3_csv_local_config.local.file_path)

    @pytest.mark.unit
    @patch.object(dynamicio.core.DynamicDataIO, "log_metrics_from_schema")
    def test_schema_metrics_are_not_logged_on_write_if_metrics_flag_is_false(self, mock_log_metrics_from_schema, valid_dataframe):
        # Given
        df = valid_dataframe
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/external.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_CSV")

        # When
        # WriteS3CsvWithSchema(source_config=s3_csv_cloud_config, log_schema_metrics=False).write(df)
        WriteS3CsvWithSchema(source_config=s3_csv_local_config).write(df)  # False is the default value

        # Then
        try:
            mock_log_metrics_from_schema.assert_not_called()
        finally:
            os.remove(s3_csv_local_config.local.file_path)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "df, expected_dtype, expected_warning",
        [
            (
                pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 12, "bool_col": False}]),
                "bool",
                None,
            ),
            (
                pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 12, "bool_col": None}]),
                "bool",
                CASTING_WARNING_MSG.format("bool_col", "bool", "object"),
            ),
            (
                pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 12, "bool_col": np.NAN}]),
                "bool",
                CASTING_WARNING_MSG.format("bool_col", "bool", "object"),
            ),
            (
                pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 12, "bool_col": pd.NaT}]),
                "bool",
                CASTING_WARNING_MSG.format("bool_col", "bool", "object"),
            ),
        ],
    )
    def test__has_valid_dtypes_does_not_attempt_to_convert_object_type_to_other_type_unless_other_is_bool_and_column_has_no_non_boolean_values_when_writing_a_parquet(
        self, caplog, df, expected_dtype, expected_warning
    ):
        # Note: In the presence of a boolean cell value in a column, if that column also has numbers or strings, df.to_parquet() will not write it out.
        # It will try to convert it to a bool and it will fail throwing an `pyarrow.lib.ArrowInvalid:` error
        #
        # This makes parquet a safer option from the available filetypes.

        # Given
        s3_parquet_with_some_bool_col_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="S3_PARQUET_WITH_BOOL")

        ParquetWithSomeBool(source_config=s3_parquet_with_some_bool_col_local_config).write(df)

        # Then
        try:
            if caplog.messages:
                assert caplog.messages[0] == expected_warning
            assert pd.read_parquet(s3_parquet_with_some_bool_col_local_config.local.file_path)["bool_col"].dtype.name == expected_dtype
        finally:
            os.remove(s3_parquet_with_some_bool_col_local_config.local.file_path)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "df, expected_dtype, expected_warning",
        [
            (
                pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 12, "bool_col": False}]),
                "bool",
                None,
            ),
            (
                pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 12, "bool_col": None}]),
                "bool",
                CASTING_WARNING_MSG.format("bool_col", "bool", "object"),
            ),
            (
                pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 12, "bool_col": np.NAN}]),
                "bool",
                CASTING_WARNING_MSG.format("bool_col", "bool", "object"),
            ),
            (
                pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 12, "bool_col": 1}]),
                "bool",
                CASTING_WARNING_MSG.format("bool_col", "bool", "object"),
            ),
            (
                pd.DataFrame.from_records(
                    [
                        {"id": 1, "foo_name": "A", "bar": 12, "bool_col": True},
                        {"id": 2, "foo_name": "B", "bar": 12, "bool_col": "random"},
                    ]
                ),
                "bool",
                CASTING_WARNING_MSG.format("bool_col", "bool", "object"),
            ),
            (
                pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 12, "bool_col": pd.NaT}]),
                "bool",
                CASTING_WARNING_MSG.format("bool_col", "bool", "object"),
            ),
        ],
    )
    def test__has_valid_dtypes_does_not_attempt_to_convert_object_type_to_other_type_unless_other_is_bool_and_column_has_no_non_boolean_values_when_writing_a_csv(
        self, caplog, df, expected_dtype, expected_warning
    ):

        # Given
        s3_csv_with_some_bool_col_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="S3_CSV_WITH_BOOL")

        CsvWithSomeBool(source_config=s3_csv_with_some_bool_col_local_config).write(df)

        # Then
        try:
            if caplog.messages:
                assert caplog.messages[0] == expected_warning
            assert pd.read_csv(s3_csv_with_some_bool_col_local_config.local.file_path)["bool_col"].dtype.name == expected_dtype
        finally:
            os.remove(s3_csv_with_some_bool_col_local_config.local.file_path)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "df, expected_dtype, expected_warning",
        [
            (
                pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 12, "bool_col": False}]),
                "bool",
                None,
            ),
            (
                pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 12, "bool_col": None}]),
                "bool",
                CASTING_WARNING_MSG.format("bool_col", "bool", "object"),
            ),
            (
                pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 12, "bool_col": np.NAN}]),
                "bool",
                CASTING_WARNING_MSG.format("bool_col", "bool", "object"),
            ),
            (
                pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 12, "bool_col": 1}]),
                "bool",
                CASTING_WARNING_MSG.format("bool_col", "bool", "object"),
            ),
            (
                pd.DataFrame.from_records(
                    [
                        {"id": 1, "foo_name": "A", "bar": 12, "bool_col": True},
                        {"id": 2, "foo_name": "B", "bar": 12, "bool_col": "random"},
                    ]
                ),
                "bool",
                CASTING_WARNING_MSG.format("bool_col", "bool", "object"),
            ),
            (
                pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 12, "bool_col": pd.NaT}]),
                "bool",
                CASTING_WARNING_MSG.format("bool_col", "bool", "object"),
            ),
        ],
    )
    def test__has_valid_dtypes_does_not_attempt_to_convert_object_type_to_other_type_unless_other_is_bool_and_column_has_no_non_boolean_values_when_writing_a_hdf(
        self, caplog, df, expected_dtype, expected_warning
    ):

        # Given
        s3_hdf_with_some_bool_col_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="S3_HDF_WITH_BOOL")

        HdfWithSomeBool(source_config=s3_hdf_with_some_bool_col_local_config).write(df)

        # Then
        try:
            if caplog.messages:
                assert caplog.messages[0] == expected_warning
            assert pd.read_hdf(s3_hdf_with_some_bool_col_local_config.local.file_path)["bool_col"].dtype.name == expected_dtype
        finally:
            os.remove(s3_hdf_with_some_bool_col_local_config.local.file_path)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "df, expected_dtype, expected_warning",
        [
            (
                pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 12, "bool_col": False}]),
                "bool",
                None,
            ),
            (
                pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 12, "bool_col": None}]),
                "bool",
                CASTING_WARNING_MSG.format("bool_col", "bool", "object"),
            ),
            (
                pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 12, "bool_col": np.NAN}]),
                "bool",
                CASTING_WARNING_MSG.format("bool_col", "bool", "object"),
            ),
            (
                pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 12, "bool_col": 1}]),
                "bool",
                CASTING_WARNING_MSG.format("bool_col", "bool", "object"),
            ),
            (
                pd.DataFrame.from_records(
                    [
                        {"id": 1, "foo_name": "A", "bar": 12, "bool_col": True},
                        {"id": 2, "foo_name": "B", "bar": 12, "bool_col": "random"},
                    ]
                ),
                "bool",
                CASTING_WARNING_MSG.format("bool_col", "bool", "object"),
            ),
            (
                pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 12, "bool_col": pd.NaT}]),
                "bool",
                CASTING_WARNING_MSG.format("bool_col", "bool", "object"),
            ),
        ],
    )
    def test__has_valid_dtypes_does_not_attempt_to_convert_object_type_to_other_type_unless_other_is_bool_and_column_has_no_non_boolean_values_when_writing_a_json(
        self, caplog, df, expected_dtype, expected_warning
    ):

        # Note: In the presence of a boolean cell value in a column, but with additional values of ambiguous type, df.to_json() will try to convert the column
        # to a type `int` or `float`, converting boolean values to numbers to `1.0 : True` and `0.0 : False`, and the rest to NaN. This can cause data corruption issues.

        # Given
        s3_json_with_some_bool_col_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="S3_JSON_WITH_BOOL")

        JsonWithSomeBool(source_config=s3_json_with_some_bool_col_local_config).write(df)

        # Then
        try:
            if caplog.messages:
                assert caplog.messages[0] == expected_warning
            assert pd.read_json(s3_json_with_some_bool_col_local_config.local.file_path)["bool_col"].dtype.name == expected_dtype
        finally:
            os.remove(s3_json_with_some_bool_col_local_config.local.file_path)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "df",
        [
            (pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 12, "bool_col": pd.NA}])),
            (
                pd.DataFrame.from_records(
                    [
                        {"id": 1, "foo_name": "A", "bar": False, "bool_col": True},
                        {"id": 2, "foo_name": "B", "bar": "BAD-VALUE", "bool_col": False},
                    ]
                )
            ),
        ],
    )
    def test__has_valid_dtypes_throws_columns_data_type_error_when_casting_fails(self, df):

        # Note: In the presence of a boolean cell value in a column, but with additional values of ambiguous type, df.to_json() will try to convert the column
        # to a type `int` or `float`, converting boolean values to numbers to `1.0 : True` and `0.0 : False`, and the rest to NaN. This can cause data corruption issues.

        # Given
        s3_parquet_with_some_bool_col_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="S3_PARQUET_WITH_BOOL")

        # Then
        with pytest.raises(ColumnsDataTypeError):
            ParquetWithSomeBool(source_config=s3_parquet_with_some_bool_col_local_config).write(df)

    @pytest.mark.unit
    def test_a_custom_validate_method_can_be_used_to_override_the_default_abstract_one(self):

        # Given
        df = pd.DataFrame.from_records([{"id": 1, "foo_name": "A", "bar": 12, "bool_col": True}, {"id": 2, "foo_name": "B", "bar": 13, "bool_col": False}])
        s3_parquet_with_some_bool_col_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="S3_PARQUET_WITH_CUSTOM_VALIDATE")

        # When
        ParquetWithCustomValidate(source_config=s3_parquet_with_some_bool_col_local_config).write(df)

        # Then
        try:
            pd.testing.assert_frame_equal(pd.read_parquet(s3_parquet_with_some_bool_col_local_config.local.file_path), df)
        finally:
            os.remove(s3_parquet_with_some_bool_col_local_config.local.file_path)

    @pytest.mark.integration
    def test_show_casting_warnings_flag_default_value_prevents_showing_casting_logs(self, caplog):
        # Given
        s3_csv_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")
        io_instance = ReadS3DataWithFalseTypes(source_config=s3_csv_cloud_config)  # i.e.show_casting_warnings=False

        # When
        with caplog.at_level(logging.INFO):
            io_instance.read()

        # Then
        assert len(caplog.records) == 0

    @pytest.mark.integration
    def test_show_casting_warnings_flag_allows_casting_logs_to_be_printed_if_set_to_true(self, caplog):
        # Given
        s3_csv_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")
        io_instance = ReadS3DataWithFalseTypes(source_config=s3_csv_cloud_config, show_casting_warnings=True)

        # When
        with caplog.at_level(logging.INFO):
            io_instance.read()

        # Then
        assert getattr(caplog.records[0], "message") == "Expected: 'float64' dtype for READ_S3_DATA_WITH_FALSE_TYPES['id]', found 'int64'"

    @pytest.mark.unit
    def test_options_are_read_from_code(self):

        # Given
        s3_parquet_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="S3_PARQUET_WITH_OPTIONS_IN_CODE")

        # When
        config_io = ReadS3ParquetIO(source_config=s3_parquet_local_config, option_1=False, option_2=True)

        # Then
        assert config_io.options == {"option_1": False, "option_2": True}

    @pytest.mark.unit
    def test_options_are_read_from_resource_definition(self):
        # Given
        s3_parquet_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="S3_PARQUET_WITH_OPTIONS_IN_DEFINITION")

        # When
        config_io = ReadS3ParquetIO(source_config=s3_parquet_local_config)

        # Then
        assert config_io.options == {"option_3": False, "option_4": True}

    @pytest.mark.unit
    def test_options_are_that_are_read_from_both_resource_definition_and_code_but_with_no_conflicts_are_merged(self):
        # Given
        s3_parquet_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="S3_PARQUET_WITH_OPTIONS_IN_DEFINITION")

        # When
        config_io = ReadS3ParquetIO(source_config=s3_parquet_local_config, option_1=False, option_2=True)

        # Then
        assert config_io.options == {"option_1": False, "option_2": True, "option_3": False, "option_4": True}

    @pytest.mark.unit
    def test_options_from_code_are_prioritized(self):
        # Given
        s3_parquet_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="S3_PARQUET_WITH_OPTIONS_IN_DEFINITION")

        # When
        config_io = ReadS3ParquetIO(source_config=s3_parquet_local_config, option_1=False, option_2=True, option_3=True)  # option_3 is conflicting

        # Then
        assert config_io.options == {"option_1": False, "option_2": True, "option_3": True, "option_4": True}

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "camel_case_string, expected_string",
        [
            ("TestStringABC", "TEST_STRING_ABC"),
            ("TestString", "TEST_STRING"),
            ("ThisIsAnotherTest", "THIS_IS_ANOTHER_TEST"),
            ("AbstractS3Test", "ABSTRACT_S3_TEST"),
            ("YetAnotherGREATTest", "YET_ANOTHER_GREAT_TEST"),
        ],
    )
    def test_transform_class_names_to_dataset_names(self, camel_case_string, expected_string):
        # Given/When
        transformed_string = DynamicDataIO._transform_class_name_to_dataset_name(camel_case_string)  # pylint: disable=W0212

        assert transformed_string == expected_string

    @pytest.mark.unit
    def test_no_options_at_all_are_provided_with_no_issues(self):

        # Given
        s3_parquet_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="S3_PARQUET_WITH_OPTIONS_IN_CODE")

        # When
        config_io = ReadS3ParquetIO(source_config=s3_parquet_local_config)

        # Then
        assert config_io.options == {}

    @pytest.mark.unit
    def test_dataset_name_is_defined_by_io_class_if_schema_from_file_is_not_provided(self):

        # Given
        s3_parquet_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PARQUET")

        # When
        config_io = ReadS3ParquetIO(source_config=s3_parquet_local_config)

        # Then
        assert config_io.name == "READ_S3_PARQUET_IO"

    @pytest.mark.unit
    def test_dataset_name_is_inferred_from_schema_if_schema_from_file_is_provided(self):

        # Given
        s3_read_from_csv_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When
        config_io = ReadS3CsvIO(source_config=s3_read_from_csv_config)

        # Then
        assert config_io.name == "READ_FROM_S3_CSV"


class TestAsyncCoreIO:
    @pytest.mark.unit
    def test_read_is_called_through_async_read(self):
        # Given
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When
        with patch.object(dynamicio.core.DynamicDataIO, "read") as mock_read:
            mock_read.return_value = pd.DataFrame.from_records([[1, "name_a"]], columns=["id", "foo_name"])
            asyncio.run(ReadS3CsvIO(source_config=s3_csv_local_config).async_read())

        # Then
        mock_read.assert_called()

    @pytest.mark.unit
    @pytest.mark.asyncio
    async def test_write_is_called_through_async_write(self):
        # Given
        df = pd.DataFrame.from_dict({"id": [3, 2, 1, 0], "foo_name": ["a", "b", "c", "d"], "bar": [1, 2, 3, 4]})

        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_CSV")

        # When
        with patch.object(dynamicio.core.DynamicDataIO, "write") as mock_write:
            await asyncio.gather(WriteS3CsvIO(source_config=s3_csv_local_config).async_write(df))

        # Then
        mock_write.assert_called()

    @pytest.mark.unit
    def test_async_read_does_indeed_operate_in_parallel(self):
        # Given
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        def dummy_read(self) -> pd.DataFrame:  # pylint: disable=unused-argument
            time.sleep(0.1)
            return pd.DataFrame.from_records([[1, "name_a"]], columns=["id", "foo_name"])

        async def multi_read(config: Mapping[str, str]) -> Tuple:
            return await asyncio.gather(
                ReadS3CsvIO(source_config=config).async_read(),
                ReadS3CsvIO(source_config=config).async_read(),
                ReadS3CsvIO(source_config=config).async_read(),
                ReadS3CsvIO(source_config=config).async_read(),
            )

        # When
        with patch.object(dynamicio.core.DynamicDataIO, "read", new=dummy_read):
            start_time = time.time()
            asyncio.run(multi_read(s3_csv_local_config))
            duration = time.time() - start_time

        # Then
        assert duration < 0.125

    @pytest.mark.unit
    def test_async_write_does_indeed_operate_in_parallel(self):
        # Given
        df = pd.DataFrame.from_dict({"id": [3, 2, 1, 0], "foo_name": ["a", "b", "c", "d"], "bar": [1, 2, 3, 4]})

        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_CSV")

        def dummy_write(self, _df: pd.DataFrame) -> bool:  # pylint: disable=unused-argument
            time.sleep(0.1)
            return True

        async def multi_write(config: Mapping[str, str], _df: pd.DataFrame) -> Tuple:
            return await asyncio.gather(
                WriteS3CsvIO(source_config=config).async_write(_df),
                WriteS3CsvIO(source_config=config).async_write(_df),
                WriteS3CsvIO(source_config=config).async_write(_df),
                WriteS3CsvIO(source_config=config).async_write(_df),
            )

        # When
        with patch.object(dynamicio.core.DynamicDataIO, "read", new=dummy_write):
            start_time = time.time()
            asyncio.run(multi_write(s3_csv_local_config, df))
            duration = time.time() - start_time

        # Then
        assert duration < 0.125
