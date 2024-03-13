# pylint: disable=no-member, missing-module-docstring, missing-class-docstring, missing-function-docstring, too-many-public-methods, too-few-public-methods, protected-access, C0103, C0302, R0801
import asyncio
import os
import time
from typing import Mapping, Tuple
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

import dynamicio
from dynamicio.config import IOConfig
from tests import constants
from tests.conftest import max_pklproto_hdf
from tests.constants import TEST_RESOURCES
from tests.mocking.io import (
    AsyncReadS3HdfIO,
    ReadFromBatchLocalHdf,
    ReadFromBatchLocalParquet,
    ReadPostgresIO,
    ReadS3CsvIO,
    ReadS3DataWithLessColumnsAndMessedOrderOfColumnsIO,
    ReadS3DataWithLessColumnsIO,
    ReadS3HdfIO,
    ReadS3JsonIO,
    ReadS3ParquetIO,
    TemplatedFile,
    WriteKafkaIO,
    WritePostgresIO,
    WriteS3CsvIO,
    WriteS3HdfIO,
    WriteS3ParquetIO,
)
from tests.mocking.models import ERModel


class TestLocalIO:
    @pytest.mark.unit
    def test_read_parquet_pandas_reader_will_only_load_columns_in_schema(self, expected_df_with_less_columns):
        # Given
        # source data read from: "[[ TEST_RESOURCES ]]/data/input/some_parquet_to_read.parquet"
        s3_parquet_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PARQUET")

        # When
        s3_parquet_df = ReadS3DataWithLessColumnsIO(source_config=s3_parquet_local_config).read()

        # Then
        assert expected_df_with_less_columns.equals(s3_parquet_df)

    @pytest.mark.unit
    def test_read_json_pandas_reader_will_maintain_columns_order_of_the_original_dataset_when_filtering_out_columns(
        self,
    ):
        # Given
        # source data read from: "[[ TEST_RESOURCES ]]/data/definitions/external.yaml/json_with_more_columns.json"
        s3_json_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/external.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_JSON")

        # When
        s3_json_df = ReadS3DataWithLessColumnsAndMessedOrderOfColumnsIO(source_config=s3_json_local_config).read()

        # Then
        assert s3_json_df.columns.to_list() == ["foo_name", "bar", "bar_type", "a_number", "b_number"]

    @pytest.mark.unit
    def test_read_hdf_pandas_reader_will_maintain_columns_order_of_the_original_dataset_when_filtering_out_columns(
        self,
    ):
        # Given
        # source data read from: "[[ TEST_RESOURCES ]]/data/definitions/external.yaml/h5_with_more_columns.h5"
        s3_hdf_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/external.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_HDF")

        # When
        s3_hdf_df = ReadS3DataWithLessColumnsAndMessedOrderOfColumnsIO(source_config=s3_hdf_local_config).read()

        # Then
        assert s3_hdf_df.columns.to_list() == ["foo_name", "bar", "bar_type", "a_number", "b_number"]

    @pytest.mark.unit
    def test_read_csv_pandas_reader_will_only_load_columns_in_schema(self, expected_df_with_less_columns):
        # Given
        # source data read from: "[[ TEST_RESOURCES ]]/data/input/some_csv_to_read.csv"
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV_ALT")

        # When
        s3_csv_df = ReadS3DataWithLessColumnsIO(source_config=s3_csv_local_config).read()

        # Then
        assert expected_df_with_less_columns.equals(s3_csv_df)

    @pytest.mark.unit
    def test_read_h5_pandas_reader_will_only_load_columns_in_schema(self, expected_df_with_less_columns):
        # Given
        # source data read from: "[[ TEST_RESOURCES ]]/data/input/some_hdf_to_read.h5"
        s3_parquet_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_HDF")

        # When
        s3_hdf_df = ReadS3DataWithLessColumnsIO(source_config=s3_parquet_local_config).read()

        # Then
        assert expected_df_with_less_columns.equals(s3_hdf_df)

    @pytest.mark.unit
    def test_read_json_pandas_reader_will_only_load_columns_in_schema(self, expected_df_with_less_columns):
        # Given
        # source data read from: "[[ TEST_RESOURCES ]]/data/input/some_json_to_read.json"
        s3_json_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_JSON")

        # When
        s3_json_df = ReadS3DataWithLessColumnsIO(source_config=s3_json_local_config).read()

        # Then
        assert expected_df_with_less_columns.equals(s3_json_df)

    @pytest.mark.unit
    def test_read_json_pandas_reader_will_only_filter_out_columns_not_in_schema(self, expected_df_with_less_columns):
        # Given
        s3_json_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_JSON")

        # When
        s3_json_df = ReadS3DataWithLessColumnsIO(source_config=s3_json_local_config).read()

        # Then
        assert expected_df_with_less_columns.equals(s3_json_df)

    @pytest.mark.unit
    def test_read_hdf_pandas_reader_will_only_filter_out_columns_not_in_schema(self, expected_df_with_less_columns):
        # Given
        s3_hdf_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_HDF")

        # When
        s3_hdf_df = ReadS3DataWithLessColumnsIO(source_config=s3_hdf_local_config).read()

        # Then
        assert expected_df_with_less_columns.equals(s3_hdf_df)

    @pytest.mark.unit
    @patch.object(dynamicio.mixins.with_local.WithLocal, "_read_from_local")
    def test_local_reader_is_called_for_loading_any_file_when_env_is_set_to_local(self, mock__read_from_local, expected_s3_csv_df):
        # Given
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")
        mock__read_from_local.return_value = expected_s3_csv_df

        # When
        ReadS3CsvIO(source_config=s3_csv_local_config).read()

        # Then
        mock__read_from_local.assert_called()

    @pytest.mark.unit
    def test_a_local_parquet_file_is_loaded_when_io_config_is_initialised_with_local_env_and_parquet_file_type(self, test_df):
        # Given
        pg_parquet_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

        # When
        pg_parquet_df = ReadPostgresIO(source_config=pg_parquet_local_config).read()

        # Then
        assert test_df.equals(pg_parquet_df)

    @pytest.mark.unit
    def test_a_local_h5_file_is_loaded_when_io_config_is_initialised_with_local_env_and_hdf_file_type(self, expected_s3_hdf_df):
        # Given
        s3_hdf_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_HDF")

        # When
        s3_hdf_df = ReadS3HdfIO(source_config=s3_hdf_local_config).read()

        # Then
        assert expected_s3_hdf_df.equals(s3_hdf_df)

    @pytest.mark.unit
    def test_a_local_json_file_is_loaded_when_io_config_is_initialised_with_local_env_and_json_file_type(self, expected_s3_json_df):
        # Given
        s3_json_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_JSON")

        # When
        options = {"orient": "columns"}
        s3_json_df = ReadS3JsonIO(source_config=s3_json_local_config, **options).read()

        # Then
        assert expected_s3_json_df.equals(s3_json_df)

    @pytest.mark.unit
    def test_a_local_csv_file_is_loaded_when_io_config_is_initialised_with_local_env_and_csv_file_type(self, expected_s3_csv_df):
        # Given
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When
        s3_csv_df = ReadS3CsvIO(source_config=s3_csv_local_config).read()

        # Then
        assert expected_s3_csv_df.equals(s3_csv_df)

    @pytest.mark.unit
    def test_a_local_parquet_file_is_loaded_when_io_config_is_set_with_local_env_a_parquet_file_type_for_postgres(self, test_df):
        # Given
        pg_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

        # When
        pg_df = ReadPostgresIO(source_config=pg_local_config, model=ERModel).read()

        # Then
        assert test_df.equals(pg_df)

    @pytest.mark.unit
    @patch.object(dynamicio.mixins.with_local.WithLocal, "_write_to_local")
    def test_local_writer_is_called_for_writing_any_file_when_env_is_set_to_local(self, mock__write_to_local):
        # Given
        df = pd.DataFrame.from_dict({"id": [3, 2, 1, 0], "foo_name": ["a", "b", "c", "d"], "bar": [1, 2, 3, 4]})

        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_CSV")

        # When
        WriteS3CsvIO(source_config=s3_csv_local_config).write(df)

        # Then
        mock__write_to_local.assert_called()

    @pytest.mark.unit
    def test_a_df_is_written_locally_as_parquet_when_io_config_is_initialised_with_local_env_value_and_parquet_file_type(
        self,
        test_df,
    ):
        # Given
        df = test_df

        pg_parquet_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_PG_PARQUET")

        # When
        WritePostgresIO(source_config=pg_parquet_local_config).write(df)

        # Then
        try:
            assert os.path.isfile(pg_parquet_local_config.local.file_path)
        finally:
            os.remove(pg_parquet_local_config.local.file_path)

    @pytest.mark.unit
    def test_a_df_is_written_locally_as_csv_when_io_config_is_initialised_with_local_env_value_and_csv_file_type(
        self,
    ):
        # Given
        df = pd.DataFrame.from_dict({"id": [3, 2, 1, 0], "foo_name": ["a", "b", "c", "d"], "bar": [1, 2, 3, 4]})

        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_CSV")

        # When
        WriteS3CsvIO(source_config=s3_csv_local_config).write(df)

        # Then
        try:
            assert os.path.isfile(s3_csv_local_config.local.file_path)
        finally:
            os.remove(s3_csv_local_config.local.file_path)

    @pytest.mark.unit
    def test_a_df_is_written_locally_as_json_when_io_config_is_initialised_with_local_env_value_and_json_file_type(self, input_messages_df):
        # Given
        df = input_messages_df

        kafka_json_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")

        # When
        WriteKafkaIO(source_config=kafka_json_local_config).write(df)

        # Then
        try:
            assert os.path.isfile(kafka_json_local_config.local.file_path)
        finally:
            os.remove(kafka_json_local_config.local.file_path)

    @pytest.mark.unit
    def test_a_df_is_written_locally_as_h5_when_io_config_is_initialised_with_local_env_value_and_hdf_file_type(
        self,
    ):
        # Given
        df = pd.DataFrame.from_dict({"col_1": [3, 2, 1, 0], "col_2": ["a", "b", "c", "d"]})

        s3_hdf_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_HDF")

        # When
        WriteS3HdfIO(source_config=s3_hdf_local_config).write(df)

        # Then
        try:
            assert os.path.isfile(s3_hdf_local_config.local.file_path)
        finally:
            os.remove(s3_hdf_local_config.local.file_path)

    @pytest.mark.unit
    def test_dynamicio_default_pickle_protocol_is_4(
        self,
    ):
        # Given
        df = pd.DataFrame.from_dict({"col_1": [3, 2, 1, 0], "col_2": ["a", "b", "c", "d"]})

        s3_hdf_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_HDF")

        # When
        WriteS3HdfIO(source_config=s3_hdf_local_config).write(df)

        # Then
        try:
            assert max_pklproto_hdf(s3_hdf_local_config.local.file_path) == 4
        finally:
            os.remove(s3_hdf_local_config.local.file_path)

    @pytest.mark.unit
    def test_dynamicio_default_pickle_protocol_is_bypassed_by_user_input(
        self,
    ):
        # Given
        df = pd.DataFrame.from_dict({"col_1": [3, 2, 1, 0], "col_2": ["a", "b", "c", "d"]})

        s3_hdf_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_HDF")

        # When
        WriteS3HdfIO(source_config=s3_hdf_local_config, protocol=5).write(df)

        # Then
        try:
            assert max_pklproto_hdf(s3_hdf_local_config.local.file_path) == 5
        finally:
            os.remove(s3_hdf_local_config.local.file_path)

    @pytest.mark.unit
    def test_read_resolves_file_path_if_templated_for_some_input_data(self):
        # source data read from: "[[ TEST_RESOURCES ]]/data/input/some_csv_to_read.parquet"
        config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="TEMPLATED_FILE_PATH")

        io_object = TemplatedFile(source_config=config, file_name_to_replace="some_csv_to_read")

        with patch.object(io_object, "_read_csv_file") as mocked__read_csv_file:
            mocked__read_csv_file.return_value = pd.read_csv(os.path.join(TEST_RESOURCES, "data/input/some_csv_to_read.csv"))
            io_object.read()

        mocked__read_csv_file.assert_called_once_with(
            config.local.file_path.format(file_name_to_replace="some_csv_to_read"),
            io_object.schema,
        )

    @pytest.mark.unit
    def test_write_resolves_file_path_if_templated_for_some_output_data(self):
        # source data read from: "[[ TEST_RESOURCES ]]/data/input/some_csv_to_read.parquet"
        config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="TEMPLATED_FILE_PATH")

        io_object = TemplatedFile(source_config=config, file_name_to_replace="some_csv_to_read")

        df = pd.read_csv(os.path.join(TEST_RESOURCES, "data/input/some_csv_to_read.csv"))
        with patch.object(io_object, "_write_csv_file") as mocked__write_csv_file:
            io_object.write(df)

        mocked__write_csv_file.assert_called_once()
        (called_with_df, called_with_file_path) = mocked__write_csv_file.call_args[0]
        pd.testing.assert_frame_equal(df, called_with_df)
        assert called_with_file_path == config.local.file_path.format(file_name_to_replace="some_csv_to_read")

    @pytest.mark.integration
    def test_local_writers_only_write_out_castable_columns_according_to_the_io_schema_case_float64_to_int64_id(
        self,
    ):

        # Given
        # Note col_1 will be interpreted with type float64
        input_df = pd.DataFrame.from_dict({"col_1": [3.0, 2.0, 1.0], "col_2": ["a", "b", "c"], "col_3": ["a", "b", "c"]})

        s3_parquet_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_PARQUET")

        # When
        # class WriteS3ParquetIO(DynamicDataIO):
        #     schema = {"col_1": "int64", "col_2": "object"}
        #
        #     @staticmethod
        #     def validate(df: pd.DataFrame):
        #         pass
        write_s3_io = WriteS3ParquetIO(source_config=s3_parquet_local_config)
        write_s3_io.write(input_df)

        # # Then
        try:
            output_df = pd.read_parquet(s3_parquet_local_config.local.file_path)
            assert list(output_df.dtypes) == [
                np.dtype("int64"),
                np.dtype("O"),
            ]  # order of the list matters
        finally:
            os.remove(s3_parquet_local_config.local.file_path)

    @pytest.mark.integration
    def test_local_writers_only_write_out_columns_in_a_provided_io_schema(self):

        # Given
        input_df = pd.DataFrame.from_dict({"col_1": [3, 2, 1], "col_2": ["a", "b", "c"], "col_3": ["a", "b", "c"]})

        s3_parquet_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_PARQUET")

        # When
        # class WriteS3ParquetIO(DynamicDataIO):
        #     schema = {"col_1": "int64", "col_2": "object"}
        #
        #     @staticmethod
        #     def validate(df: pd.DataFrame):
        #         pass
        write_s3_io = WriteS3ParquetIO(source_config=s3_parquet_local_config)
        write_s3_io.write(input_df)

        # Then
        try:
            output_df = pd.read_parquet(s3_parquet_local_config.local.file_path)
            no_of_columns_of_output_df = len(list(output_df.columns))
            no_of_columns_of_input_df = len(list(input_df.columns))
            assert (no_of_columns_of_input_df - no_of_columns_of_output_df == 1) and (set(output_df.columns) == {*write_s3_io.schema.columns.keys()})  # pylint: disable=no-member
        finally:
            os.remove(s3_parquet_local_config.local.file_path)

    @pytest.mark.unit
    def test_pyarrow_is_used_as_backend_parquet(self):

        # When
        implementation = dynamicio.mixins.with_local.pd.io.parquet.get_engine("auto")

        # Then
        assert implementation.__class__.__name__ == "PyArrowImpl"

    @pytest.mark.integration
    def test_write_parquet_file_is_called_with_additional_pyarrow_args(self):

        # Given
        input_df = pd.DataFrame.from_dict({"col_1": [3, 2, 1], "col_2": ["a", "b", "c"], "col_3": ["a", "b", "c"]})

        s3_parquet_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_PARQUET")

        to_parquet_kwargs = {
            "use_deprecated_int96_timestamps": False,
            "coerce_timestamps": "ms",
            "allow_truncated_timestamps": True,
            "row_group_size": 1000000,
        }

        # When
        with patch.object(dynamicio.mixins.with_local.pd.DataFrame, "to_parquet") as mocked__to_parquet:
            write_s3_io = WriteS3ParquetIO(source_config=s3_parquet_local_config, **to_parquet_kwargs)
            write_s3_io.write(input_df)

        # Then
        mocked__to_parquet.assert_called_once_with(os.path.join(constants.TEST_RESOURCES, "data/processed/write_some_parquet.parquet"), **to_parquet_kwargs)

    @pytest.mark.integration
    @patch.object(dynamicio.mixins.with_local.pd, "read_parquet")
    def test_read_parquet_file_is_called_with_additional_pyarrow_args(self, mock__read_parquet):

        # Given
        config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="S3_PARQUET_WITH_OPTIONS_IN_CODE")

        read_parquet_kwargs = {"filters": [("a", "<", "2")]}

        # When
        ReadFromBatchLocalParquet(config, **read_parquet_kwargs).read()
        # Then
        mock__read_parquet.assert_called_once_with(config.local.file_path, columns=["id", "foo_name", "bar"], **read_parquet_kwargs)

    @pytest.mark.unit
    def test_read_with_pyarrow_is_called_as_default_when_no_engine_option_is_provided(self):
        # Given
        config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PARQUET")

        # When
        with patch.object(dynamicio.mixins.with_local.WithLocal, "_WithLocal__read_with_pyarrow") as mocked__read_with_pyarrow:
            ReadS3ParquetIO(config).read()

        # Then
        mocked__read_with_pyarrow.assert_called_once_with(config.local.file_path, columns=["id", "foo_name", "bar"])

    @pytest.mark.unit
    def test_read_with_pyarrow_is_called_when_engine_option_is_set_to_pyarrow(self):
        # Given
        config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PARQUET")

        # When
        with patch.object(dynamicio.mixins.with_local.WithLocal, "_WithLocal__read_with_pyarrow") as mocked__read_with_pyarrow:
            ReadS3ParquetIO(config, engine="pyarrow").read()

        # Then
        mocked__read_with_pyarrow.assert_called_once_with(config.local.file_path, engine="pyarrow", columns=["id", "foo_name", "bar"])

    @pytest.mark.unit
    def test_read_with_fastparquet_is_called_when_engine_option_is_set_to_fastparquet(self):
        # Given
        config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PARQUET")

        # When
        with patch.object(dynamicio.mixins.with_local.WithLocal, "_WithLocal__read_with_fastparquet") as mocked__read_with_fastparquet:
            ReadS3ParquetIO(config, engine="fastparquet").read()

        # Then
        mocked__read_with_fastparquet.assert_called_once_with(config.local.file_path, engine="fastparquet", columns=["id", "foo_name", "bar"])

    @pytest.mark.unit
    def test_write_with_pyarrow_is_called_as_default_when_no_engine_option_is_provided(self):
        # Given
        input_df = pd.DataFrame.from_dict({"col_1": [3, 2, 1], "col_2": ["a", "b", "c"], "col_3": ["a", "b", "c"]})

        config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_PARQUET")

        # When
        with patch.object(dynamicio.mixins.with_local.WithLocal, "_WithLocal__write_with_pyarrow") as mocked__write_with_pyarrow:
            WriteS3ParquetIO(config).write(input_df)

        # Then
        mocked__write_with_pyarrow.assert_called()

    @pytest.mark.unit
    def test_write_with_pyarrow_is_called_when_engine_option_is_set_to_pyarrow(self):
        # Given
        input_df = pd.DataFrame.from_dict({"col_1": [3, 2, 1], "col_2": ["a", "b", "c"], "col_3": ["a", "b", "c"]})

        config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_PARQUET")

        # When
        with patch.object(dynamicio.mixins.with_local.WithLocal, "_WithLocal__write_with_pyarrow") as mocked__write_with_pyarrow:
            WriteS3ParquetIO(config, engine="pyarrow").write(input_df)

        # Then
        mocked__write_with_pyarrow.assert_called()

    @pytest.mark.unit
    def test_write_with_fastparquet_is_called_when_engine_option_is_set_to_fastparquet(self):
        # Given
        input_df = pd.DataFrame.from_dict({"col_1": [3, 2, 1], "col_2": ["a", "b", "c"], "col_3": ["a", "b", "c"]})

        config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_PARQUET")

        # When
        with patch.object(dynamicio.mixins.with_local.WithLocal, "_WithLocal__write_with_fastparquet") as mocked__write_with_fastparquet:
            WriteS3ParquetIO(config, engine="fastparquet").write(input_df)

        # Then
        mocked__write_with_fastparquet.assert_called()

    @pytest.mark.unit
    def test_async_read_does_not_operate_in_parallel_for_hdf_files(self):

        # Given
        s3_hdf_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_HDF")

        async def multi_read(config: Mapping[str, str]) -> Tuple:
            return await asyncio.gather(
                AsyncReadS3HdfIO(source_config=config).async_read(),
                AsyncReadS3HdfIO(source_config=config).async_read(),
            )

        def dummy_read_hdf(*args, **kwargs) -> pd.DataFrame:  # pylint: disable=unused-argument
            time.sleep(0.1)
            return pd.DataFrame.from_dict({"col_1": [3, 2, 1, 0], "col_2": ["a", "b", "c", "d"]})

        # When
        with patch.object(dynamicio.mixins.with_local.pd, "read_hdf", new=dummy_read_hdf):
            start_time = time.time()
            asyncio.run(multi_read(s3_hdf_cloud_config))
            duration = time.time() - start_time

        # Then
        assert duration >= 0.2

    @pytest.mark.unit
    def test_async_write_does_not_operate_in_parallel_for_hdf_files(self):

        # Given
        df = pd.DataFrame.from_dict({"col_1": [3, 2, 1, 0], "col_2": ["a", "b", "c", "d"]})
        s3_hdf_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_HDF")

        async def multi_write(config: Mapping[str, str], _df: pd.DataFrame) -> Tuple:
            return await asyncio.gather(WriteS3HdfIO(source_config=config).async_write(_df), WriteS3HdfIO(source_config=config).async_write(_df))

        @dynamicio.mixins.utils.allow_options([*dynamicio.mixins.utils.args_of(pd.DataFrame.to_hdf), *["protocol"]])
        def dummy_to_hdf(*args, **kwargs):  # pylint: disable=unused-argument
            time.sleep(0.1)

        # When
        with patch.object(dynamicio.mixins.with_local.pd.DataFrame, "to_hdf", new=dummy_to_hdf):
            start_time = time.time()
            asyncio.run(multi_write(s3_hdf_local_config, df))
            duration = time.time() - start_time

        # Then
        assert duration >= 0.2


class TestBatchLocal:
    @pytest.mark.unit
    def test_multiple_files_are_loaded_when_batch_local_type_is_used_for_parquet(self, expected_s3_parquet_df):
        # Given
        parquet_local_batch_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_BATCH_LOCAL_PARQUET")
        expected_concatenated_df = expected_s3_parquet_df

        # When
        concatenated_df = ReadFromBatchLocalParquet(source_config=parquet_local_batch_config).read()

        # Then
        pd.testing.assert_frame_equal(expected_concatenated_df, concatenated_df)

    @pytest.mark.unit
    def test_multiple_files_are_loaded_when_batch_local_type_is_used_for_parquet_with_templated_string(self, expected_s3_parquet_df):
        # Given
        parquet_local_batch_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_BATCH_LOCAL_TEMPLATED_PARQUET")
        expected_concatenated_df = expected_s3_parquet_df

        # When
        concatenated_df = ReadFromBatchLocalParquet(source_config=parquet_local_batch_config, templated="batch").read()

        # Then
        pd.testing.assert_frame_equal(expected_concatenated_df, concatenated_df)

    @pytest.mark.unit
    def test_files_that_dont_comply_to_the_provided_file_type_are_ignored(self, expected_s3_parquet_df):
        # Given
        parquet_local_batch_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_BATCH_LOCAL_NOT_JUST_PARQUET")
        expected_concatenated_df = expected_s3_parquet_df

        # When
        concatenated_df = ReadFromBatchLocalParquet(source_config=parquet_local_batch_config).read()

        # Then
        pd.testing.assert_frame_equal(expected_concatenated_df, concatenated_df)

    @pytest.mark.unit
    def test_if_hdf_file_is_chosen_then_file_type_is_converted_to_h5_for_filtering(self, expected_s3_parquet_df):
        # Given
        parquet_local_batch_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_BATCH_LOCAL_NOT_JUST_PARQUET")
        expected_concatenated_df = expected_s3_parquet_df

        # When
        concatenated_df = ReadFromBatchLocalParquet(source_config=parquet_local_batch_config).read()

        # Then
        pd.testing.assert_frame_equal(expected_concatenated_df, concatenated_df)

    @pytest.mark.unit
    def test_multiple_files_are_loaded_when_batch_local_type_is_used_for_hdf(self, expected_s3_hdf_df):
        # Given
        parquet_local_batch_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_BATCH_LOCAL_HDF")
        expected_concatenated_df = expected_s3_hdf_df

        # When
        concatenated_df = ReadFromBatchLocalHdf(source_config=parquet_local_batch_config).read()

        # Then
        pd.testing.assert_frame_equal(expected_concatenated_df, concatenated_df.sort_values(by="id").reset_index(drop=True))

    @pytest.mark.unit
    def test_multiple_files_are_loaded_when_batch_local_type_is_used_for_templated_parquet_paths(self, expected_concatenated_01_df, expected_concatenated_02_df):
        # Given
        parquet_local_batch_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_DYNAMIC_FROM_BATCH_LOCAL_PARQUET")

        # When
        concatenated_01_df = ReadFromBatchLocalParquet(source_config=parquet_local_batch_config, runner_id="01").read()
        concatenated_02_df = ReadFromBatchLocalParquet(source_config=parquet_local_batch_config, runner_id="02").read()

        # Then
        pd.testing.assert_frame_equal(expected_concatenated_01_df, concatenated_01_df)
        pd.testing.assert_frame_equal(expected_concatenated_02_df, concatenated_02_df)
