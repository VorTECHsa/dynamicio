# pylint: disable=no-member, missing-module-docstring, missing-class-docstring, missing-function-docstring, too-many-public-methods, too-few-public-methods, protected-access, C0103, C0302, R0801
import asyncio
import os
import time
from tempfile import NamedTemporaryFile
from typing import Any, Mapping, Tuple
from unittest import mock
from unittest.mock import ANY, MagicMock, patch

import numpy as np
import pandas as pd
import pytest
from kafka import KafkaProducer
from sqlalchemy.sql.base import ImmutableColumnCollection

from dynamicio import WithLocal, WithPostgres, mixins
from dynamicio.config import IOConfig
from dynamicio.errors import ColumnsDataTypeError
from dynamicio.mixins import WithKafka, WithS3File, WithS3PathPrefix, allow_options, args_of, get_string_template_field_names, resolve_template
from tests import constants
from tests.conftest import max_pklproto_hdf
from tests.constants import TEST_RESOURCES
from tests.mocking.io import (
    AsyncReadS3HdfIO,
    MockKafkaProducer,
    ReadFromBatchLocalHdf,
    ReadFromBatchLocalParquet,
    ReadPostgresIO,
    ReadS3CsvIO,
    ReadS3DataWithLessColumnsAndMessedOrderOfColumnsIO,
    ReadS3DataWithLessColumnsIO,
    ReadS3HdfIO,
    ReadS3JsonIO,
    ReadS3ParquetIO,
    ReadS3ParquetWEmptyFilesIO,
    ReadS3ParquetWithDifferentCastableDTypeIO,
    ReadS3ParquetWithDifferentNonCastableDTypeIO,
    ReadS3ParquetWithLessColumnsIO,
    TemplatedFile,
    WriteExtendedPostgresIO,
    WriteKafkaIO,
    WritePostgresIO,
    WriteS3CsvIO,
    WriteS3HdfIO,
    WriteS3JsonIO,
    WriteS3ParquetIO,
)
from tests.mocking.models import ERModel, PgModel


class TestGetStringTemplateFieldNames:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        ["s", "expected_result"],
        [
            ("", []),
            ("abc", []),
            ("{abc}", ["abc"]),
            ("a{abc}d{def}", ["abc", "def"]),
            ("a{0}b{1}", ["0", "1"]),
            ("{abc:.2f}", ["abc"]),
        ],
    )
    def test_returns_correct_result(self, s, expected_result):
        result = get_string_template_field_names(s)
        assert result == expected_result


class TestResolveTemplate:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        ["s", "options", "expected_result"],
        [
            ("{abc}d{def}", {"abc": "100", "def": "hello"}, "100dhello"),
            ("{hello}", {"world": "100", "hello": "world"}, "world"),
        ],
    )
    def test_returns_correct_result(self, s, options, expected_result):
        result = resolve_template(s, options)
        assert result == expected_result

    @pytest.mark.unit
    @pytest.mark.parametrize(["s"], [("abc{0}",), ("{1def}def",)])
    def test_raises_value_error_if_s_has_fields_which_are_not_valid_identifiers(self, s):
        with pytest.raises(ValueError):
            resolve_template(s, None)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        ["s", "options"],
        [("{abc}", {}), ("{abc}", {"def": "something"}), ("{abc}{def}", {"def": "700"})],
    )
    def test_raises_value_error_if_template_field_cannot_be_resolved_to_options(self, s, options):
        with pytest.raises(ValueError):
            resolve_template(s, options)


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
    @patch.object(mixins.WithLocal, "_read_from_local")
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
    @patch.object(WithLocal, "_write_to_local")
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
            assert os.path.isfile(pg_parquet_local_config["local"]["file_path"])
        finally:
            os.remove(pg_parquet_local_config["local"]["file_path"])

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
            assert os.path.isfile(s3_csv_local_config["local"]["file_path"])
        finally:
            os.remove(s3_csv_local_config["local"]["file_path"])

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
            assert os.path.isfile(kafka_json_local_config["local"]["file_path"])
        finally:
            os.remove(kafka_json_local_config["local"]["file_path"])

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
            assert os.path.isfile(s3_hdf_local_config["local"]["file_path"])
        finally:
            os.remove(s3_hdf_local_config["local"]["file_path"])

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
            assert max_pklproto_hdf(s3_hdf_local_config["local"]["file_path"]) == 4
        finally:
            os.remove(s3_hdf_local_config["local"]["file_path"])

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
            assert max_pklproto_hdf(s3_hdf_local_config["local"]["file_path"]) == 5
        finally:
            os.remove(s3_hdf_local_config["local"]["file_path"])

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
            config["local"]["file_path"].format(file_name_to_replace="some_csv_to_read"),
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

        mocked__write_csv_file.assert_called_once_with(
            df,
            config["local"]["file_path"].format(file_name_to_replace="some_csv_to_read"),
        )

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
            output_df = pd.read_parquet(s3_parquet_local_config["local"]["file_path"])
            assert list(output_df.dtypes) == [
                np.dtype("int64"),
                np.dtype("O"),
            ]  # order of the list matters
        finally:
            os.remove(s3_parquet_local_config["local"]["file_path"])

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
            output_df = pd.read_parquet(s3_parquet_local_config["local"]["file_path"])
            no_of_columns_of_output_df = len(list(output_df.columns))
            no_of_columns_of_input_df = len(list(input_df.columns))

            assert (no_of_columns_of_input_df - no_of_columns_of_output_df == 1) and (set(list(output_df.columns)) == {*write_s3_io.schema})  # pylint: disable=no-member
        finally:
            os.remove(s3_parquet_local_config["local"]["file_path"])

    @pytest.mark.unit
    def test_pyarrow_is_used_as_backend_parquet(self):

        # When
        implementation = mixins.pd.io.parquet.get_engine("auto")

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
        with patch.object(mixins.pd.DataFrame, "to_parquet") as mocked__to_parquet:
            write_s3_io = WriteS3ParquetIO(source_config=s3_parquet_local_config, **to_parquet_kwargs)
            write_s3_io.write(input_df)

        # Then
        mocked__to_parquet.assert_called_once_with(os.path.join(constants.TEST_RESOURCES, "data/processed/write_some_parquet.parquet"), **to_parquet_kwargs)

    @pytest.mark.integration
    @patch.object(mixins.pd, "read_parquet")
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
        mock__read_parquet.assert_called_once_with(config["local"]["file_path"], columns=["id", "foo_name", "bar"], **read_parquet_kwargs)

    @pytest.mark.unit
    def test_read_with_pyarrow_is_called_as_default_when_no_engine_option_is_provided(self):
        # Given
        config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PARQUET")

        # When
        with patch.object(mixins.WithLocal, "_WithLocal__read_with_pyarrow") as mocked__read_with_pyarrow:
            ReadS3ParquetIO(config).read()

        # Then
        mocked__read_with_pyarrow.assert_called_once_with(config["local"]["file_path"], columns=["id", "foo_name", "bar"])

    @pytest.mark.unit
    def test_read_with_pyarrow_is_called_when_engine_option_is_set_to_pyarrow(self):
        # Given
        config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PARQUET")

        # When
        with patch.object(mixins.WithLocal, "_WithLocal__read_with_pyarrow") as mocked__read_with_pyarrow:
            ReadS3ParquetIO(config, engine="pyarrow").read()

        # Then
        mocked__read_with_pyarrow.assert_called_once_with(config["local"]["file_path"], engine="pyarrow", columns=["id", "foo_name", "bar"])

    @pytest.mark.unit
    def test_read_with_fastparquet_is_called_when_engine_option_is_set_to_fastparquet(self):
        # Given
        config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PARQUET")

        # When
        with patch.object(mixins.WithLocal, "_WithLocal__read_with_fastparquet") as mocked__read_with_fastparquet:
            ReadS3ParquetIO(config, engine="fastparquet").read()

        # Then
        mocked__read_with_fastparquet.assert_called_once_with(config["local"]["file_path"], engine="fastparquet", columns=["id", "foo_name", "bar"])

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
        with patch.object(mixins.WithLocal, "_WithLocal__write_with_pyarrow") as mocked__write_with_pyarrow:
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
        with patch.object(mixins.WithLocal, "_WithLocal__write_with_pyarrow") as mocked__write_with_pyarrow:
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
        with patch.object(mixins.WithLocal, "_WithLocal__write_with_fastparquet") as mocked__write_with_fastparquet:
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
        with patch.object(mixins.pd, "read_hdf", new=dummy_read_hdf):
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

        @allow_options([*args_of(pd.DataFrame.to_hdf), *["protocol"]])
        def dummy_to_hdf(*args, **kwargs):  # pylint: disable=unused-argument
            time.sleep(0.1)

        # When
        with patch.object(mixins.pd.DataFrame, "to_hdf", new=dummy_to_hdf):
            start_time = time.time()
            asyncio.run(multi_write(s3_hdf_local_config, df))
            duration = time.time() - start_time

        # Then
        assert duration >= 0.2


class TestS3FileIO:
    @pytest.mark.unit
    def test_read_resolves_file_path_if_templated(self):
        # source data read from: "[[ TEST_RESOURCES ]]/data/input/some_csv_to_read.parquet"
        config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="TEMPLATED_FILE_PATH")

        file_path = f"{constants.TEST_RESOURCES}/data/input/some_csv_to_read.csv"

        # When
        with patch.object(WithLocal, "_read_csv_file") as mock__read_csv_file, patch.object(WithS3File, "_s3_reader") as mock_s3_reader:
            with open(file_path, "r") as file:  # pylint: disable=unspecified-encoding
                mock_s3_reader.return_value = file
                TemplatedFile(source_config=config, file_name_to_replace="some_csv_to_read").read()

        mock__read_csv_file.assert_called_once_with(file_path, {"id": "int64", "foo_name": "object", "bar": "int64"})

    @pytest.mark.unit
    def test_write_resolves_file_path_if_templated(self):
        # Given
        # source data read from: "[[ TEST_RESOURCES ]]/data/input/some_csv_to_read.parquet"
        config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="TEMPLATED_FILE_PATH")

        # When
        with patch.object(WithLocal, "_write_csv_file") as mock__write_csv_file:
            df = pd.read_csv(os.path.join(TEST_RESOURCES, "data/input/some_csv_to_read.csv"))
            TemplatedFile(source_config=config, file_name_to_replace="some_csv_to_read").write(df)

        # Then
        args, _ = mock__write_csv_file.call_args
        assert "s3://mock-bucket/path/to/some_csv_to_read.csv" == args[1]

    @pytest.mark.unit
    @patch.object(WithS3File, "_read_from_s3_file")
    def test_read_from_s3_file_is_called_for_loading_a_file_with_env_as_cloud_s3(self, mock__read_from_s3_file, expected_s3_csv_df):
        # Given
        mock__read_from_s3_file.return_value = expected_s3_csv_df
        s3_csv_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When
        ReadS3CsvIO(source_config=s3_csv_cloud_config).read()

        # Then
        mock__read_from_s3_file.assert_called()

    @pytest.mark.unit
    def test_s3_reader_is_not_called_for_loading_a_parquet_with_env_as_cloud_s3_and_type_as_parquet_and_no_disk_space_flag(self):
        # Given
        s3_parquet_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PARQUET")

        file_path = f"{constants.TEST_RESOURCES}/data/input/some_csv_to_read.csv"

        # When
        with patch.object(WithS3File, "_s3_reader") as mock_s3_reader, patch.object(WithS3File, "_read_parquet_file") as mock_read_parquet_file:
            with open(file_path, "r") as file:  # pylint: disable=unspecified-encoding
                mock_s3_reader.return_value = file
                ReadS3ParquetIO(source_config=s3_parquet_cloud_config, no_disk_space=True).read()

        # Then
        mock_s3_reader.assert_not_called()
        mock_read_parquet_file.assert_called()

    @pytest.mark.unit
    def test_s3_reader_is_called_for_loading_a_hdf_with_env_as_cloud_s3_and_type_as_hdf(self):
        # Given
        s3_hdf_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_HDF")

        # When
        with patch.object(WithS3File, "_s3_reader") as mock__s3_reader, patch.object(WithS3File, "_read_hdf_file"):
            ReadS3HdfIO(source_config=s3_hdf_cloud_config, no_disk_space=True).read()

        # Then
        mock__s3_reader.assert_called()

    @pytest.mark.unit
    def test_s3_reader_is_not_called_for_loading_a_json_with_env_as_cloud_s3_and_type_as_json_and_no_disk_space_flag(self):
        # Given
        s3_json_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_JSON")

        # When
        with patch.object(WithS3File, "_s3_reader") as mock__s3_reader, patch.object(WithS3File, "_read_json_file") as mock__read_json_file:
            ReadS3JsonIO(source_config=s3_json_cloud_config, no_disk_space=True).read()

        # Then
        mock__s3_reader.assert_not_called()
        mock__read_json_file.assert_called()

    @pytest.mark.unit
    def test_s3_reader_is_not_called_for_loading_a_csv_with_env_as_cloud_s3_and_type_as_csv_and_no_disk_space_flag(self):
        # Given
        s3_csv_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When
        with patch.object(WithS3File, "_s3_reader") as mock__s3_reader, patch.object(WithS3File, "_read_csv_file") as mock__read_csv_file:
            ReadS3CsvIO(source_config=s3_csv_cloud_config, no_disk_space=True).read()

        # Then
        mock__s3_reader.assert_not_called()
        mock__read_csv_file.assert_called()

    @pytest.mark.unit
    def test_ValueError_is_raised_if_file_path_missing_from_config(self):
        # Given
        s3_csv_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_MISSING_FILE_PATH")

        # When / Then
        with pytest.raises(ValueError):
            ReadS3CsvIO(source_config=s3_csv_cloud_config).read()

    @pytest.mark.unit
    def test_s3_writers_only_validate_schema_prior_writing_out_the_dataframe(self):
        # Given
        input_df = pd.DataFrame.from_dict({"col_1": [3, 2, 1], "col_2": ["a", "b", "c"], "col_3": ["a", "b", "c"]})

        s3_parquet_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_PARQUET")

        # When
        # class WriteS3ParquetIO(DynamicDataIO):
        #     schema = {"col_1": "int64", "col_2": "object"}
        #
        #     @staticmethod
        #     def validate(df: pd.DataFrame):
        #         pass
        with patch.object(WithS3File, "_s3_writer") as mock__s3_writer, patch.object(WriteS3ParquetIO, "_apply_schema") as mock__apply_schema, patch.object(
            WriteS3ParquetIO, "_write_parquet_file"
        ) as mock__write_parquet_file:
            with NamedTemporaryFile(delete=False) as temp_file:
                mock__s3_writer.return_value = temp_file
                WriteS3ParquetIO(source_config=s3_parquet_cloud_config).write(input_df)

        # Then
        mock__apply_schema.assert_called()
        mock__write_parquet_file.assert_called()

    @pytest.mark.unit
    def test_columns_data_type_error_exception_is_not_generated_if_column_dtypes_can_be_casted_to_the_expected_dtypes(self, expected_s3_parquet_df):
        # Given
        s3_parquet_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PARQUET")

        # When
        with patch.object(WithS3File, "_read_parquet_file") as mock__read_parquet_file, patch.object(WithS3File, "_s3_reader"):
            mock__read_parquet_file.return_value = expected_s3_parquet_df
            ReadS3ParquetWithDifferentCastableDTypeIO(source_config=s3_parquet_cloud_config).read()

        assert True, "No exception was raised"

    @pytest.mark.unit
    @patch.object(WithS3File, "_s3_reader")
    @patch.object(WithS3File, "_read_parquet_file")
    def test_columns_data_type_error_exception_is_generated_if_column_dtypes_dont_map_to_the_expected_dtypes(self, mock__s3_reader, moc__read_parquet_file, expected_s3_parquet_df):
        """
        ------------------------------ Captured log call -------------------------------

        WARNING  ...:dataio.py:273 Expected: 'float64' dtype for column: 'id', found: 'int64' instead.
        WARNING  ...:dataio.py:273 Expected: 'int64' dtype for column: 'foo_name', found: 'object' instead.
        ERROR    ...:dataio.py:277 Tried casting column: 'foo_name' to 'int64' from 'object', but failed.

        =========================== short test summary info ============================

        FAILED ...:test_columns_data_type_error_exception_is_generated_if_column_dtypes_dont_map_to_the_expected_dtypes

        ============================== 1 failed in 0.48s ===============================

        """
        # Given
        dataframe_returned = expected_s3_parquet_df
        mock__s3_reader.return_value = dataframe_returned

        s3_parquet_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PARQUET")

        # When/Then
        with pytest.raises(ColumnsDataTypeError):
            ReadS3ParquetWithDifferentNonCastableDTypeIO(source_config=s3_parquet_cloud_config).read()
            moc__read_parquet_file.assert_called()

    @pytest.mark.unit
    def test_read_parquet_file_is_called_while_s3_reader_is_not_for_loading_a_parquet_with_env_as_cloud_s3_and_type_as_parquet_with_no_disk_space_option(
        self,
    ):
        # Given
        s3_parquet_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PARQUET")

        # When
        with patch.object(WithS3File, "_s3_reader") as mock__s3_reader, patch.object(WithLocal, "_read_parquet_file") as mock__read_parquet_file:
            ReadS3ParquetIO(source_config=s3_parquet_cloud_config, no_disk_space=True).read()

        # Then
        mock__s3_reader.assert_not_called()
        mock__read_parquet_file.assert_called()

    @pytest.mark.unit
    @patch.object(WithS3File, "_write_to_s3_file")
    def test_s3_writer_is_called_for_writing_a_file_with_env_is_set_to_cloud_s3(self, mock__write_to_s3_file):
        # Given
        df = pd.DataFrame.from_dict({"id": [3, 2, 1, 0], "foo_name": ["a", "b", "c", "d"], "bar": [1, 2, 3, 4]})

        s3_json_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_JSON")

        # When
        ReadS3HdfIO(source_config=s3_json_local_config).write(df)

        # Then
        mock__write_to_s3_file.assert_called()

    @pytest.mark.unit
    def test_write_parquet_file_is_called_for_writing_a_parquet_with_env_as_cloud_s3_and_type_as_s3(self):
        # Given
        df = pd.DataFrame.from_dict({"col_1": [3, 2, 1, 0], "col_2": ["a", "b", "c", "d"]})

        s3_parquet_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_PARQUET")

        # When
        with patch.object(WithS3File, "_s3_writer") as mock__s3_writer, patch.object(WithLocal, "_write_parquet_file") as mock__write_parquet_file:
            with NamedTemporaryFile(delete=False) as temp_file:
                mock__s3_writer.return_value = temp_file
                WriteS3ParquetIO(source_config=s3_parquet_local_config).write(df)

        # Then
        mock__write_parquet_file.assert_called()

    @pytest.mark.unit
    def test_write_csv_file_is_called_for_writing_a_parquet_with_env_as_cloud_s3_and_type_as_csv(self):
        # Given
        df = pd.DataFrame.from_dict({"id": [3, 2, 1, 0], "foo_name": ["a", "b", "c", "d"], "bar": [1, 2, 3, 4]})

        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_CSV")

        # When
        with patch.object(WithS3File, "_s3_writer") as mock__s3_writer, patch.object(WithLocal, "_write_csv_file") as mock__write_csv_file:
            with NamedTemporaryFile(delete=False) as temp_file:
                mock__s3_writer.return_value = temp_file
                WriteS3CsvIO(source_config=s3_csv_local_config).write(df)

        # Then
        mock__write_csv_file.assert_called()

    @pytest.mark.unit
    def test_write_json_file_is_called_for_writing_a_parquet_with_env_as_cloud_s3_and_type_as_json(self):
        # Given
        df = pd.DataFrame.from_dict({"col_1": [3, 2, 1, 0], "col_2": ["a", "b", "c", "d"]})

        s3_json_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_JSON")

        # When
        with patch.object(WithS3File, "_s3_writer") as mock__s3_writer, patch.object(WithLocal, "_write_json_file") as mock__write_json_file:
            with NamedTemporaryFile(delete=False) as temp_file:
                mock__s3_writer.return_value = temp_file
                WriteS3JsonIO(source_config=s3_json_local_config).write(df)

        # Then
        mock__write_json_file.assert_called()

    @pytest.mark.unit
    def test_write_hdf_file_is_called_for_writing_a_parquet_with_env_as_cloud_s3_and_type_as_hdf(self):
        # Given
        df = pd.DataFrame.from_dict({"col_1": [3, 2, 1, 0], "col_2": ["a", "b", "c", "d"]})
        s3_hdf_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_HDF")

        # When
        with patch.object(WithS3File, "_s3_writer") as mock__s3_writer, patch.object(WithLocal, "_write_hdf_file") as mock__write_hdf_file:
            with NamedTemporaryFile(delete=False) as temp_file:
                mock__s3_writer.return_value = temp_file
                WriteS3HdfIO(source_config=s3_hdf_local_config).write(df)

        # Then
        mock__write_hdf_file.assert_called()


class TestS3PathPrefixIO:
    @pytest.mark.unit
    def test_ValueError_is_raised_if_path_prefix_missing_from_config(self):
        # Given
        s3_csv_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_MISSING_PATH_PREFIX")

        # When / Then
        with pytest.raises(ValueError):
            ReadS3CsvIO(source_config=s3_csv_cloud_config).read()

    @pytest.mark.unit
    def test_ValueError_is_raised_if_path_prefix_missing_from_config_when_uploading(self):
        # Given
        input_df = pd.DataFrame.from_dict({"col_1": [3, 2, 1], "col_2": ["a", "b", "c"], "col_3": ["a", "b", "c"]})
        s3_parquet_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_MISSING_PATH_PREFIX")

        # When / Then
        with pytest.raises(ValueError):
            WriteS3ParquetIO(source_config=s3_parquet_cloud_config).write(input_df)

    @pytest.mark.unit
    def test_ValueError_is_raised_if_partition_cols_missing_from_options_when_uploading(self):
        # Given
        input_df = pd.DataFrame.from_dict({"col_1": [3, 2, 1], "col_2": ["a", "b", "c"], "col_3": ["a", "b", "c"]})
        s3_parquet_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_PATH_PREFIX_PARQUET")

        # When / Then
        with pytest.raises(ValueError):
            WriteS3ParquetIO(source_config=s3_parquet_cloud_config).write(input_df)

    @pytest.mark.unit
    def test_ValueError_is_raised_if_file_type_not_parquet_when_uploading(self):
        # Given
        input_df = pd.DataFrame.from_dict({"col_1": [3, 2, 1], "col_2": ["a", "b", "c"], "col_3": ["a", "b", "c"]})
        s3_parquet_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_PATH_PREFIX_NOT_PARQUET")

        # When / Then
        with pytest.raises(ValueError):
            WriteS3ParquetIO(source_config=s3_parquet_cloud_config).write(input_df)

    @pytest.mark.unit
    @patch.object(WithS3PathPrefix, "_read_from_s3_path_prefix")
    def test_read_from_s3_path_prefix_is_called_for_loading_a_path_prefix_with_env_as_cloud_s3(self, mock__read_from_s3_path_prefix, expected_s3_csv_df):
        # Given
        mock__read_from_s3_path_prefix.return_value = expected_s3_csv_df
        s3_csv_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PATH_PREFIX_CSV")

        # When
        ReadS3CsvIO(source_config=s3_csv_cloud_config).read()

        # Then
        mock__read_from_s3_path_prefix.assert_called()

    @pytest.mark.unit
    @patch.object(WithS3PathPrefix, "_write_to_s3_path_prefix")
    def test_write_to_s3_path_prefix_is_called_for_uploading_to_a_path_prefix_with_env_as_cloud_s3(self, mock__write_to_s3_path_prefix):
        # Given
        input_df = pd.DataFrame.from_dict({"col_1": [3, 2, 1], "col_2": ["a", "b", "c"], "col_3": ["a", "b", "c"]})

        s3_parquet_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_PATH_PREFIX_PARQUET")

        # When
        WriteS3ParquetIO(source_config=s3_parquet_cloud_config).write(input_df)

        # Then
        mock__write_to_s3_path_prefix.assert_called()

    @pytest.mark.unit
    @patch.object(WriteS3ParquetIO, "_write_parquet_file")
    # pylint: disable=unused-argument
    def test_awscli_runner_is_called_with_correct_s3_path_and_aws_command_when_uploading_a_path_prefix_with_env_as_cloud_s3(self, mock__write_parquet_file, mock_temporary_directory):
        # Given
        input_df = pd.DataFrame.from_dict({"col_1": [3, 2, 1], "col_2": ["a", "b", "c"], "col_3": ["a", "b", "c"]})
        s3_parquet_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_PATH_PREFIX_PARQUET")

        # When
        with patch.object(mixins, "awscli_runner") as mocked__awscli_runner:
            WriteS3ParquetIO(source_config=s3_parquet_cloud_config, partition_cols="col_2").write(input_df)

        # Then
        mocked__awscli_runner.assert_called_with("s3", "sync", "temp", "s3://mock-bucket/mock-key", "--acl", "bucket-owner-full-control", "--only-show-errors", "--exact-timestamps")

    @pytest.mark.unit
    # pylint: disable=unused-argument
    def test_awscli_runner_is_called_with_correct_s3_path_and_aws_command_when_loading_a_path_prefix_with_env_as_cloud_s3(
        self, mock_listdir, mock_temporary_directory, mock__read_hdf_file
    ):
        # Given
        s3_hdf_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PATH_PREFIX_HDF")

        # When
        with patch.object(mixins, "awscli_runner") as mocked__awscli_runner:
            ReadS3HdfIO(source_config=s3_hdf_cloud_config).read()

        # Then
        mocked__awscli_runner.assert_called_with("s3", "sync", "s3://mock-bucket/mock-key", "temp", "--acl", "bucket-owner-full-control", "--only-show-errors", "--exact-timestamps")

    @pytest.mark.unit
    # pylint: disable=unused-argument
    def test__read_hdf_file_is_called_with_correct_local_file_path_when_loading_a_path_prefix_with_env_as_cloud_s3_and_type_as_hdf(
        self, mock_listdir, mock_temporary_directory, mock__read_hdf_file
    ):
        # Given
        s3_hdf_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PATH_PREFIX_HDF")

        # When
        with patch.object(mixins, "awscli_runner") as mocked__awscli_runner:
            mocked__awscli_runner.return_value = True
            ReadS3HdfIO(source_config=s3_hdf_cloud_config).read()

        # Then
        assert len(mock__read_hdf_file.mock_calls) == 3
        mock__read_hdf_file.assert_has_calls(
            [
                mock.call("temp/obj_1.h5", {"id": "int64", "foo_name": "object", "bar": "int64"}),
                mock.call("temp/obj_2.h5", {"id": "int64", "foo_name": "object", "bar": "int64"}),
                mock.call("temp/obj_3.h5", {"id": "int64", "foo_name": "object", "bar": "int64"}),
            ]
        )

    @pytest.mark.unit
    # pylint: disable=unused-argument
    def test__read_parquet_file_is_called_with_correct_local_file_path_when_loading_a_path_prefix_with_env_as_cloud_s3_and_type_as_parquet(
        self, mock_listdir, mock_temporary_directory, mock__read_parquet_file
    ):
        # Given
        s3_parquet_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PATH_PREFIX_PARQUET")

        # When
        with patch.object(mixins, "awscli_runner") as mocked__awscli_runner:
            mocked__awscli_runner.return_value = True
            ReadS3ParquetIO(source_config=s3_parquet_cloud_config).read()

        # Then
        assert len(mock__read_parquet_file.mock_calls) == 3
        mock__read_parquet_file.assert_has_calls(
            [
                mock.call("temp/obj_1.h5", {"id": "int64", "foo_name": "object", "bar": "int64"}),
                mock.call("temp/obj_2.h5", {"id": "int64", "foo_name": "object", "bar": "int64"}),
                mock.call("temp/obj_3.h5", {"id": "int64", "foo_name": "object", "bar": "int64"}),
            ]
        )

    @pytest.mark.unit
    def test_read_parquet_file_is_called_while_awscli_runner_is_not_for_loading_a_parquet_with_env_as_cloud_s3_and_type_as_parquet_with_no_disk_space_option(
        self,
    ):
        # Given
        s3_parquet_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PATH_PREFIX_PARQUET")

        # When
        with patch.object(mixins, "awscli_runner") as mock__awscli_runner, patch.object(WithLocal, "_read_parquet_file") as mock__read_parquet_file:
            ReadS3ParquetIO(source_config=s3_parquet_cloud_config, no_disk_space=True).read()

        # Then
        mock__read_parquet_file.assert_called()
        mock__awscli_runner.assert_not_called()

    @pytest.mark.unit
    # pylint: disable=unused-argument
    def test__read_parquet_file_can_read_directory_of_parquet_files_loading_only_necessary_columns(self, mock_parquet_temporary_directory):
        # Given
        s3_parquet_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PATH_PREFIX_PARQUET")

        # When
        with patch.object(mixins, "awscli_runner") as mocked__awscli_runner:
            mocked__awscli_runner.return_value = True
            df = ReadS3ParquetWithLessColumnsIO(source_config=s3_parquet_cloud_config).read()

        # Then
        assert df.shape == (15, 2) and df.columns.tolist() == ["id", "foo_name"]

    @pytest.mark.unit
    # pylint: disable=unused-argument
    def test__read_parquet_file_can_filter_out_rows_using_appropriate_options(self, mock_parquet_temporary_directory):
        # Given
        s3_parquet_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PATH_PREFIX_PARQUET")

        # When
        with patch.object(mixins, "awscli_runner") as mocked__awscli_runner:
            mocked__awscli_runner.return_value = True
            df = ReadS3ParquetIO(source_config=s3_parquet_cloud_config, filters=[[("foo_name", "==", "name_a")]]).read()

        # Then
        assert df.shape == (8, 3) and df.columns.tolist() == ["id", "foo_name", "bar"] and df.foo_name.unique() == ["name_a"]

    @pytest.mark.unit
    # pylint: disable=unused-argument
    def test__read_csv_file_is_called_with_correct_local_file_path_when_loading_a_path_prefix_with_env_as_cloud_s3_and_type_as_csv(
        self,
        mock_listdir,
        mock_temporary_directory,
        mock__read_csv_file,
    ):
        # Given
        s3_csv_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PATH_PREFIX_CSV")

        # When
        with patch.object(mixins, "awscli_runner") as mocked__awscli_runner:
            mocked__awscli_runner.return_value = True
            ReadS3ParquetIO(source_config=s3_csv_cloud_config).read()

        # Then
        assert len(mock__read_csv_file.mock_calls) == 3
        mock__read_csv_file.assert_has_calls(
            [
                mock.call("temp/obj_1.h5", {"id": "int64", "foo_name": "object", "bar": "int64"}),
                mock.call("temp/obj_2.h5", {"id": "int64", "foo_name": "object", "bar": "int64"}),
                mock.call("temp/obj_3.h5", {"id": "int64", "foo_name": "object", "bar": "int64"}),
            ]
        )

    @pytest.mark.unit
    # pylint: disable=unused-argument
    def test__read_json_file_is_called_with_correct_local_file_path_when_loading_a_path_prefix_with_env_as_cloud_s3_and_type_as_json(
        self,
        mock_listdir,
        mock_temporary_directory,
        mock__read_json_file,
    ):
        # Given
        s3_csv_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PATH_PREFIX_JSON")

        # When
        with patch.object(mixins, "awscli_runner") as mocked__awscli_runner:
            mocked__awscli_runner.return_value = True
            ReadS3ParquetIO(source_config=s3_csv_cloud_config).read()

        # Then
        assert len(mock__read_json_file.mock_calls) == 3
        mock__read_json_file.assert_has_calls(
            [
                mock.call("temp/obj_1.h5", {"id": "int64", "foo_name": "object", "bar": "int64"}),
                mock.call("temp/obj_2.h5", {"id": "int64", "foo_name": "object", "bar": "int64"}),
                mock.call("temp/obj_3.h5", {"id": "int64", "foo_name": "object", "bar": "int64"}),
            ]
        )

    @pytest.mark.unit
    # pylint: disable=unused-argument
    def test_a_concatenated_hdf_file_is_returned_with_schema_columns_when_loading_a_path_prefix_with_env_as_cloud_s3_and_type_as_hdf(
        self,
        mock_listdir,
        mock_temporary_directory,
        mock__read_hdf_file,
    ):
        # Given
        s3_hdf_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PATH_PREFIX_HDF")

        # When
        with patch.object(mixins, "awscli_runner") as mocked__awscli_runner:
            mocked__awscli_runner.return_Value = True
            h5_df = ReadS3HdfIO(source_config=s3_hdf_cloud_config).read()

        # Then
        pd.testing.assert_frame_equal(
            h5_df,
            pd.DataFrame(
                {
                    "id": [1, 2, 3],
                    "foo_name": ["class_a", "class_a", "class_a"],
                    "bar": [1001, 1001, 1001],
                }
            ),
        )

    @pytest.mark.unit
    # pylint: disable=unused-argument
    def test_a_ValueError_is_raised_if_file_type_is_not_supported_when_loading_a_path_prefix_with_env_as_cloud_s3(
        self,
        mock_listdir,
        mock_temporary_directory,
        mock__read_hdf_file,
    ):
        # Given
        s3_txt_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PATH_PREFIX_TXT")

        # When
        with pytest.raises(ValueError):
            ReadS3JsonIO(source_config=s3_txt_cloud_config).read()

    @pytest.mark.unit
    # pylint: disable=unused-argument
    def test__read_parquet_file_can_read_directory_of_parquet_files_containing_empty_files(
        self,
        mock_parquet_temporary_directory_w_empty_files,
    ):
        # Given
        s3_parquet_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PATH_PREFIX_PARQUET")

        # When
        with patch.object(mixins, "awscli_runner") as mocked__awscli_runner:
            mocked__awscli_runner.return_value = True
            df = ReadS3ParquetWEmptyFilesIO(source_config=s3_parquet_cloud_config).read()

        # Then
        assert df.shape == (10, 2) and df.columns.tolist() == ["id", "bar"]


class TestPostgresIO:
    @pytest.mark.unit
    def test_when_reading_from_postgres_with_env_as_cloud_get_table_columns_returns_valid_list_of_columns_for_a_model(self, expected_columns):
        # Given
        pg_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

        # When
        columns = ReadPostgresIO(source_config=pg_cloud_config)._get_table_columns(ERModel)  # pylint: disable=protected-access
        # Then
        assert columns == expected_columns

    @pytest.mark.unit
    @patch.object(WithPostgres, "_read_from_postgres")
    def test_read_from_postgres_is_called_for_loading_a_table_with_columns_with_env_as_cloud_and_type_as_postgres(self, mock__read_from_postgres, test_df):
        # Given
        mock__read_from_postgres.return_value = test_df
        postgres_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

        # When
        ReadPostgresIO(source_config=postgres_cloud_config).read()

        # Then
        mock__read_from_postgres.assert_called()

    @pytest.mark.unit
    @patch.object(WithPostgres, "_write_to_postgres")
    def test_write_to_postgres_is_called_for_uploading_a_table_with_columns_with_env_as_cloud_and_type_as_postgres(self, mock__write_to_postgres, test_df):
        # Given
        df = test_df
        postgres_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_PG_PARQUET")

        # When
        WritePostgresIO(source_config=postgres_cloud_config).write(df)

        # Then
        mock__write_to_postgres.assert_called()

    @pytest.mark.unit
    @patch.object(WithPostgres, "_write_to_postgres")
    def test_write_to_postgres_is_called_with_truncate_and_append_option(self, mock__write_to_postgres, test_df):
        # Given
        df = test_df
        postgres_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(
            source_key="WRITE_TO_PG_PARQUET",
        )

        # When
        write_config = WritePostgresIO(source_config=postgres_cloud_config, truncate_and_append=True)

        write_config.write(df)

        # Then
        mock__write_to_postgres.assert_called_with(test_df)
        assert "truncate_and_append" in write_config.options

    @pytest.mark.unit
    @patch.object(WithPostgres, "_read_from_postgres")
    def test_read_from_postgres_by_implicitly_generating_datamodel_from_schema(self, mock__read_from_postgres, test_df):
        # Given
        mock__read_from_postgres.return_value = test_df
        postgres_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

        # When / Then
        ReadPostgresIO(source_config=postgres_cloud_config).read()
        mock__read_from_postgres.assert_called()

    @pytest.mark.unit
    @patch.object(WithPostgres, "_read_database")
    def test_read_from_postgres_with_query(self, mock__read_database):
        # Given
        postgres_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

        # When
        ReadPostgresIO(source_config=postgres_cloud_config, sql_query="SELECT * FROM example").read()

        # Then
        mock__read_database.assert_called_with(ANY, "SELECT * FROM example", sql_query="SELECT * FROM example")

    @pytest.mark.unit
    @patch.object(pd, "read_sql")
    def test_read_from_postgres_with_query_and_options(self, mock__read_sql):
        # Given
        postgres_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

        # When
        ReadPostgresIO(source_config=postgres_cloud_config, sql_query="SELECT * FROM example", parse_dates=["date"], wrong_arg="whatever").read()

        # Then
        mock__read_sql.assert_called_with(sql="SELECT * FROM example", con=ANY, parse_dates=["date"])

    @pytest.mark.unit
    def test_generate_model_from_schema_returns_model(self):
        # Given
        postgres_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

        # When
        schema = postgres_cloud_config["schema"]
        schema_name = postgres_cloud_config["name"]
        model = ReadPostgresIO(source_config=postgres_cloud_config)._generate_model_from_schema(schema, schema_name)

        # Then
        assert len(model.__table__.columns) == len(schema) and model.__tablename__ == schema_name

    @pytest.mark.unit
    def test_get_table_columns_from_generated_model_returns_valid_list_of_columns(self):
        # Given
        pg_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_POSTGRES")

        # When
        schema = pg_cloud_config["schema"]
        schema_name = pg_cloud_config["name"]
        model = ReadPostgresIO(source_config=pg_cloud_config)._generate_model_from_schema(schema, schema_name)  # pylint: disable=protected-access
        columns = ReadPostgresIO(source_config=pg_cloud_config)._get_table_columns(model)  # pylint: disable=protected-access

        # Then
        assert isinstance(model.__table__.columns, ImmutableColumnCollection)
        for x, y in zip(columns, [PgModel.id, PgModel.foo, PgModel.bar, PgModel.baz]):
            assert str(x) == str(y)

    @pytest.mark.unit
    def test_to_check_if_dataframe_has_valid_data_types(self):
        # Given
        postgres_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_PG_PARQUET")

        df = pd.DataFrame.from_records(
            [
                ["cm_1", "id_1", 1000, "12/12/2000", True, 12.76],
                ["cm_2", "id_2", 1000, "01/02/1990", False, 199.76],
                ["cm_3", "id_3", 1000, "01/05/1990", False, 12.76],
            ],
            columns=["id", "foo", "bar", "start_date", "active", "net"],
        )

        # When
        is_valid = WriteExtendedPostgresIO(source_config=postgres_cloud_config, show_casting_warnings=True)._has_valid_dtypes(df)

        # Then
        assert is_valid is True


class TestKafkaIO:
    @pytest.mark.unit
    @patch.object(mixins, "KafkaProducer")
    @patch.object(MockKafkaProducer, "send")
    def test_write_to_kafka_is_called_for_writing_an_iterable_of_dicts_with_env_as_cloud_kafka(self, mock__kafka_producer, mock__kafka_producer_send, input_messages_df):
        # Given
        def rows_generator(_df, chunk_size):
            _chunk = []
            for _, row in df.iterrows():
                _chunk.append(row.to_dict())
                if len(_chunk) == chunk_size:
                    yield pd.DataFrame(_chunk)
                    _chunk.clear()

        df = input_messages_df

        mock__kafka_producer.return_value = MockKafkaProducer()
        mock__kafka_producer_send.return_value = MagicMock()

        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")

        # When
        for chunk in rows_generator(_df=df, chunk_size=2):
            WriteKafkaIO(kafka_cloud_config).write(chunk)
            # Then
            assert mock__kafka_producer_send.call_count == 1

    @pytest.mark.unit
    @patch.object(mixins, "KafkaProducer")
    @patch.object(MockKafkaProducer, "send")
    def test_write_to_kafka_is_called_with_document_transformer_if_provided_for_writing_an_iterable_of_dicts_with_env_as_cloud_kafka(
        self, mock__kafka_producer, mock__kafka_producer_send, input_messages_df
    ):
        # Given
        def rows_generator(_df, chunk_size):
            _chunk = []
            for _, row in df.iterrows():
                _chunk.append(row.to_dict())
                if len(_chunk) == chunk_size:
                    yield pd.DataFrame(_chunk)
                    _chunk.clear()

        df = input_messages_df.iloc[[0]]

        mock__kafka_producer.return_value = MockKafkaProducer()
        mock__kafka_producer_send.return_value = MagicMock()

        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")

        # When
        for chunk in rows_generator(_df=df, chunk_size=2):
            WriteKafkaIO(kafka_cloud_config, document_transformer=lambda v: dict(**v, worked=True)).write(chunk)
            # Then
            mock__kafka_producer_send.assert_called_once_with(
                {
                    "id": "message01",
                    "foo": "xxxxxxxx",
                    "bar": 0,
                    "baz": ["a", "b", "c"],
                    "worked": True,
                }
            )

    @pytest.mark.unit
    def test_kafka_producer_default_value_serialiser_is_used_unless_alternative_is_given(self, test_df):
        # Given
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")
        write_kafka_io = WriteKafkaIO(kafka_cloud_config)

        # When
        with patch.object(mixins, "KafkaProducer") as mock__kafka_producer, patch.object(MockKafkaProducer, "send") as mock__kafka_producer_send:
            mock__kafka_producer.DEFAULT_CONFIG = KafkaProducer.DEFAULT_CONFIG
            mock__kafka_producer.return_value = MockKafkaProducer()
            mock__kafka_producer_send.return_value = MagicMock()
            write_kafka_io.write(test_df)

        # Then
        value_serializer = write_kafka_io._WithKafka__kafka_config.pop("value_serializer")
        assert "WithKafka._default_value_serializer" in str(value_serializer)

    @pytest.mark.unit
    def test_kafka_producer_default_key_serialiser_is_used_unless_alternative_is_given(self, test_df):
        # Given
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")
        write_kafka_io = WriteKafkaIO(kafka_cloud_config)

        # When
        with patch.object(mixins, "KafkaProducer") as mock__kafka_producer, patch.object(MockKafkaProducer, "send") as mock__kafka_producer_send:
            mock__kafka_producer.DEFAULT_CONFIG = KafkaProducer.DEFAULT_CONFIG
            mock__kafka_producer.return_value = MockKafkaProducer()
            mock__kafka_producer_send.return_value = MagicMock()
            write_kafka_io.write(test_df)

        # Then
        key_serializer = write_kafka_io._WithKafka__kafka_config.pop("key_serializer")
        assert "WithKafka._default_key_serializer" in str(key_serializer)

    @pytest.mark.unit
    @patch.object(MockKafkaProducer, "send")
    @patch.object(mixins, "KafkaProducer")
    def test_kafka_producer_default_compression_type_is_snappy(self, mock__kafka_producer, mock__kafka_producer_send, test_df):
        # Given
        mock__kafka_producer.DEFAULT_CONFIG = KafkaProducer.DEFAULT_CONFIG
        mock__kafka_producer.return_value = MockKafkaProducer()
        mock__kafka_producer_send.return_value = MagicMock()
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")
        write_kafka_io = WriteKafkaIO(kafka_cloud_config)

        # When
        write_kafka_io.write(test_df)

        # Then
        write_kafka_io._WithKafka__kafka_config.pop("value_serializer")  # Removed as it returns a unique function identifier
        write_kafka_io._WithKafka__kafka_config.pop("key_serializer")  # Removed as it returns a unique function identifier
        assert write_kafka_io._WithKafka__kafka_config == {"bootstrap_servers": "mock-kafka-server", "compression_type": "snappy"}

    @pytest.mark.unit
    @patch.object(MockKafkaProducer, "send")
    @patch.object(mixins, "KafkaProducer")
    def test_kafka_producer_options_are_replaced_by_the_user_options(self, mock__kafka_producer, mock__kafka_producer_send, test_df):
        # Given
        mock__kafka_producer.DEFAULT_CONFIG = KafkaProducer.DEFAULT_CONFIG
        mock__kafka_producer.return_value = MockKafkaProducer()
        mock__kafka_producer_send.return_value = MagicMock()
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")
        write_kafka_io = WriteKafkaIO(kafka_cloud_config, compression_type="lz4", acks=2)

        # When
        write_kafka_io.write(test_df)

        # Then
        value_serializer = write_kafka_io._WithKafka__kafka_config.pop("value_serializer")  # Removed as it returns a unique function identifier
        write_kafka_io._WithKafka__kafka_config.pop("key_serializer")  # Removed as it returns a unique function identifier
        assert write_kafka_io._WithKafka__kafka_config == {
            "acks": 2,
            "bootstrap_servers": "mock-kafka-server",
            "compression_type": "lz4",
        } and "WithKafka._default_value_serializer" in str(value_serializer)

    @pytest.mark.unit
    def test_producer_send_method_sends_messages_with_index_as_key_by_default_if_a_keygen_is_not_provided(self, test_df):
        # Given
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")
        write_kafka_io = WriteKafkaIO(kafka_cloud_config)

        # When
        with patch.object(mixins, "KafkaProducer") as mock__kafka_producer:
            mock__kafka_producer.DEFAULT_CONFIG = KafkaProducer.DEFAULT_CONFIG
            mock_producer = MockKafkaProducer()
            mock__kafka_producer.return_value = mock_producer
            write_kafka_io.write(test_df)

        # Then
        assert mock_producer.my_stream == [
            {"key": 0, "value": {"bar": 1000, "baz": "ABC", "foo": "id_1", "id": "cm_1"}},
            {"key": 1, "value": {"bar": 1000, "baz": "ABC", "foo": "id_2", "id": "cm_2"}},
            {"key": 2, "value": {"bar": 1000, "baz": "ABC", "foo": "id_3", "id": "cm_3"}},
        ]

    @pytest.mark.unit
    def test_producer_send_method_can_send_keyed_messages_using_a_custom_key_generator(self, test_df):
        # Given
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")
        write_kafka_io = WriteKafkaIO(kafka_cloud_config, key_generator=lambda _, message: "XXX")

        # When
        with patch.object(mixins, "KafkaProducer") as mock__kafka_producer:
            mock__kafka_producer.DEFAULT_CONFIG = KafkaProducer.DEFAULT_CONFIG
            mock_producer = MockKafkaProducer()
            mock__kafka_producer.return_value = mock_producer
            write_kafka_io.write(test_df)

        # Then
        assert mock_producer.my_stream == [
            {"key": "XXX", "value": {"bar": 1000, "baz": "ABC", "foo": "id_1", "id": "cm_1"}},
            {"key": "XXX", "value": {"bar": 1000, "baz": "ABC", "foo": "id_2", "id": "cm_2"}},
            {"key": "XXX", "value": {"bar": 1000, "baz": "ABC", "foo": "id_3", "id": "cm_3"}},
        ]

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "key, encoded_key",
        [
            (None, None),
            ("cacik", b"cacik"),
        ],
    )
    def test_default_key_serialiser_returns_none_if_key_is_not_provided_and_an_encoded_string_otherwise(self, key, encoded_key):
        # Given/When/Then
        assert encoded_key == WithKafka._default_key_serializer(key)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "value, encoded_value",
        [
            (None, b"null"),
            ({"a": 1, "b": "cacik"}, b'{"a": 1, "b": "cacik"}'),
            ({"a": 1, "b": None}, b'{"a": 1, "b": null}'),
        ],
    )
    def test_default_value_serialiser_returns_encoded_mapping_if_key_is_not_provided_and_an_encoded_string_otherwise(self, value, encoded_value):
        # Given/When/Then
        assert encoded_value == WithKafka._default_value_serializer(value)

    @pytest.mark.unit
    def test_default_key_generator_and_transformer_are_used_if_none_are_provided_by_the_user(self):
        # Given
        keyed_test_df = pd.DataFrame.from_records(
            [
                ["key-01", "cm_1", "id_1", 1000, "ABC"],
                ["key-01", "cm_2", "id_2", 1000, "ABC"],  # <-- index is non-unique
                ["key-02", "cm_3", "id_3", 1000, "ABC"],
            ],
            columns=["key", "id", "foo", "bar", "baz"],
        ).set_index("key")
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")
        write_kafka_io = WriteKafkaIO(kafka_cloud_config)

        # When
        with patch.object(mixins, "KafkaProducer") as mock__kafka_producer:
            mock__kafka_producer.DEFAULT_CONFIG = KafkaProducer.DEFAULT_CONFIG
            mock_producer = MockKafkaProducer()
            mock__kafka_producer.return_value = mock_producer

            # When
            write_kafka_io.write(keyed_test_df)
            assert (write_kafka_io._WithKafka__key_generator("idx", "value") == "idx") and (write_kafka_io._WithKafka__document_transformer("value") == "value")

    @pytest.mark.unit
    def test_custom_key_generator_and_transformer_are_used_if_they_are_provided_by_the_user(self):
        # Given
        keyed_test_df = pd.DataFrame.from_records(
            [
                ["key-01", "cm_1", "id_1", 1000, "ABC"],
                ["key-01", "cm_2", "id_2", 1000, "ABC"],
                ["key-02", "cm_3", "id_3", 1000, "ABC"],
            ],
            columns=["key", "id", "foo", "bar", "baz"],
        ).set_index("key")
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")
        write_kafka_io = WriteKafkaIO(kafka_cloud_config, key_generator=lambda idx, _: "xxx", document_transformer=lambda _: "xxx")

        # When
        with patch.object(mixins, "KafkaProducer") as mock__kafka_producer:
            mock__kafka_producer.DEFAULT_CONFIG = KafkaProducer.DEFAULT_CONFIG
            mock_producer = MockKafkaProducer()
            mock__kafka_producer.return_value = mock_producer

            # When
            write_kafka_io.write(keyed_test_df)
            assert (write_kafka_io._WithKafka__key_generator("idx", "value") == "xxx") and (write_kafka_io._WithKafka__document_transformer("value") == "xxx")


class TestAllowedOptions:
    @pytest.fixture(autouse=True)
    def _pass_fixtures(self, capsys):
        self.capsys = capsys  # pylint: disable=attribute-defined-outside-init

    @pytest.mark.unit
    def test_args_of_returns_valid_set_of_allowed_kwargs_for_a_given_function(self):
        # Given
        def magic_function(arg_a: str, arg_b: int, arg_c: bool) -> bool:
            print(f"{arg_a}: {arg_b}")
            return arg_c

        func = magic_function

        # When
        options = args_of(func)

        # Then
        assert options == {"arg_a", "arg_b", "arg_c"}

    @pytest.mark.integration
    def test_allow_options_can_use_iterable_returned_from_args_of_to_filter_out_invalid_options(
        self,
    ):
        # Given
        def magic_function(arg_a: str, arg_b: int, arg_c: bool) -> bool:
            print(f"{arg_a}: {arg_b}")
            return arg_c

        func = magic_function

        @mixins.allow_options(args_of(func))
        def mock_method(**options: Any):
            return [*options]

        # When
        options = mock_method(arg_a="A", arg_b=1, arg_c=True, invalid_option="I SHOULDN'T BE HERE")

        # Then
        assert options == ["arg_a", "arg_b", "arg_c"]

    @pytest.mark.integration
    def test_allow_options_does_not_filter_out_valid_args_when_they_are_passed_as_args_and_not_as_kwargs(
        self,
    ):
        # Given
        def magic_function(arg_a: str, arg_b: int, arg_c: bool) -> bool:
            return [arg_a, arg_b, arg_c]

        func = magic_function

        @mixins.allow_options(args_of(func))
        def mock_method(schema: "str", **options: Any):
            print(schema)
            return magic_function(**options)

        # When
        # options = mock_method(schema="schema", **{"arg_a": "A", "arg_b": 1, "arg_c": True, "invalid_option": "I SHOULDN'T BE HERE"})  # THIS WOULD FAIL!
        options = mock_method(
            "schema",
            **{"arg_a": "A", "arg_b": 1, "arg_c": True, "invalid_option": "I SHOULDN'T BE HERE"},
        )

        # Then
        captured = self.capsys.readouterr()
        assert (captured.out == "schema\n") and (options == ["A", 1, True])

    @pytest.mark.integration  # This is an integration test as it uses `allow_options()` after `args_of()`
    def test_when_reading_locally_or_from_s3_invalid_options_are_ignored(self, expected_s3_csv_df):
        # Given
        invalid_option = "INVALID_OPTION"
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When
        s3_csv_df = ReadS3CsvIO(source_config=s3_csv_local_config, foo=invalid_option).read()

        # Then
        assert expected_s3_csv_df.equals(s3_csv_df)

    @pytest.mark.integration
    def test_when_reading_locally_or_from_s3_valid_options_are_considered(self, expected_s3_csv_df):
        # Given
        # VALID OPTION: dtype=None
        s3_csv_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="LOCAL",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When
        s3_csv_df = ReadS3CsvIO(source_config=s3_csv_local_config, dtype=None).read()

        # Then
        assert expected_s3_csv_df.equals(s3_csv_df)


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
