# pylint: disable=no-member, missing-module-docstring, missing-class-docstring, missing-function-docstring, too-many-public-methods, too-few-public-methods, protected-access, C0103, C0302, R0801
import os
import shutil
from tempfile import NamedTemporaryFile
from unittest import mock
from unittest.mock import patch

import pandas as pd
import pydantic
import pytest
import yaml

import dynamicio.mixins.with_local
import dynamicio.mixins.with_s3
from dynamicio.config import IOConfig
from dynamicio.errors import ColumnsDataTypeError
from tests import constants
from tests.constants import TEST_RESOURCES
from tests.mocking.io import (
    ReadS3CsvIO,
    ReadS3HdfIO,
    ReadS3JsonIO,
    ReadS3ParquetIO,
    ReadS3ParquetWEmptyFilesIO,
    ReadS3ParquetWithDifferentCastableDTypeIO,
    ReadS3ParquetWithDifferentNonCastableDTypeIO,
    ReadS3ParquetWithLessColumnsIO,
    TemplatedFile,
    WriteS3CsvIO,
    WriteS3HdfIO,
    WriteS3JsonIO,
    WriteS3ParquetIO,
)


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
        with patch.object(dynamicio.mixins.with_local.WithLocal, "_read_csv_file") as mock__read_csv_file, patch.object(
            dynamicio.mixins.with_s3.WithS3File, "_s3_named_file_reader"
        ) as mock_s3_reader:
            with open(file_path, "r") as file:  # pylint: disable=unspecified-encoding
                mock_s3_reader.return_value = file
                io_obj = TemplatedFile(source_config=config, file_name_to_replace="some_csv_to_read")
                final_schema = io_obj.schema
                io_obj.read()

        mock__read_csv_file.assert_called_once_with(file_path, final_schema)

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
        with patch.object(dynamicio.mixins.with_local.WithLocal, "_write_csv_file") as mock__write_csv_file:
            df = pd.read_csv(os.path.join(TEST_RESOURCES, "data/input/some_csv_to_read.csv"))
            TemplatedFile(source_config=config, file_name_to_replace="some_csv_to_read").write(df)

        # Then
        args, _ = mock__write_csv_file.call_args
        assert "s3://mock-bucket/path/to/some_csv_to_read.csv" == args[1]

    @pytest.mark.unit
    @patch.object(dynamicio.mixins.with_s3.WithS3File, "_read_from_s3_file")
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
        with patch.object(dynamicio.mixins.with_s3.WithS3File, "_s3_reader") as mock_s3_reader, patch.object(
            dynamicio.mixins.with_s3.WithS3File, "_read_parquet_file"
        ) as mock_read_parquet_file:
            with open(file_path, "r") as file:  # pylint: disable=unspecified-encoding
                mock_s3_reader.return_value = file
                ReadS3ParquetIO(source_config=s3_parquet_cloud_config, no_disk_space=True).read()

        # Then
        mock_s3_reader.assert_not_called()
        mock_read_parquet_file.assert_called()

    @pytest.mark.unit
    def test_s3_reader_is_called_for_loading_a_hdf_with_env_as_cloud_s3_and_type_as_hdf(self, expected_s3_hdf_file_path, expected_s3_hdf_df):
        # Given
        s3_hdf_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_HDF")

        # When
        with patch.object(dynamicio.mixins.with_s3.WithS3File, "boto3_client") as mock__boto3_client:

            def mock_download_fobj(s3_bucket, s3_key, target_file):  # pylint: disable=unused-argument
                with open(expected_s3_hdf_file_path, "rb") as fin:
                    shutil.copyfileobj(fin, target_file)

            mock__boto3_client.download_fileobj.side_effect = mock_download_fobj
            loaded_hdf_pd = ReadS3HdfIO(source_config=s3_hdf_cloud_config, no_disk_space=True).read()

        # Then
        pd.testing.assert_frame_equal(loaded_hdf_pd, expected_s3_hdf_df)

    @pytest.mark.unit
    def test_s3_reader_is_not_called_for_loading_a_json_with_env_as_cloud_s3_and_type_as_json_and_no_disk_space_flag(self):
        # Given
        s3_json_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_JSON")

        # When
        with patch.object(dynamicio.mixins.with_s3.WithS3File, "_s3_reader") as mock__s3_reader, patch.object(
            dynamicio.mixins.with_s3.WithS3File, "_read_json_file"
        ) as mock__read_json_file:
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
        with patch.object(dynamicio.mixins.with_s3.WithS3File, "_s3_reader") as mock__s3_reader, patch.object(
            dynamicio.mixins.with_s3.WithS3File, "_read_csv_file"
        ) as mock__read_csv_file:
            ReadS3CsvIO(source_config=s3_csv_cloud_config, no_disk_space=True).read()

        # Then
        mock__s3_reader.assert_not_called()
        mock__read_csv_file.assert_called()

    @pytest.mark.unit
    def test_ValueError_is_raised_if_file_path_missing_from_config(self, tmp_path):
        tmp_yaml = tmp_path / "test.yaml"
        with open(tmp_yaml, encoding="utf-8", mode="w") as f_out:
            yaml.safe_dump(
                {
                    "READ_FROM_S3_MISSING_FILE_PATH": {
                        "LOCAL": {
                            "type": "local",
                            "local": {
                                "file_path": "[[ TEST_RESOURCES ]]/data/input/some_csv_to_read.csv",
                                "file_type": "csv",
                            },
                        },
                        "CLOUD": {
                            "type": "s3_file",
                            "s3": {"bucket": "[[ MOCK_BUCKET ]]", "file_type": "csv"},
                        },
                        "schema": {"file_path": "[[ TEST_RESOURCES ]]/schemas/read_from_s3_csv.yaml"},
                    }
                },
                f_out,
            )

        with pytest.raises(pydantic.ValidationError):
            IOConfig(
                path_to_source_yaml=str(tmp_yaml),
                env_identifier="CLOUD",
                dynamic_vars=constants,
            )

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
        with patch.object(dynamicio.mixins.with_s3.WithS3File, "_s3_writer") as mock__s3_writer, patch.object(WriteS3ParquetIO, "_apply_schema") as mock__apply_schema, patch.object(
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
        with patch.object(dynamicio.mixins.with_s3.WithS3File, "_read_parquet_file") as mock__read_parquet_file, patch.object(
            dynamicio.mixins.with_s3.WithS3File, "_s3_named_file_reader"
        ):
            mock__read_parquet_file.return_value = expected_s3_parquet_df
            ReadS3ParquetWithDifferentCastableDTypeIO(source_config=s3_parquet_cloud_config).read()

        assert True, "No exception was raised"

    @pytest.mark.unit
    @patch.object(dynamicio.mixins.with_s3.WithS3File, "_s3_named_file_reader")
    @patch.object(dynamicio.mixins.with_s3.WithS3File, "_read_parquet_file")
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
        with patch.object(dynamicio.mixins.with_s3.WithS3File, "_s3_reader") as mock__s3_reader, patch.object(
            dynamicio.mixins.with_local.WithLocal, "_read_parquet_file"
        ) as mock__read_parquet_file:
            ReadS3ParquetIO(source_config=s3_parquet_cloud_config, no_disk_space=True).read()

        # Then
        mock__s3_reader.assert_not_called()
        mock__read_parquet_file.assert_called()

    @pytest.mark.unit
    @patch.object(dynamicio.mixins.with_s3.WithS3File, "_write_to_s3_file")
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
        with patch.object(dynamicio.mixins.with_s3.WithS3File, "_s3_writer") as mock__s3_writer, patch.object(
            dynamicio.mixins.with_local.WithLocal, "_write_parquet_file"
        ) as mock__write_parquet_file:
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
        with patch.object(dynamicio.mixins.with_s3.WithS3File, "_s3_writer") as mock__s3_writer, patch.object(
            dynamicio.mixins.with_local.WithLocal, "_write_csv_file"
        ) as mock__write_csv_file:
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
        with patch.object(dynamicio.mixins.with_s3.WithS3File, "_s3_writer") as mock__s3_writer, patch.object(
            dynamicio.mixins.with_local.WithLocal, "_write_json_file"
        ) as mock__write_json_file:
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
        with patch.object(dynamicio.mixins.with_s3.WithS3File, "_s3_writer") as mock__s3_writer:
            with NamedTemporaryFile(delete=False) as temp_file:
                mock__s3_writer.return_value = temp_file
                WriteS3HdfIO(source_config=s3_hdf_local_config).write(df)

        # Then
        assert os.stat(temp_file.name).st_size == 1064192, "Confirm that the output file size did not change"


class TestS3PathPrefixIO:
    @pytest.mark.unit
    def test_error_is_raised_if_path_prefix_missing_from_config(self, tmp_path):
        tmp_yaml = tmp_path / "test.yaml"
        with open(tmp_yaml, encoding="utf-8", mode="w") as f_out:
            yaml.safe_dump(
                {
                    "READ_FROM_S3_MISSING_PATH_PREFIX": {
                        "LOCAL": {
                            "type": "local",
                            "local": {
                                "file_path": "[[ TEST_RESOURCES ]]/data/input/some_csv_to_read.csv",
                                "file_type": "csv",
                            },
                        },
                        "CLOUD": {
                            "type": "s3_path_prefix",
                            "s3": {"bucket": "[[ MOCK_BUCKET ]]", "file_type": "csv"},
                        },
                        "schema": {"file_path": "[[ TEST_RESOURCES ]]/schemas/read_from_s3_csv.yaml"},
                    }
                },
                f_out,
            )

        with pytest.raises(pydantic.ValidationError):
            IOConfig(
                path_to_source_yaml=str(tmp_yaml),
                env_identifier="CLOUD",
                dynamic_vars=constants,
            )

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
    def test_error_is_raised_if_file_type_not_parquet_when_uploading(self, tmp_path):
        tmp_yaml = tmp_path / "test.yaml"
        with open(tmp_yaml, encoding="utf-8", mode="w") as f_out:
            yaml.safe_dump(
                {
                    "WRITE_TO_S3_PATH_PREFIX_NOT_PARQUET": {
                        "CLOUD": {
                            "type": "s3_path_prefix",
                            "s3": {
                                "bucket": "[[ MOCK_BUCKET ]]",
                                "path_prefix": "[[ MOCK_KEY ]]",
                                "file_type": "not_parquet",
                            },
                        }
                    }
                },
                f_out,
            )

        with pytest.raises(pydantic.ValidationError):
            IOConfig(
                path_to_source_yaml=str(tmp_yaml),
                env_identifier="CLOUD",
                dynamic_vars=constants,
            )

    @pytest.mark.unit
    @patch.object(dynamicio.mixins.with_s3.WithS3PathPrefix, "_read_from_s3_path_prefix")
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
    @patch.object(dynamicio.mixins.with_s3.WithS3PathPrefix, "_write_to_s3_path_prefix")
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
        with patch.object(dynamicio.mixins.with_s3, "awscli_runner") as mocked__awscli_runner:
            WriteS3ParquetIO(source_config=s3_parquet_cloud_config, partition_cols="col_2").write(input_df)

        # Then
        mocked__awscli_runner.assert_called_with(
            "s3", "sync", "temp", "s3://mock-bucket/data/some_dir/", "--acl", "bucket-owner-full-control", "--only-show-errors", "--exact-timestamps"
        )

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
        with patch.object(dynamicio.mixins.with_s3, "awscli_runner") as mocked__awscli_runner:
            ReadS3HdfIO(source_config=s3_hdf_cloud_config).read()

        # Then
        mocked__awscli_runner.assert_called_with(
            "s3", "sync", "s3://mock-bucket/data/some_dir/", "temp", "--acl", "bucket-owner-full-control", "--only-show-errors", "--exact-timestamps"
        )

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
        with patch.object(dynamicio.mixins.with_s3, "awscli_runner") as mocked__awscli_runner:
            mocked__awscli_runner.return_value = True
            read_obj = ReadS3HdfIO(source_config=s3_hdf_cloud_config)
            actual_schema = read_obj.schema
            read_obj.read()

        # Then
        assert len(mock__read_hdf_file.mock_calls) == 3
        mock__read_hdf_file.assert_has_calls(
            [
                mock.call("temp/obj_1.h5", actual_schema),
                mock.call("temp/obj_2.h5", actual_schema),
                mock.call("temp/obj_3.h5", actual_schema),
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
        with patch.object(dynamicio.mixins.with_s3, "awscli_runner") as mocked__awscli_runner:
            mocked__awscli_runner.return_value = True
            read_obj = ReadS3ParquetIO(source_config=s3_parquet_cloud_config)
            actual_schema = read_obj.schema
            read_obj.read()

        # Then
        assert len(mock__read_parquet_file.mock_calls) == 3
        mock__read_parquet_file.assert_has_calls(
            [
                mock.call("temp/obj_1.h5", actual_schema),
                mock.call("temp/obj_2.h5", actual_schema),
                mock.call("temp/obj_3.h5", actual_schema),
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
        with patch.object(dynamicio.mixins.with_s3, "awscli_runner") as mock__awscli_runner, patch.object(
            dynamicio.mixins.with_local.WithLocal, "_read_parquet_file"
        ) as mock__read_parquet_file:
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
        with patch.object(dynamicio.mixins.with_s3, "awscli_runner") as mocked__awscli_runner:
            mocked__awscli_runner.return_value = True
            df = ReadS3ParquetWithLessColumnsIO(source_config=s3_parquet_cloud_config).read()

        # Then
        assert df.shape == (45, 2) and df.columns.tolist() == ["id", "foo_name"]

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
        with patch.object(dynamicio.mixins.with_s3, "awscli_runner") as mocked__awscli_runner:
            mocked__awscli_runner.return_value = True
            df = ReadS3ParquetIO(source_config=s3_parquet_cloud_config, filters=[[("foo_name", "==", "name_a")]]).read()

        # Then
        assert df.shape == (24, 3) and df.columns.tolist() == ["id", "foo_name", "bar"] and df.foo_name.unique() == ["name_a"]

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
        with patch.object(dynamicio.mixins.with_s3, "awscli_runner") as mocked__awscli_runner:
            mocked__awscli_runner.return_value = True
            read_obj = ReadS3ParquetIO(source_config=s3_csv_cloud_config)
            actual_schema = read_obj.schema
            read_obj.read()

        # Then
        assert len(mock__read_csv_file.mock_calls) == 3
        mock__read_csv_file.assert_has_calls(
            [
                mock.call("temp/obj_1.h5", actual_schema),
                mock.call("temp/obj_2.h5", actual_schema),
                mock.call("temp/obj_3.h5", actual_schema),
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
        with patch.object(dynamicio.mixins.with_s3, "awscli_runner") as mocked__awscli_runner:
            mocked__awscli_runner.return_value = True
            read_obj = ReadS3ParquetIO(source_config=s3_csv_cloud_config)
            actual_schema = read_obj.schema
            read_obj.read()

        # Then
        assert len(mock__read_json_file.mock_calls) == 3
        mock__read_json_file.assert_has_calls(
            [
                mock.call("temp/obj_1.h5", actual_schema),
                mock.call("temp/obj_2.h5", actual_schema),
                mock.call("temp/obj_3.h5", actual_schema),
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
        with patch.object(dynamicio.mixins.with_s3, "awscli_runner") as mocked__awscli_runner:
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
        tmp_path,
        mock_listdir,
        mock_temporary_directory,
        mock__read_hdf_file,
    ):
        # Given
        test_yaml_file = tmp_path / "mytest.yml"
        with open(test_yaml_file, encoding="utf-8", mode="w") as f_out:
            yaml.dump(
                {
                    "READ_FROM_S3_PATH_PREFIX_TXT": {
                        "CLOUD": {
                            "type": "s3_path_prefix",
                            "s3": {
                                "bucket": "test-bucket",
                                "path_prefix": "[[ MOCK_KEY ]]",
                                "file_type": "txt",
                            },
                        }
                    }
                },
                f_out,
            )

        # When & Then
        with pytest.raises(pydantic.ValidationError):
            IOConfig(
                path_to_source_yaml=str(test_yaml_file),
                env_identifier="CLOUD",
                dynamic_vars=constants,
            )

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
        with patch.object(dynamicio.mixins.with_s3, "awscli_runner") as mocked__awscli_runner:
            mocked__awscli_runner.return_value = True
            df = ReadS3ParquetWEmptyFilesIO(source_config=s3_parquet_cloud_config).read()

        # Then
        assert df.shape == (10, 2) and df.columns.tolist() == ["id", "bar"]
