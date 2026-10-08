# pylint: disable=no-member, too-many-positional-arguments, missing-module-docstring, missing-class-docstring, missing-function-docstring, too-many-public-methods, too-few-public-methods, protected-access, C0103, C0302, R0801
import io
import os
import shutil
from tempfile import NamedTemporaryFile
from unittest import mock
from unittest.mock import MagicMock, patch

import pandas as pd
import pyarrow.parquet as pq
import pydantic
import pytest
import yaml

# Application Imports
import dynamicio.errors
import dynamicio.mixins.with_local
import dynamicio.mixins.with_s3
from dynamicio.config import IOConfig
from tests import constants
from tests.mocking.io import (
    ReadS3CsvIO,
    ReadS3HdfIO,
    ReadS3JsonIO,
    ReadS3JsonOrientRecordsAltIO,
    ReadS3JsonOrientRecordsIO,
    ReadS3ParquetIO,
    ReadS3ParquetWEmptyFilesIO,
    ReadS3ParquetWithDifferentNonCastableDTypeIO,
    ReadS3ParquetWithLessColumnsIO,
    TemplatedFile,
    WriteS3IO,
    WriteS3JsonOrientRecordsIO,
)


class TestHdfIO:
    @pytest.mark.unit
    def test_save_then_load_round_trips_a_dataframe_through_in_memory_hdf(self):
        # Given
        df = pd.DataFrame({"id": [1, 2], "foo_name": ["a", "b"], "bar": [3, 4]})
        hdf_io = dynamicio.mixins.with_s3.HdfIO()
        fobj = io.BytesIO()

        # When
        hdf_io.save(df, fobj, label="round_trip_test")
        fobj.seek(0)
        loaded_df = hdf_io.load(fobj, label="round_trip_test")

        # Then
        pd.testing.assert_frame_equal(loaded_df.reset_index(drop=True), df)

    @pytest.mark.unit
    def test_save_accepts_a_custom_key_via_options(self):
        # Given
        df = pd.DataFrame({"id": [1], "foo_name": ["a"], "bar": [2]})
        hdf_io = dynamicio.mixins.with_s3.HdfIO()
        fobj = io.BytesIO()

        # When
        hdf_io.save(df, fobj, label="custom_key_test", options={"key": "custom"})
        fobj.seek(0)
        loaded_df = hdf_io.load(fobj, label="custom_key_test", options={"key": "custom"})

        # Then
        pd.testing.assert_frame_equal(loaded_df.reset_index(drop=True), df)


class TestS3FileIO:
    @pytest.mark.unit
    def test_read_resolves_file_path_if_templated(self):
        # Given
        # TEMPLATED_FILE_PATH:
        #   LOCAL:
        #     ...
        #   CLOUD:
        # type: "s3_file"
        #     s3:
        #       bucket: "[[ MOCK_BUCKET ]]"
        #       file_path: "path/to/{file_name_to_replace}.csv"
        #       file_type: "csv"
        cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="TEMPLATED_FILE_PATH")

        file_path = f"{constants.TEST_RESOURCES}/data/input/some_csv_to_read.csv"
        expected_df = pd.read_csv(file_path)

        # When
        with patch.object(dynamicio.mixins.with_s3.WithS3File, "_read_s3_csv_file") as mock__read_csv_file:
            mock__read_csv_file.return_value = expected_df
            io_obj = TemplatedFile(source_config=cloud_config, file_name_to_replace="some_csv_to_read")
            final_schema = io_obj.schema
            io_obj.read()

        # Then
        mock__read_csv_file.assert_called_once()
        called_args = mock__read_csv_file.call_args[0]
        assert called_args[0] == "s3://mock-bucket/path/to/some_csv_to_read.csv"
        assert called_args[1] == final_schema

    @pytest.mark.unit
    def test_write_resolves_file_path_if_templated(self):
        # Given
        # TEMPLATED_FILE_PATH:
        #   LOCAL:
        #     ...
        #   CLOUD:
        # type: "s3_file"
        #     s3:
        #       bucket: "[[ MOCK_BUCKET ]]"
        #       file_path: "path/to/{file_name_to_replace}.csv"
        #       file_type: "csv"
        cloud_config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="TEMPLATED_FILE_PATH")

        # When
        file_path = os.path.join(constants.TEST_RESOURCES, "data/input/some_csv_to_read.csv")
        df = pd.read_csv(file_path)

        # Patch S3 write method instead of local one
        with patch.object(dynamicio.mixins.with_s3.WithS3File, "_write_s3_csv_file") as mock__write_csv_file:
            TemplatedFile(source_config=cloud_config, file_name_to_replace="some_csv_to_read").write(df)

        # Then
        args, _ = mock__write_csv_file.call_args
        assert args[0].equals(df)
        assert args[1] == "s3://mock-bucket/path/to/some_csv_to_read.csv"

    @pytest.mark.unit
    @patch.object(dynamicio.mixins.with_s3.WithS3File, "_read_from_s3_file")
    def test_read_from_s3_file_is_called_for_loading_a_file_with_env_as_cloud_s3(self, mock__read_from_s3_file, expected_s3_csv_df):
        # Given
        mock__read_from_s3_file.return_value = expected_s3_csv_df
        cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        # When
        ReadS3CsvIO(source_config=cloud_config).read()

        # Then
        mock__read_from_s3_file.assert_called()

    @pytest.mark.unit
    @patch("dynamicio.mixins.with_s3.boto3.client")
    def test_boto3_client_is_used_for_loading_a_hdf_with_env_as_cloud_s3_and_type_as_hdf(self, mock_boto3_client, expected_s3_hdf_file_path):
        # Given
        cloud_config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_HDF")

        def mock_download_fobj(_, __, fobj):
            with open(expected_s3_hdf_file_path, "rb") as fin:
                shutil.copyfileobj(fin, fobj)

        mock_boto3_client.return_value.download_fileobj.side_effect = mock_download_fobj

        # When
        with patch.object(dynamicio.mixins.with_s3.HdfIO, "load") as mock_hdf_load:
            # match schema columns to avoid filtering errors
            mock_hdf_load.return_value = pd.DataFrame({"id": [1], "foo_name": ["a"], "bar": [1]})
            ReadS3HdfIO(source_config=cloud_config).read()

        # Then
        mock_boto3_client.return_value.download_fileobj.assert_called_once()

    @pytest.mark.unit
    @patch("dynamicio.mixins.with_s3.wr.s3.read_csv")
    def test_wrangler_csv_reader_is_used_for_loading_a_csv_with_env_as_cloud_s3_and_file_type_csv(self, mock_wrangler_csv_reader, expected_s3_csv_df):
        # Given
        cloud_config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_CSV")

        mock_wrangler_csv_reader.return_value = expected_s3_csv_df

        # When
        ReadS3CsvIO(source_config=cloud_config).read()

        # Then
        mock_wrangler_csv_reader.assert_called()

    @pytest.mark.unit
    @patch("dynamicio.mixins.with_s3.wr.s3.read_parquet")
    def test_wrangler_parquet_reader_is_used_for_loading_a_csv_with_env_as_cloud_s3_and_file_type_parquet(self, mock_wrangler_parquet_reader, expected_s3_parquet_df):
        # Given
        cloud_config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PARQUET")

        mock_wrangler_parquet_reader.return_value = expected_s3_parquet_df

        # When
        ReadS3ParquetIO(source_config=cloud_config, pyarrow_additional_kwargs={}).read()

        # Then
        mock_wrangler_parquet_reader.assert_called()

    @pytest.mark.unit
    @patch("dynamicio.mixins.with_s3.wr.s3.read_json")
    def test_wrangler_json_reader_is_used_for_loading_a_csv_with_env_as_cloud_s3_and_file_type_json(self, mock_wrangler_json_reader, expected_s3_json_df):
        # Given
        cloud_config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_JSON")

        mock_wrangler_json_reader.return_value = expected_s3_json_df

        # When
        ReadS3JsonIO(source_config=cloud_config).read()

        # Then
        mock_wrangler_json_reader.assert_called()

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
                            # The file path should have been defined here...
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
    @patch.object(dynamicio.mixins.with_s3.WithS3File, "_write_to_s3_file")
    def test_s3_writer_is_called_for_writing_a_file_with_env_is_set_to_cloud_s3(self, mock__write_to_s3_file):
        # Given
        df = pd.DataFrame.from_dict({"col_1": [3, 2, 1, 0], "col_2": ["a", "b", "c", "d"]})

        s3_json_local_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_JSON")

        # When
        WriteS3IO(source_config=s3_json_local_config).write(df)

        # Then
        mock__write_to_s3_file.assert_called_once()

    @pytest.mark.unit
    def test_wrangler_to_parquet_is_called_for_writing_a_parquet_with_env_as_cloud_s3_and_file_type_as_parquet(self):
        # Given
        # WRITE_TO_S3_PARQUET:
        #   LOCAL:
        #     ...
        #   CLOUD:
        # type: "s3_file"
        #     s3:
        #       bucket: "[[ MOCK_BUCKET ]]"
        #       file_path: "test/write_some_parquet.parquet"
        #       file_type: "parquet"
        df = pd.DataFrame.from_dict({"col_1": [3, 2, 1, 0], "col_2": ["a", "b", "c", "d"]})

        cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_PARQUET")

        # When
        with patch.object(dynamicio.mixins.with_s3.wr.s3, "to_parquet") as mock__wr_s3_parquet_writer:
            WriteS3IO(source_config=cloud_config, sanitize_columns=False).write(df)

        # Then
        mock__wr_s3_parquet_writer.assert_called_once()

    @pytest.mark.unit
    @patch.object(dynamicio.mixins.with_s3, "_download_to_memory")
    def test_columns_data_type_error_exception_is_generated_if_column_dtypes_dont_map_to_the_expected_dtypes(self, mock__wr_read_parquet, expected_s3_parquet_df):
        # Given
        buffer = io.BytesIO()
        expected_s3_parquet_df.to_parquet(buffer)
        mock__wr_read_parquet.side_effect = lambda *_: io.BytesIO(buffer.getvalue())
        s3_parquet_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PARQUET")

        # When / Then
        with pytest.raises(dynamicio.errors.ColumnsDataTypeError):
            ReadS3ParquetWithDifferentNonCastableDTypeIO(source_config=s3_parquet_cloud_config).read()

    @pytest.mark.unit
    def test_wrangler_to_csv_is_called_for_writing_a_csv_with_env_as_cloud_s3_and_file_type_as_csv(self):
        # Given
        # WRITE_TO_S3_CSV:
        #   LOCAL:
        #     ...
        #   CLOUD:
        # type: "s3_file"
        #     s3:
        #       bucket: "[[ MOCK_BUCKET ]]"
        #       file_path: "test/write_some_csv.csv"
        #       file_type: "csv"
        df = pd.DataFrame.from_dict({"col_1": [3, 2, 1, 0], "col_2": ["a", "b", "c", "d"]})

        cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_CSV")

        # When
        with patch.object(dynamicio.mixins.with_s3.wr.s3, "to_csv") as mock__wr_s3_csv_writer:
            WriteS3IO(source_config=cloud_config).write(df)

        # Then
        mock__wr_s3_csv_writer.assert_called_once()

    @pytest.mark.unit
    def test_wrangler_to_json_is_called_for_writing_a_json_with_env_as_cloud_s3_and_file_type_as_json(self):
        # Given
        # WRITE_TO_S3_JSON:
        #   LOCAL:
        #     ...
        #   CLOUD:
        # type: "s3_file"
        #     s3:
        #       bucket: "[[ MOCK_BUCKET ]]"
        #       file_path: "test/write_some_json.json"
        #       file_type: "json"
        df = pd.DataFrame.from_dict({"col_1": [3, 2, 1, 0], "col_2": ["a", "b", "c", "d"]})

        cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_JSON")

        # When
        with patch.object(dynamicio.mixins.with_s3.wr.s3, "to_json") as mock__wr_s3_json_writer:
            with NamedTemporaryFile(delete=False) as temp_file:
                mock__wr_s3_json_writer.return_value = temp_file
                WriteS3IO(source_config=cloud_config).write(df)

        # Then
        mock__wr_s3_json_writer.assert_called()

    @pytest.mark.unit
    def test_boto3_client_is_used_for_uploading_a_hdf_with_env_as_cloud_s3_and_file_type_as_hdf(self):
        # Given
        # WRITE_TO_S3_HDF:
        #   LOCAL:
        #     ...
        #   CLOUD:
        # type: "s3_file"
        #     s3:
        #       bucket: "[[ MOCK_BUCKET ]]"
        #       file_path: "test/write_some_h5.h5"
        #       file_type: "hdf"
        df = pd.DataFrame.from_dict({"col_1": [3, 2, 1, 0], "col_2": ["a", "b", "c", "d"]})

        cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(
            source_key="WRITE_TO_S3_HDF"
        )  # ✅ fix: this should be HDF not JSON

        mock_boto3_client = MagicMock()

        # When
        with patch("dynamicio.mixins.with_s3.boto3.client", return_value=mock_boto3_client), patch.object(dynamicio.mixins.with_s3.HdfIO, "save") as mock_save:
            WriteS3IO(source_config=cloud_config).write(df)

        # Then
        mock_save.assert_called_once()
        mock_boto3_client.upload_fileobj.assert_called_once()


class TestAllowedArgsAreConfiguredCorrectlyForWithS3File:

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "source_key, patch_target, input_options, expected_kwargs",
        [
            ("READ_FROM_S3_PARQUET", "read_parquet", {"ignore_empty": True, "invalid_opt": True}, {"ignore_empty": True}),
            (
                "READ_FROM_S3_CSV",
                "read_csv",
                {"compression": "gzip", "dataset": True, "skipinitialspace": True, "invalid_opt": 123},
                {"compression": "gzip", "dataset": True, "skipinitialspace": True},
            ),
            ("READ_FROM_S3_JSON", "read_json", {"version_id": "1.0.1", "orient": "records", "invalid": "nope"}, {"version_id": "1.0.1", "orient": "records"}),
        ],
    )
    def test_wr_s3_readers_accept_only_valid_options(self, source_key, patch_target, input_options, expected_kwargs):
        # Given
        # We provide options from a combination of kwargs from both aws-wrangler and pandas readers
        config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key=source_key)

        sample_df = pd.DataFrame({"id": [1], "foo_name": ["a"], "bar": [1]})

        # When
        with patch.object(dynamicio.mixins.with_s3.wr.s3, patch_target) as mock_reader:
            mock_reader.return_value = sample_df
            io_cls = {"READ_FROM_S3_PARQUET": ReadS3ParquetIO, "READ_FROM_S3_CSV": ReadS3CsvIO, "READ_FROM_S3_JSON": ReadS3JsonIO}[source_key]
            io_cls(source_config=config, **input_options).read()

        # Then
        call_kwargs = mock_reader.call_args.kwargs
        for k, v in expected_kwargs.items():
            assert call_kwargs[k] == v
        assert all(k not in call_kwargs for k in input_options if k not in expected_kwargs)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "source_key, patch_target, input_options, expected_options",
        [
            ("WRITE_TO_S3_PARQUET", "to_parquet", {"compression": "snappy", "sanitize_columns": False, "invalid_opt": True}, {"compression": "snappy", "sanitize_columns": False, "dataset": False}),
            (
                "WRITE_TO_S3_CSV",
                "to_csv",
                {"concurrent_partitioning": True, "compression": "gzip", "invalid_opt": 123},
                {"concurrent_partitioning": True, "compression": "gzip", "index": False},
            ),
            ("WRITE_TO_S3_JSON", "to_json", {"mode": "overwrite", "date_format": "iso", "invalid": "nope"}, {"mode": "overwrite", "date_format": "iso"}),
        ],
    )
    def test_wr_s3_writers_accept_only_valid_options(self, source_key, patch_target, input_options, expected_options):
        df = pd.DataFrame({"col_1": [1, 2], "col_2": ["a", "b"]})
        config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key=source_key)

        with patch.object(dynamicio.mixins.with_s3.wr.s3, patch_target) as mock_writer:
            WriteS3IO(source_config=config, **input_options).write(df)

        call_kwargs = mock_writer.call_args.kwargs
        for k, v in expected_options.items():
            assert call_kwargs[k] == v
        assert all(k not in call_kwargs for k in input_options if k not in expected_options)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "input_options, raw_df_data, expected_df, raises_exception, io_class",
        [
            # ✅ Supported: Single record json
            # Sample Json Input:
            # {
            #   "data": {
            #     "release": "feb09",
            #     "timestamp": 1614268643313
            #   }
            # }
            (
                {"orient": "records"},
                pd.DataFrame([{"data": {"release": "current", "timestamp": 1744281068}}]),  # raw_wrangler_json_read_df
                pd.DataFrame([{"data": {"release": "current", "timestamp": 1744281068}}]),  # expected_df
                False,
                ReadS3JsonOrientRecordsIO,
            ),
            # ✅ Supported: Multiple records
            # Sample Json Input:
            # [
            #   { "release": "feb09", "timestamp": 1614268643313 },
            #   { "release": "feb10", "timestamp": 1614268643313 }
            # ]
            (
                {"orient": "records"},
                pd.DataFrame(
                    [
                        {"release": "feb09", "timestamp": 1614268643313},
                        {"release": "feb10", "timestamp": 1614268643313},
                    ]
                ),
                pd.DataFrame(
                    [
                        {"release": "feb09", "timestamp": 1614268643313},
                        {"release": "feb10", "timestamp": 1614268643313},
                    ]
                ),
                False,
                ReadS3JsonOrientRecordsAltIO,
            ),
            # ✅ Supported: index orientation, passed through as-is
            (
                {"orient": "index"},
                pd.DataFrame([{"data": {"release": "current", "timestamp": 1744281068}}]),
                pd.DataFrame([{"data": {"release": "current", "timestamp": 1744281068}}]),
                False,
                ReadS3JsonOrientRecordsIO,
            ),
        ],
    )
    def test_json_reader_applies_postprocessing_for_unsupported_orientations(self, input_options, raw_df_data, expected_df, raises_exception, io_class):
        # Given
        config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_JSON")

        # When
        with patch("dynamicio.mixins.with_s3.wr.s3.read_json", return_value=raw_df_data) as mock_reader:
            if raises_exception:
                with pytest.raises(ValueError):
                    io_class(source_config=config, **input_options).read()
            else:
                df = io_class(source_config=config, **input_options).read()
                call_kwargs = mock_reader.call_args.kwargs
                assert call_kwargs["orient"] == input_options["orient"]
                assert call_kwargs["lines"] == (input_options["orient"] == "records")
                pd.testing.assert_frame_equal(df, expected_df)

    @pytest.mark.unit
    def test_json_reader_honours_explicit_lines_false(self):
        # Given
        config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_JSON")
        raw_df_data = pd.DataFrame([{"data": {"release": "current", "timestamp": 1744281068}}])

        # When
        with patch("dynamicio.mixins.with_s3.wr.s3.read_json", return_value=raw_df_data) as mock_reader:
            ReadS3JsonOrientRecordsIO(source_config=config, orient="records", lines=False).read()

        # Then
        call_kwargs = mock_reader.call_args.kwargs
        assert call_kwargs["lines"] is False

    @pytest.mark.unit
    def test_json_reader_warns_and_ignores_convert_dates_true(self, caplog):
        # Given
        config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_JSON")
        raw_df_data = pd.DataFrame([{"data": {"release": "current", "timestamp": 1744281068}}])

        # When
        with patch("dynamicio.mixins.with_s3.wr.s3.read_json", return_value=raw_df_data) as mock_reader:
            ReadS3JsonOrientRecordsIO(source_config=config, orient="records", convert_dates=True).read()

        # Then
        call_kwargs = mock_reader.call_args.kwargs
        assert "convert_dates" not in call_kwargs
        assert "Ignoring 'convert_dates=True'" in caplog.text

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "input_options, io_class, expected_lines",
        [
            # records, default lines
            ({"orient": "records"}, WriteS3JsonOrientRecordsIO, True),
            # records, explicit lines override honoured as given
            ({"orient": "records", "lines": False}, WriteS3JsonOrientRecordsIO, False),
            # index, defaults lines to False (not records)
            ({"orient": "index"}, WriteS3JsonOrientRecordsIO, False),
            # values, explicit lines honoured
            ({"orient": "values", "lines": True}, WriteS3JsonOrientRecordsIO, True),
            # split, defaults lines to False
            ({"orient": "split"}, WriteS3JsonOrientRecordsIO, False),
        ],
    )
    def test_json_writer_passes_orient_and_lines_through(self, input_options, io_class, expected_lines):
        df_input = pd.DataFrame([{"release": "feb09", "timestamp": 1614268643313}])

        config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_JSON")

        # When
        with patch.object(dynamicio.mixins.with_s3.wr.s3, "to_json") as mock_to_json:
            io_class(source_config=config, **input_options).write(df_input)

        # Then
        mock_to_json.assert_called()
        call_kwargs = mock_to_json.call_args.kwargs
        assert call_kwargs["orient"] == input_options["orient"]
        assert call_kwargs["lines"] == expected_lines

    @pytest.mark.unit
    def test_hdf_writer_accepts_only_valid_options(self):
        # Given
        df = pd.DataFrame({"col_1": [1, 2], "col_2": ["x", "y"]})
        config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_HDF")

        mock_boto3 = MagicMock()
        with patch("dynamicio.mixins.with_s3.boto3.client", return_value=mock_boto3):
            with patch("dynamicio.mixins.with_s3.HdfIO.save") as mock_save:
                mock_save.return_value = None

                # When
                WriteS3IO(source_config=config, key="my_key", invalid_opt="nope").write(df)

        # Then
        call_kwargs = mock_save.call_args.kwargs["options"]
        assert call_kwargs.get("key") == "my_key"
        assert "invalid_opt" not in call_kwargs

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "source_key,file_type",
        [
            ("WRITE_TO_S3_PARQUET", "parquet"),
            ("WRITE_TO_S3_CSV", "csv"),
            ("WRITE_TO_S3_JSON", "json"),
        ],
    )
    def test_dataset_true_raises_value_error(self, source_key, file_type):
        # Given
        df = pd.DataFrame({"col_1": [1], "col_2": ["a"]})
        config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key=source_key)

        # When/Then
        # Patch wr.s3.to_{file_type} to prevent actual call
        patch_target = f"{dynamicio.mixins.with_s3.wr.s3.__name__}.to_{file_type}"
        with patch(patch_target):
            with pytest.raises(ValueError, match=rf"\[s3-{file_type}\] dataset=True is not supported.*"):
                WriteS3IO(source_config=config, dataset=True).write(df)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "source_key,file_type",
        [
            ("WRITE_TO_S3_PARQUET", "parquet"),
            ("WRITE_TO_S3_CSV", "csv"),
            ("WRITE_TO_S3_JSON", "json"),
        ],
    )
    def test_s3_path_as_directory_raises_value_error(self, source_key, file_type):
        # Given
        df = pd.DataFrame({"col_1": [1], "col_2": ["a"]})
        config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key=source_key)

        # Force file_path to end with slash
        config.s3.file_path = "test/some_dir/"

        # Patch wr.s3.to_{file_type} to prevent actual call
        patch_target = f"{dynamicio.mixins.with_s3.wr.s3.__name__}.to_{file_type}"
        with patch(patch_target):
            with pytest.raises(ValueError, match=rf"\[s3-{file_type}\] .*must be a file, not a directory.*"):
                WriteS3IO(source_config=config).write(df)


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
            WriteS3IO(source_config=s3_parquet_cloud_config).write(input_df)

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
        WriteS3IO(source_config=s3_parquet_cloud_config).write(input_df)

        # Then
        mock__write_to_s3_path_prefix.assert_called()

    @pytest.mark.unit
    @patch.object(WriteS3IO, "_write_parquet_file")
    # pylint: disable=unused-argument
    def test_s3_sync_is_called_with_correct_s3_path_and_aws_command_when_uploading_a_path_prefix_with_env_as_cloud_s3(self, mock__write_parquet_file, mock_temporary_directory):
        # Given
        input_df = pd.DataFrame.from_dict({"col_1": [3, 2, 1], "col_2": ["a", "b", "c"], "col_3": ["a", "b", "c"]})
        s3_parquet_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_PATH_PREFIX_PARQUET")

        # When
        with patch.object(dynamicio.mixins.with_s3, "s3_sync_up") as mocked__s3_sync:
            WriteS3IO(source_config=s3_parquet_cloud_config, partition_cols="col_2").write(input_df)

        # Then
        mocked__s3_sync.assert_called_with("temp", "s3://mock-bucket/data/some_dir/")

    @pytest.mark.unit
    # pylint: disable=unused-argument
    def test_s3_sync_is_called_with_correct_s3_path_and_aws_command_when_loading_a_path_prefix_with_env_as_cloud_s3(
        self, mock_listdir, mock_temporary_directory, mock__read_hdf_file
    ):
        # Given
        s3_hdf_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PATH_PREFIX_HDF")

        # When
        with patch.object(dynamicio.mixins.with_s3, "s3_sync_down") as mocked__s3_sync:
            ReadS3HdfIO(source_config=s3_hdf_cloud_config).read()

        # Then
        mocked__s3_sync.assert_called_with("s3://mock-bucket/data/some_dir/", "temp")

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
        with patch.object(dynamicio.mixins.with_s3, "s3_sync_down") as mocked__s3_sync:
            mocked__s3_sync.return_value = True
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
        with patch.object(dynamicio.mixins.with_s3, "s3_sync_down") as mocked__s3_sync:
            mocked__s3_sync.return_value = True
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
    def test_read_parquet_file_is_called_while_s3_sync_is_not_for_loading_a_parquet_with_env_as_cloud_s3_and_type_as_parquet_with_no_disk_space_option(
        self,
    ):
        # Given
        s3_parquet_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PATH_PREFIX_PARQUET")

        # When
        with (
            patch.object(dynamicio.mixins.with_s3, "s3_sync_down") as mock__s3_sync,
            patch.object(dynamicio.mixins.with_s3.WithS3PathPrefix, "_iter_s3_files", return_value=[io.BytesIO(b"1")]),
            patch.object(dynamicio.mixins.with_local.WithLocal, "_read_parquet_file", return_value=pd.DataFrame({"id": [1], "foo_name": ["a"], "bar": [1]})) as mock__read_parquet_file,
        ):
            ReadS3ParquetIO(source_config=s3_parquet_cloud_config, no_disk_space=True).read()

        # Then
        mock__read_parquet_file.assert_called()
        mock__s3_sync.assert_not_called()

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
        with patch.object(dynamicio.mixins.with_s3, "s3_sync_down") as mocked__s3_sync:
            mocked__s3_sync.return_value = True
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
        with patch.object(dynamicio.mixins.with_s3, "s3_sync_down") as mocked__s3_sync:
            mocked__s3_sync.return_value = True
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
        with patch.object(dynamicio.mixins.with_s3, "s3_sync_down") as mocked__s3_sync:
            mocked__s3_sync.return_value = True
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
        with patch.object(dynamicio.mixins.with_s3, "s3_sync_down") as mocked__s3_sync:
            mocked__s3_sync.return_value = True
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
        with patch.object(dynamicio.mixins.with_s3, "s3_sync_down") as mocked__s3_sync:
            mocked__s3_sync.return_Value = True
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
        with patch.object(dynamicio.mixins.with_s3, "s3_sync_down") as mocked__s3_sync:
            mocked__s3_sync.return_value = True
            df = ReadS3ParquetWEmptyFilesIO(source_config=s3_parquet_cloud_config).read()

        # Then
        assert df.shape == (10, 2) and df.columns.tolist() == ["id", "bar"]


class TestS3ParquetTransferPaths:
    """Single-file parquet goes through one shared boto3 client by default; awswrangler only on request."""

    @staticmethod
    def _config(source_key, yaml_file):
        return IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, yaml_file),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key=source_key)

    @pytest.mark.unit
    def test_default_parquet_read_uses_one_get_object_and_never_awswrangler(self, expected_s3_parquet_df):
        body = io.BytesIO()
        expected_s3_parquet_df.to_parquet(body)
        client = MagicMock()
        client.get_object.return_value = {"ContentLength": body.tell(), "Body": io.BytesIO(body.getvalue())}
        config = self._config("READ_FROM_S3_PARQUET", "definitions/input.yaml")

        with patch.object(dynamicio.mixins.with_s3, "_shared_s3_client", return_value=client), patch.object(dynamicio.mixins.with_s3.wr.s3, "read_parquet") as mock_wr:
            df = ReadS3ParquetIO(source_config=config, use_threads=True).read()

        client.get_object.assert_called_once()
        mock_wr.assert_not_called()
        assert len(df) == len(expected_s3_parquet_df)

    @pytest.mark.unit
    def test_default_parquet_write_is_one_put_object_with_acl_and_honours_pyarrow_options(self):
        df = pd.DataFrame.from_dict({"col_1": [3, 2, 1, 0], "col_2": ["a", "b", "c", "d"]})
        client = MagicMock()
        config = self._config("WRITE_TO_S3_PARQUET", "definitions/processed.yaml")

        with patch.object(dynamicio.mixins.with_s3, "_shared_s3_client", return_value=client), patch.object(dynamicio.mixins.with_s3.wr.s3, "to_parquet") as mock_wr:
            WriteS3IO(source_config=config, use_threads=True, coerce_timestamps="ms", allow_truncated_timestamps=True, row_group_size=2).write(df)

        mock_wr.assert_not_called()
        kwargs = client.put_object.call_args.kwargs
        assert kwargs["ACL"] == "bucket-owner-full-control"
        assert pq.ParquetFile(io.BytesIO(kwargs["Body"])).metadata.num_row_groups == 2

    @pytest.mark.unit
    @patch("dynamicio.mixins.with_s3.wr.s3.read_parquet")
    def test_wrangler_reads_share_one_boto3_session_and_skip_prefix_listing(self, mock_reader, expected_s3_parquet_df):
        mock_reader.return_value = expected_s3_parquet_df
        config = self._config("READ_FROM_S3_PARQUET", "definitions/input.yaml")

        ReadS3ParquetIO(source_config=config, pyarrow_additional_kwargs={}).read()
        ReadS3ParquetIO(source_config=config, pyarrow_additional_kwargs={}).read()

        first, second = (call.kwargs for call in mock_reader.call_args_list)
        assert first["boto3_session"] is second["boto3_session"]
        assert isinstance(first["path"], list) and len(first["path"]) == 1

    @pytest.mark.unit
    @patch("dynamicio.mixins.with_s3.wr.s3.read_parquet")
    def test_caller_supplied_boto3_session_is_respected(self, mock_reader, expected_s3_parquet_df):
        mock_reader.return_value = expected_s3_parquet_df
        config = self._config("READ_FROM_S3_PARQUET", "definitions/input.yaml")
        custom = object()

        ReadS3ParquetIO(source_config=config, boto3_session=custom).read()

        assert mock_reader.call_args.kwargs["boto3_session"] is custom


class TestS3CopilotRegressions:
    """S3 HDF `pickle_protocol` must reach the writer, S3 JSON must honour `single_record`, prefix sync fans out per file."""

    def test_s3_hdf_write_honours_pickle_protocol(self):
        df = pd.DataFrame({"a": [1, 2]})
        seen = {}

        class _Pickle:
            def __init__(self, protocol):
                seen["protocol"] = protocol

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        with patch.object(dynamicio.mixins.with_s3.utils, "pickle_protocol", _Pickle), patch.object(dynamicio.mixins.with_s3, "boto3") as boto:
            dynamicio.mixins.with_s3.WithS3File._write_s3_hdf_file(df, "s3://b/k.h5", pickle_protocol=4)
        assert seen["protocol"] == 4
        boto.client.return_value.upload_fileobj.assert_called_once()

    def test_s3_json_read_supports_single_record(self):
        schema = MagicMock()
        schema.columns = {"a": None}
        raw = pd.DataFrame({"a": [1, 2]}, index=["x", "y"])
        with patch.object(dynamicio.mixins.with_s3.wr.s3, "read_json", return_value=raw) as read_json:
            df = dynamicio.mixins.with_s3.WithS3File._read_s3_json_file("s3://b/k.json", schema, orient="index", single_record=True)
        assert "single_record" not in read_json.call_args.kwargs
        assert df.iloc[0, 0] == {"x": 1, "y": 2}

    def test_sync_down_and_up_use_one_request_per_file(self, tmp_path):
        client = MagicMock()
        client.get_paginator.return_value.paginate.return_value = [{"Contents": [{"Key": "p/a.parquet"}, {"Key": "p/sub/b.parquet"}, {"Key": "p/sub/"}]}]
        client.get_object.side_effect = lambda **kw: {"ContentLength": 3, "Body": io.BytesIO(b"abc")}
        with patch.object(dynamicio.mixins.with_s3, "_shared_s3_client", return_value=client):
            dynamicio.mixins.with_s3.s3_sync_down("s3://b/p/", str(tmp_path))
            assert (tmp_path / "a.parquet").read_bytes() == b"abc" and (tmp_path / "sub" / "b.parquet").read_bytes() == b"abc"
            client.head_object.assert_not_called()
            dynamicio.mixins.with_s3.s3_sync_up(str(tmp_path), "s3://b/q/", acl="private")
        keys = sorted(c.kwargs["Key"] for c in client.put_object.call_args_list)
        assert keys == ["q/a.parquet", "q/sub/b.parquet"]
        assert all(c.kwargs["ACL"] == "private" for c in client.put_object.call_args_list)
