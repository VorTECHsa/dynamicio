# pylint: disable=no-member, too-many-positional-arguments, missing-module-docstring, missing-class-docstring, missing-function-docstring, too-many-public-methods, too-few-public-methods, protected-access, C0103, C0302, R0801
import io
import os
import shutil
import threading
import types
import urllib.parse
import urllib.request
from io import StringIO
from tempfile import NamedTemporaryFile
from unittest import mock
from unittest.mock import MagicMock, patch

import boto3
import pandas as pd
import pyarrow.fs as pafs
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
    ReadS3ParquetWithLateColumnIO,
    ReadS3ParquetWithLessColumnsIO,
    TemplatedFile,
    WriteS3IO,
    WriteS3JsonOrientRecordsIO,
)


def _fake_s3_read_down(local_dir):
    """Stand-in for `s3_read_down` that feeds the parquet files in `local_dir` to the reader, as the real one would from S3."""

    def fake(_source_url, reader, include_pattern=None, **_):  # pylint: disable=unused-argument
        results = []
        for root, _, names in sorted(os.walk(local_dir)):
            for name in sorted(names):
                with open(os.path.join(root, name), "rb") as fobj:
                    results.append(reader(io.BytesIO(fobj.read())))
        return results

    return fake


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
    @pytest.mark.parametrize("options, expected_index", [({}, True), ({"index": False}, False), ({"index": True}, True)])
    def test_s3_csv_write_honours_the_callers_index_option_and_defaults_to_pandas(self, options, expected_index):
        # Given
        df = pd.DataFrame.from_dict({"col_1": [3, 2], "col_2": ["a", "b"]})
        cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_CSV")

        # When (used to raise "got multiple values for keyword argument 'index'" when index was passed)
        with patch.object(dynamicio.mixins.with_s3.wr.s3, "to_csv") as mock__wr_s3_csv_writer:
            WriteS3IO(source_config=cloud_config, **options).write(df)

        # Then
        assert mock__wr_s3_csv_writer.call_args.kwargs["index"] is expected_index

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
            (
                "WRITE_TO_S3_PARQUET",
                "to_parquet",
                {"compression": "snappy", "sanitize_columns": False, "invalid_opt": True},
                {"compression": "snappy", "sanitize_columns": False, "dataset": False},
            ),
            (
                "WRITE_TO_S3_CSV",
                "to_csv",
                {"concurrent_partitioning": True, "compression": "gzip", "invalid_opt": 123},
                {"concurrent_partitioning": True, "compression": "gzip", "index": True},
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
                assert call_kwargs["lines"] is False  # pandas default: a JSON array, as written by earlier releases
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

    @staticmethod
    def _json_object_store():
        """Stand-in for S3 that keeps wrangler's real pandas semantics: `wr.s3.to_json` always forwards `index`, objects are plain JSON text."""
        store = {}

        def to_json(df, path, index=True, s3_additional_kwargs=None, **pandas_kwargs):  # pylint: disable=unused-argument
            store[path] = df.to_json(index=index, **pandas_kwargs)

        def read_json(path, **pandas_kwargs):
            return pd.read_json(StringIO(store[path]), **pandas_kwargs)

        return store, to_json, read_json

    @pytest.mark.unit
    def test_json_reader_reads_a_json_array_written_by_pandas_defaults(self):
        # Given an object written the way releases up to 7.x did: `df.to_json(orient="records")`, a JSON array (not JSON Lines)
        expected = pd.DataFrame({"data": ["feb09", "feb10"]})
        config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_JSON")
        store, _, read_json = self._json_object_store()
        store[f"s3://{config.s3.bucket}/{config.s3.file_path}"] = expected.to_json(orient="records")

        # When
        with patch("dynamicio.mixins.with_s3.wr.s3.read_json", side_effect=read_json):
            df = ReadS3JsonOrientRecordsIO(source_config=config, orient="records").read()

        # Then
        pd.testing.assert_frame_equal(df, expected)

    @pytest.mark.unit
    @pytest.mark.parametrize("orient", ["records", "index", "columns", "split", "values"])
    def test_json_writer_output_is_byte_identical_to_pandas_defaults(self, orient):
        # Given downstream consumers (and rollbacks to older releases) parse what pandas itself writes for the orient
        df_input = pd.DataFrame({"release": ["feb09", "feb10"], "timestamp": [1614268643313, 1614268643314]}, index=["a", "b"])
        config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_JSON")
        store, to_json, _ = self._json_object_store()

        # When
        with patch("dynamicio.mixins.with_s3.wr.s3.to_json", side_effect=to_json):
            WriteS3JsonOrientRecordsIO(source_config=config, orient=orient).write(df_input)

        # Then
        written = next(iter(store.values()))
        assert written == df_input.to_json(orient=orient)

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
            # records, default lines (a JSON array, as pandas writes it)
            ({"orient": "records"}, WriteS3JsonOrientRecordsIO, False),
            # records, explicit lines override honoured as given
            ({"orient": "records", "lines": False}, WriteS3JsonOrientRecordsIO, False),
            # index, lines False
            ({"orient": "index"}, WriteS3JsonOrientRecordsIO, False),
            # values, explicit lines honoured
            ({"orient": "values", "lines": True}, WriteS3JsonOrientRecordsIO, True),
            # split, lines False
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
    @pytest.mark.parametrize(
        "input_options, expected_index",
        [
            ({"orient": "records"}, False),
            ({"orient": "values"}, False),
            ({"orient": "index"}, True),
            ({"orient": "columns"}, True),
            ({"orient": "split"}, True),
            ({"orient": "split", "index": False}, False),
        ],
    )
    def test_json_writer_index_defaults_to_what_pandas_accepts_for_the_orient(self, input_options, expected_index):
        # Given
        df_input = pd.DataFrame({"release": ["feb09"], "timestamp": [1614268643313]}, index=["a"])
        config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_S3_JSON")

        # When
        with patch.object(dynamicio.mixins.with_s3.wr.s3, "to_json") as mock_to_json:
            WriteS3JsonOrientRecordsIO(source_config=config, **input_options).write(df_input)

        # Then the kwargs are valid for pandas itself (orient=index with index=False raises)
        call_kwargs = mock_to_json.call_args.kwargs
        assert call_kwargs["index"] is expected_index
        df_input.to_json(orient=call_kwargs["orient"], index=call_kwargs["index"], lines=call_kwargs["lines"] and call_kwargs["orient"] in ("records", "values"))

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
    def test_s3_sync_is_called_with_correct_s3_path_and_aws_command_when_loading_a_path_prefix_with_env_as_cloud_s3(self, mock_listdir, mock_temporary_directory, mock__read_hdf_file):
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
    def test__read_parquet_file_is_called_once_per_object_when_loading_a_path_prefix_with_env_as_cloud_s3_and_type_as_parquet(self):
        # Given
        s3_parquet_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PATH_PREFIX_PARQUET")
        fobjs = [io.BytesIO(b"1"), io.BytesIO(b"2"), io.BytesIO(b"3")]
        df = pd.DataFrame({"id": [1], "foo_name": ["a"], "bar": [1]})

        # When
        with (
            patch.object(dynamicio.mixins.with_s3, "s3_read_down", side_effect=lambda _url, reader, **_: [reader(f) for f in fobjs]),
            patch.object(dynamicio.mixins.with_local.WithLocal, "_read_parquet_file", return_value=df) as mock__read_parquet_file,
        ):
            read_obj = ReadS3ParquetIO(source_config=s3_parquet_cloud_config)
            actual_schema = read_obj.schema
            result = read_obj.read()

        # Then
        assert len(mock__read_parquet_file.mock_calls) == 3
        mock__read_parquet_file.assert_has_calls([mock.call(f, actual_schema) for f in fobjs])
        assert len(result) == 3

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
            patch.object(dynamicio.mixins.with_s3, "s3_read_down", side_effect=lambda _url, reader, **_: [reader(io.BytesIO(b"1"))]),
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
        with patch.object(dynamicio.mixins.with_s3, "s3_read_down", side_effect=_fake_s3_read_down(os.path.join(constants.TEST_RESOURCES, "data/input/batch/parquet"))):
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
        with patch.object(dynamicio.mixins.with_s3, "s3_read_down", side_effect=_fake_s3_read_down(os.path.join(constants.TEST_RESOURCES, "data/input/batch/parquet"))):
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
        with patch.object(dynamicio.mixins.with_s3, "s3_read_down", side_effect=_fake_s3_read_down(os.path.join(constants.TEST_RESOURCES, "data/input/batch/parquet_w_empty_files"))):
            df = ReadS3ParquetWEmptyFilesIO(source_config=s3_parquet_cloud_config).read()

        # Then
        assert df.shape == (10, 2) and df.columns.tolist() == ["id", "bar"]


class TestS3PathPrefixParquetSchemaEvolution:
    """Prefixes accumulate files over years: objects written before a column was added must not break reads."""

    @staticmethod
    def _config():
        return IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PATH_PREFIX_PARQUET")

    @staticmethod
    def _parquet(df):
        buffer = io.BytesIO()
        df.to_parquet(buffer)
        buffer.seek(0)
        return buffer

    @classmethod
    def _old_and_new(cls):
        old = pd.DataFrame({"id": [1, 2], "foo_name": ["a", "b"]})  # predates `model_action`
        new = pd.DataFrame({"id": [3], "foo_name": ["c"], "model_action": ["x"]})
        return old, new

    @pytest.mark.unit
    @pytest.mark.parametrize("old_first", [True, False])
    def test_objects_lacking_a_schema_column_are_filled_with_nulls_whatever_the_listing_order(self, old_first):
        # Given
        old, new = self._old_and_new()
        frames = [old, new] if old_first else [new, old]

        # When
        with patch.object(dynamicio.mixins.with_s3, "s3_read_down", side_effect=lambda _url, reader, **_: [reader(self._parquet(df)) for df in frames]):
            df = ReadS3ParquetWithLateColumnIO(source_config=self._config()).read()

        # Then
        assert df.columns.tolist() == ["id", "foo_name", "model_action"]
        assert sorted(df["id"].tolist()) == [1, 2, 3]
        assert df.loc[df["id"] == 3, "model_action"].iloc[0] == "x"
        assert df.loc[df["id"].isin([1, 2]), "model_action"].isna().all()

    @pytest.mark.unit
    def test_objects_lacking_a_schema_column_are_filled_with_nulls_with_no_disk_space_option(self):
        # Given
        old, new = self._old_and_new()

        # When
        with patch.object(dynamicio.mixins.with_s3, "s3_read_down", side_effect=lambda _url, reader, **_: [reader(self._parquet(old)), reader(self._parquet(new))]):
            df = ReadS3ParquetWithLateColumnIO(source_config=self._config(), no_disk_space=True).read()

        # Then
        assert df.columns.tolist() == ["id", "foo_name", "model_action"]
        assert len(df) == 3 and df["model_action"].isna().sum() == 2

    @pytest.mark.unit
    def test_prefix_whose_objects_are_all_empty_returns_an_empty_frame_with_schema_columns(self):
        # Given
        _, new = self._old_and_new()

        # When
        with patch.object(dynamicio.mixins.with_s3, "s3_read_down", side_effect=lambda _url, reader, **_: [reader(self._parquet(new.iloc[0:0]))]):
            df = ReadS3ParquetWithLateColumnIO(source_config=self._config(), no_disk_space=True).read()

        # Then
        assert df.empty and "id" in df.columns

    @pytest.mark.unit
    def test_null_filled_object_columns_hold_none_like_a_pyarrow_dataset_read(self):
        # Given
        old, new = self._old_and_new()

        # When
        with patch.object(dynamicio.mixins.with_s3, "s3_read_down", side_effect=lambda _url, reader, **_: [reader(self._parquet(old)), reader(self._parquet(new))]):
            df = ReadS3ParquetWithLateColumnIO(source_config=self._config()).read()

        # Then
        assert df["model_action"].tolist() == [None, None, "x"]

    @pytest.mark.unit
    def test_s3_file_path_ending_in_a_slash_reads_all_objects_under_it_like_a_dataset(self):
        # Given a single-file source whose file_path is a "directory" (downstream pipelines read one this way)
        old, new = self._old_and_new()
        schema = ReadS3ParquetWithLateColumnIO(source_config=self._config()).schema

        # When
        with (
            patch.object(dynamicio.mixins.with_s3, "s3_read_down", side_effect=lambda _url, reader, **_: [reader(self._parquet(old)), reader(self._parquet(new))]) as mock_read_down,
            patch.object(dynamicio.mixins.with_s3, "_download_to_memory") as mock_download,
        ):
            df = dynamicio.mixins.with_s3.WithS3File._read_s3_parquet_file("s3://bucket/dir/", schema)

        # Then no GetObject on the "dir/" key, objects concatenated in listing order, missing columns null-filled
        mock_download.assert_not_called()
        mock_read_down.assert_called_once()
        assert df["id"].tolist() == [1, 2, 3] and df["model_action"].tolist() == [None, None, "x"]

    @pytest.mark.unit
    def test_s3_file_path_ending_in_a_slash_with_no_objects_raises(self):
        schema = ReadS3ParquetWithLateColumnIO(source_config=self._config()).schema
        with patch.object(dynamicio.mixins.with_s3, "s3_read_down", return_value=[]), pytest.raises(FileNotFoundError, match="No objects found under s3://bucket/dir/"):
            dynamicio.mixins.with_s3.WithS3File._read_s3_parquet_file("s3://bucket/dir/", schema)

    @pytest.mark.unit
    def test_a_warning_names_the_columns_missing_from_some_objects(self):
        # Given
        old, new = self._old_and_new()

        # When
        with (
            patch.object(dynamicio.mixins.with_s3, "s3_read_down", side_effect=lambda _url, reader, **_: [reader(self._parquet(df)) for df in (old, old, new)]),
            patch.object(dynamicio.mixins.with_s3, "logger") as mock_logger,
        ):
            ReadS3ParquetWithLateColumnIO(source_config=self._config()).read()

        # Then
        (message,) = [c.args[0] for c in mock_logger.warning.call_args_list]
        assert "'model_action': 2" in message

    @pytest.mark.unit
    def test_no_warning_is_logged_when_every_object_has_all_columns(self):
        # Given
        _, new = self._old_and_new()

        # When
        with (
            patch.object(dynamicio.mixins.with_s3, "s3_read_down", side_effect=lambda _url, reader, **_: [reader(self._parquet(new))]),
            patch.object(dynamicio.mixins.with_s3, "logger") as mock_logger,
        ):
            ReadS3ParquetWithLateColumnIO(source_config=self._config()).read()

        # Then
        mock_logger.warning.assert_not_called()

    @pytest.mark.unit
    def test_a_column_missing_from_every_object_still_raises(self):
        # Given
        old, _ = self._old_and_new()

        # When / Then
        with patch.object(dynamicio.mixins.with_s3, "s3_read_down", side_effect=lambda _url, reader, **_: [reader(self._parquet(old)), reader(self._parquet(old))]):
            with pytest.raises(ValueError, match="missing from every parquet object"):
                ReadS3ParquetWithLateColumnIO(source_config=self._config()).read()


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

    def test_read_down_parses_in_memory_in_key_order_with_one_request_per_file(self):
        client = MagicMock()
        client.get_paginator.return_value.paginate.return_value = [{"Contents": [{"Key": "p/b.parquet"}, {"Key": "p/a.parquet"}, {"Key": "p/skip.csv"}, {"Key": "p/sub/"}]}]
        client.get_object.side_effect = lambda **kw: {"ContentLength": 1, "Body": io.BytesIO(kw["Key"].encode())}
        with patch.object(dynamicio.mixins.with_s3, "_shared_s3_client", return_value=client):
            results = dynamicio.mixins.with_s3.s3_read_down("s3://b/p/", lambda fobj: fobj.read(), include_pattern="*.parquet")
        assert results == [b"p/b.parquet", b"p/a.parquet"]
        assert client.get_object.call_count == 2
        client.head_object.assert_not_called()


class TestS3ParquetRangedRead:
    """A single-file parquet read with `filters` reads byte ranges over pyarrow's S3 file system instead of the whole object."""

    KEY = "s3:sample-prefix/mock-key"

    @staticmethod
    def _config():
        return IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PARQUET")

    @pytest.fixture
    def bucket_root(self, tmp_path):
        df = pd.DataFrame({"id": list(range(12)), "foo_name": [f"name_{i}" for i in range(12)], "bar": [i * 10 for i in range(12)]})
        path = tmp_path / constants.MOCK_BUCKET / self.KEY
        path.parent.mkdir(parents=True)
        df.to_parquet(path, row_group_size=4, index=False)
        return tmp_path

    @staticmethod
    def _local_s3(root):
        return pafs.SubTreeFileSystem(str(root), pafs.LocalFileSystem())

    def _object_path(self, root):
        return root / constants.MOCK_BUCKET / self.KEY

    def _corrupt_row_groups(self, root, row_groups):
        path = self._object_path(root)
        metadata = pq.ParquetFile(path).metadata
        with open(path, "r+b") as fobj:
            for index in row_groups:
                for column_index in range(metadata.num_columns):
                    column = metadata.row_group(index).column(column_index)
                    start = column.dictionary_page_offset if column.has_dictionary_page else column.data_page_offset
                    fobj.seek(start)
                    fobj.write(b"\xff" * column.total_compressed_size)

    @pytest.mark.unit
    def test_filtered_read_skips_row_groups_whose_statistics_cannot_match(self, bucket_root):
        # Given
        self._corrupt_row_groups(bucket_root, [1, 2])
        with pytest.raises(Exception):
            pd.read_parquet(self._object_path(bucket_root))

        # When
        with (
            patch.object(dynamicio.mixins.with_s3, "_s3_filesystem", return_value=self._local_s3(bucket_root)),
            patch.object(dynamicio.mixins.with_s3, "_download_to_memory") as mock_download,
        ):
            df = ReadS3ParquetIO(source_config=self._config(), filters=[("id", "<=", 3)]).read()

        # Then
        mock_download.assert_not_called()
        assert df["id"].tolist() == [0, 1, 2, 3] and df.columns.tolist() == ["id", "foo_name", "bar"]

    @pytest.mark.unit
    def test_filtered_read_returns_the_same_frame_as_the_full_download(self, bucket_root):
        # Given
        filters = [("id", ">=", 3), ("id", "<=", 9)]
        body = io.BytesIO(self._object_path(bucket_root).read_bytes())

        # When
        with patch.object(dynamicio.mixins.with_s3, "_s3_filesystem", return_value=self._local_s3(bucket_root)):
            ranged = ReadS3ParquetIO(source_config=self._config(), filters=filters).read()
        downloaded = dynamicio.mixins.with_local.WithLocal._read_parquet_file(body, ReadS3ParquetIO(source_config=self._config()).schema, filters=filters)

        # Then
        assert ranged["id"].tolist() == list(range(3, 10))
        pd.testing.assert_frame_equal(ranged, downloaded)

    @pytest.mark.unit
    def test_filtered_read_opens_the_bucket_and_key_on_the_s3_file_system(self):
        # Given
        schema = ReadS3ParquetIO(source_config=self._config()).schema

        # When
        with patch.object(dynamicio.mixins.with_s3, "_s3_filesystem") as mock_filesystem, patch.object(dynamicio.mixins.with_s3.pd, "read_parquet") as mock_read:
            dynamicio.mixins.with_s3.WithS3File._read_s3_parquet_file("s3://bucket/dir/file.parquet", schema, filters=[("id", "==", 1)])

        # Then
        mock_filesystem.assert_called_once_with("bucket")
        assert mock_read.call_args.args == ("bucket/dir/file.parquet",)
        assert mock_read.call_args.kwargs["filesystem"] is mock_filesystem.return_value
        assert mock_read.call_args.kwargs["filters"] == [("id", "==", 1)]

    @pytest.mark.unit
    @pytest.mark.parametrize("options", [{}, {"filters": None}, {"filters": [("id", "==", 1)], "engine": "fastparquet"}])
    def test_read_without_filters_downloads_the_whole_object(self, options, expected_s3_parquet_df):
        # Given
        body = io.BytesIO()
        expected_s3_parquet_df.to_parquet(body)
        body.seek(0)
        schema = ReadS3ParquetIO(source_config=self._config()).schema

        # When
        with (
            patch.object(dynamicio.mixins.with_s3, "_s3_filesystem") as mock_filesystem,
            patch.object(dynamicio.mixins.with_s3, "_download_to_memory", return_value=body) as mock_download,
            patch.object(dynamicio.mixins.with_local.WithLocal, "_read_parquet_file") as mock_read,
        ):
            dynamicio.mixins.with_s3.WithS3File._read_s3_parquet_file("s3://bucket/dir/file.parquet", schema, **options)

        # Then
        mock_filesystem.assert_not_called()
        mock_download.assert_called_once_with("bucket", "dir/file.parquet")
        assert mock_read.call_args.args[0] is body

    @pytest.mark.unit
    @pytest.mark.parametrize("options", [{"filters": [("id", "==", 1)]}, {"filters": [("id", "==", 1)], "engine": "auto"}])
    def test_filtered_read_downloads_the_whole_object_when_the_pandas_default_engine_is_fastparquet(self, options, expected_s3_parquet_df):
        # Given
        body = io.BytesIO()
        expected_s3_parquet_df.to_parquet(body)
        body.seek(0)
        schema = ReadS3ParquetIO(source_config=self._config()).schema

        # When
        with (
            pd.option_context("io.parquet.engine", "fastparquet"),
            patch.object(dynamicio.mixins.with_s3, "_s3_filesystem") as mock_filesystem,
            patch.object(dynamicio.mixins.with_s3, "_download_to_memory", return_value=body) as mock_download,
            patch.object(dynamicio.mixins.with_local.WithLocal, "_read_parquet_file") as mock_read,
        ):
            dynamicio.mixins.with_s3.WithS3File._read_s3_parquet_file("s3://bucket/dir/file.parquet", schema, **options)

        # Then
        mock_filesystem.assert_not_called()
        mock_download.assert_called_once_with("bucket", "dir/file.parquet")
        assert mock_read.call_args.args[0] is body

    @pytest.mark.unit
    @patch("dynamicio.mixins.with_s3.wr.s3.read_parquet")
    def test_wrangler_only_options_take_precedence_over_the_ranged_read(self, mock_reader, expected_s3_parquet_df):
        # Given
        mock_reader.return_value = expected_s3_parquet_df

        # When
        with patch.object(dynamicio.mixins.with_s3, "_s3_filesystem") as mock_filesystem:
            ReadS3ParquetIO(source_config=self._config(), pyarrow_additional_kwargs={}, filters=[("id", "==", 1)]).read()

        # Then
        mock_reader.assert_called_once()
        mock_filesystem.assert_not_called()

    @pytest.mark.unit
    def test_caller_supplied_filesystem_is_respected(self, bucket_root):
        # Given
        schema = ReadS3ParquetIO(source_config=self._config()).schema
        caller_filesystem = self._local_s3(bucket_root)

        # When
        with patch.object(dynamicio.mixins.with_s3, "_s3_filesystem") as mock_filesystem:
            df = dynamicio.mixins.with_s3.WithS3File._read_s3_parquet_file(f"s3://{constants.MOCK_BUCKET}/{self.KEY}", schema, filters=[("id", "==", 1)], filesystem=caller_filesystem)

        # Then
        assert df["id"].tolist() == [1]
        mock_filesystem.assert_not_called()

    @pytest.mark.unit
    def test_explicit_none_filesystem_uses_the_s3_file_system(self, bucket_root):
        # Given
        schema = ReadS3ParquetIO(source_config=self._config()).schema

        # When
        with patch.object(dynamicio.mixins.with_s3, "_s3_filesystem", return_value=self._local_s3(bucket_root)) as mock_filesystem:
            df = dynamicio.mixins.with_s3.WithS3File._read_s3_parquet_file(f"s3://{constants.MOCK_BUCKET}/{self.KEY}", schema, filters=[("id", "==", 1)], filesystem=None)

        # Then
        assert df["id"].tolist() == [1]
        mock_filesystem.assert_called_once_with(constants.MOCK_BUCKET)

    @pytest.mark.unit
    def test_filtered_read_does_not_take_partition_columns_from_hive_style_keys(self, tmp_path):
        # Given
        df = pd.DataFrame({"id": [1, 2, 3], "foo_name": ["a", "b", "c"], "bar": [10, 20, 30]})
        for key in ["id=1/data.parquet", "day=7/data.parquet"]:
            (tmp_path / constants.MOCK_BUCKET / key).parent.mkdir(parents=True)
            df.to_parquet(tmp_path / constants.MOCK_BUCKET / key, index=False)
        schema = ReadS3ParquetIO(source_config=self._config()).schema

        # When
        with patch.object(dynamicio.mixins.with_s3, "_s3_filesystem", return_value=self._local_s3(tmp_path)):
            frames = [
                dynamicio.mixins.with_s3.WithS3File._read_s3_parquet_file(f"s3://{constants.MOCK_BUCKET}/{key}", schema, filters=[("id", "<", 3)])
                for key in ["id=1/data.parquet", "day=7/data.parquet"]
            ]

        # Then
        for frame in frames:
            pd.testing.assert_frame_equal(frame, df[df["id"] < 3])

    @pytest.mark.unit
    def test_s3_filesystem_is_built_once_per_process_and_bucket_in_the_bucket_region(self):
        # Given
        dynamicio.mixins.with_s3._FILESYSTEM.clear()

        # When
        with (
            patch.object(dynamicio.mixins.with_s3.pafs, "S3FileSystem", side_effect=lambda **_: object()) as mock_s3,
            patch.object(dynamicio.mixins.with_s3.pafs, "resolve_s3_region", return_value="eu-west-1") as mock_region,
        ):
            first = dynamicio.mixins.with_s3._s3_filesystem("bucket-a")
            again = dynamicio.mixins.with_s3._s3_filesystem("bucket-a")
            other = dynamicio.mixins.with_s3._s3_filesystem("bucket-b")
            with patch.object(dynamicio.mixins.with_s3.os, "getpid", return_value=-1):
                forked = dynamicio.mixins.with_s3._s3_filesystem("bucket-a")
        dynamicio.mixins.with_s3._FILESYSTEM.clear()

        # Then
        assert first is again
        assert other is not first and forked is not first
        assert mock_s3.call_count == 3
        mock_s3.assert_called_with(region="eu-west-1")
        assert [call.args for call in mock_region.call_args_list] == [("bucket-a",), ("bucket-b",), ("bucket-a",)]


class TestS3ParquetRangedReadOverS3Protocol:
    """The same reads against moto's S3 server: pyarrow and boto3 send real HTTP requests, so the byte ranges can be checked."""

    KEY = "s3:sample-prefix/mock-key"

    @staticmethod
    def _config():
        return IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_S3_PARQUET")

    @pytest.fixture
    def moto_s3_server(self, monkeypatch):
        """Run moto's S3 as a local HTTP server, recording (method, path, Range header, bytes sent) for every request.

        pyarrow's S3 client is C++, so moto's in-process `mock_aws` does not see its calls; a server does.
        """
        moto_server = pytest.importorskip("moto.server")  # dev dependency only; the dependency-ranges CI jobs do not install it
        serving = pytest.importorskip("werkzeug.serving")
        for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
            monkeypatch.setenv(name, "testing")
        monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
        requests: list = []
        app = moto_server.DomainDispatcherApplication(moto_server.create_backend_app)

        def recording_app(environ, start_response):
            record = {"method": environ["REQUEST_METHOD"], "path": urllib.parse.unquote(environ["PATH_INFO"]), "range": environ.get("HTTP_RANGE"), "bytes": 0}
            requests.append(record)

            def recording_start_response(status, headers, *args):
                record["bytes"] = int(dict(headers).get("Content-Length", 0)) if record["method"] == "GET" else 0
                return start_response(status, headers, *args)

            return app(environ, recording_start_response)

        server = serving.make_server("127.0.0.1", 0, recording_app, threaded=True)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        endpoint = f"127.0.0.1:{server.server_port}"
        yield types.SimpleNamespace(
            endpoint=endpoint,
            requests=requests,
            client=boto3.client("s3", endpoint_url=f"http://{endpoint}", region_name="us-east-1"),
            filesystem=pafs.S3FileSystem(endpoint_override=endpoint, scheme="http", access_key="testing", secret_key="testing", region="us-east-1"),
        )
        urllib.request.urlopen(urllib.request.Request(f"http://{endpoint}/moto-api/reset", method="POST"))  # pylint: disable=consider-using-with
        server.shutdown()

    @pytest.fixture
    def s3_object(self, moto_s3_server):
        df = pd.DataFrame({"id": list(range(300_000)), "foo_name": [f"name_{i}" for i in range(300_000)], "bar": [i * 10 for i in range(300_000)]})
        body = io.BytesIO()
        df.to_parquet(body, row_group_size=100_000, index=False)
        moto_s3_server.client.create_bucket(Bucket=constants.MOCK_BUCKET)
        moto_s3_server.client.put_object(Bucket=constants.MOCK_BUCKET, Key=self.KEY, Body=body.getvalue())
        moto_s3_server.requests.clear()
        return body.getvalue()

    def _object_gets(self, server):
        return [r for r in server.requests if r["method"] == "GET" and r["path"] == f"/{constants.MOCK_BUCKET}/{self.KEY}"]

    @pytest.mark.unit
    def test_filtered_read_requests_only_byte_ranges_outside_the_skipped_row_groups(self, moto_s3_server, s3_object):
        # Given
        metadata = pq.ParquetFile(io.BytesIO(s3_object)).metadata
        skipped = []
        for index in (1, 2):
            for column_index in range(metadata.num_columns):
                column = metadata.row_group(index).column(column_index)
                start = column.dictionary_page_offset if column.has_dictionary_page else column.data_page_offset
                skipped.append((start, start + column.total_compressed_size))

        # When
        with (
            patch.object(dynamicio.mixins.with_s3, "_s3_filesystem", return_value=moto_s3_server.filesystem),
            patch.object(dynamicio.mixins.with_s3, "_download_to_memory") as mock_download,
        ):
            df = ReadS3ParquetIO(source_config=self._config(), filters=[("id", "<=", 99_999)]).read()

        # Then
        mock_download.assert_not_called()
        assert df["id"].tolist() == list(range(100_000))
        gets = self._object_gets(moto_s3_server)
        assert gets and all(r["range"] for r in gets)
        requested = [tuple(int(n) for n in r["range"].removeprefix("bytes=").split("-")) for r in gets]
        footer_reads = [(a, b) for a, b in requested if b == len(s3_object) - 1]  # pyarrow reads the file tail speculatively to find the footer
        data_reads = [(a, b) for a, b in requested if b != len(s3_object) - 1]
        assert footer_reads and data_reads
        assert not [(a, b) for a, b in data_reads for s, e in skipped if a < e and b >= s]
        assert sum(r["bytes"] for r in gets) < len(s3_object) / 2

    @pytest.mark.unit
    @pytest.mark.usefixtures("s3_object")
    def test_filtered_read_returns_the_same_frame_as_the_full_download_over_s3(self, moto_s3_server):
        # Given
        filters = [("id", ">=", 150_000), ("id", "<=", 250_000)]

        # When
        with patch.object(dynamicio.mixins.with_s3, "_s3_filesystem", return_value=moto_s3_server.filesystem):
            ranged = ReadS3ParquetIO(source_config=self._config(), filters=filters).read()
        with patch.object(dynamicio.mixins.with_s3, "_shared_s3_client", return_value=moto_s3_server.client):
            body = dynamicio.mixins.with_s3._download_to_memory(constants.MOCK_BUCKET, self.KEY)
        downloaded = dynamicio.mixins.with_local.WithLocal._read_parquet_file(body, ReadS3ParquetIO(source_config=self._config()).schema, filters=filters)

        # Then
        assert ranged["id"].tolist() == list(range(150_000, 250_001))
        pd.testing.assert_frame_equal(ranged, downloaded)

    @pytest.mark.unit
    def test_read_without_filters_is_one_whole_object_get_over_boto3(self, moto_s3_server, s3_object):
        # When
        with (
            patch.object(dynamicio.mixins.with_s3, "_shared_s3_client", return_value=moto_s3_server.client),
            patch.object(dynamicio.mixins.with_s3, "_s3_filesystem") as mock_filesystem,
        ):
            df = ReadS3ParquetIO(source_config=self._config()).read()

        # Then
        mock_filesystem.assert_not_called()
        gets = self._object_gets(moto_s3_server)
        assert len(gets) == 1 and gets[0]["range"] is None and gets[0]["bytes"] == len(s3_object)
        assert len(df) == 300_000
