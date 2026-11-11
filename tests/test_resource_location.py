# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
import os
from unittest.mock import patch

import pyarrow.fs as pafs
import pytest

import dynamicio.mixins.with_s3
from dynamicio.config import IOConfig
from dynamicio.resource_location import ResourceLocation
from tests import constants


class TestResourceLocation:
    @staticmethod
    def _config(env, key):
        return IOConfig(path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml"), env_identifier=env, dynamic_vars=constants).get(source_key=key)

    @pytest.mark.unit
    def test_an_s3_resource_resolves_to_the_dynamicio_file_system_and_bucket_path(self):
        # Given
        config = self._config("CLOUD", "WRITE_TO_S3_PARQUET")

        # When
        with patch.object(dynamicio.mixins.with_s3, "_s3_filesystem", return_value="the-file-system") as mock_filesystem:
            filesystem, path = ResourceLocation.of(config)

        # Then
        assert filesystem == "the-file-system"
        assert path == f"{constants.MOCK_BUCKET}/test/write_some_parquet.parquet"
        mock_filesystem.assert_called_once_with(constants.MOCK_BUCKET, write=False)

    @pytest.mark.unit
    def test_an_s3_write_location_asks_for_the_write_file_system(self):
        # When
        with patch.object(dynamicio.mixins.with_s3, "_s3_filesystem") as mock_filesystem:
            ResourceLocation.of(self._config("CLOUD", "WRITE_TO_S3_PARQUET"), write=True)

        # Then
        mock_filesystem.assert_called_once_with(constants.MOCK_BUCKET, write=True)

    @pytest.mark.unit
    def test_a_local_resource_resolves_to_a_local_file_system_and_its_path(self):
        # When
        filesystem, path = ResourceLocation.of(self._config("LOCAL", "WRITE_TO_S3_PARQUET"))

        # Then
        assert isinstance(filesystem, pafs.LocalFileSystem)
        assert path == os.path.join(constants.TEST_RESOURCES, "data/processed/write_some_parquet.parquet")

    @pytest.mark.unit
    def test_a_local_read_location_does_not_create_the_directory_but_a_write_location_does(self, tmp_path):
        # Given
        config = self._config("LOCAL", "WRITE_TO_S3_PARQUET")
        target = tmp_path / "new" / "dir"
        config.local.file_path = str(target / "{name}.parquet")

        # When
        _, read_path = ResourceLocation.of(config, name="a")
        created_by_read = target.exists()
        _, write_path = ResourceLocation.of(config, write=True, name="a")

        # Then
        assert not created_by_read and target.is_dir()
        assert read_path == write_path == str(target / "a.parquet")

    @pytest.mark.unit
    def test_placeholders_of_the_path_are_filled_from_the_template_and_missing_ones_are_reported(self):
        # Given
        config = self._config("CLOUD", "WRITE_TO_S3_PARQUET")
        config.s3.file_path = "parts/{table}/{shard}.parquet"

        # When
        with patch.object(dynamicio.mixins.with_s3, "_s3_filesystem"):
            _, path = ResourceLocation.of(config, table="events", shard="3")
            with pytest.raises(ValueError, match="Expected values for all fields"):
                ResourceLocation.of(config, table="events")

        # Then
        assert path == f"{constants.MOCK_BUCKET}/parts/events/3.parquet"

    @pytest.mark.unit
    def test_a_resource_that_is_not_a_single_file_is_rejected(self):
        # Given
        config = self._config("LOCAL", "WRITE_TO_S3_PARQUET")
        config.data_backend_type = type(config.data_backend_type)("postgres")

        # When / Then
        with pytest.raises(ValueError, match="single `local` and `s3_file` resources"):
            ResourceLocation.of(config)
