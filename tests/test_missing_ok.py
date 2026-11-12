# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import asyncio
from unittest.mock import patch

import pytest

# Application Imports
from dynamicio import UnifiedIO
from dynamicio.config import IOConfig
from dynamicio.errors import NoObjectsFoundError, ObjectNotFoundError
from tests import constants


class ReadOptionalParquetIO(UnifiedIO):
    schema = {"id": "int64"}


BINDING = """
OPTIONAL_PARQUET:
  LOCAL:
    type: "local"
    local:
      file_path: "{file_path}"
      file_type: "parquet"
"""


def source_config(tmp_path):
    yaml_path = tmp_path / "input.yaml"
    yaml_path.write_text(BINDING.format(file_path=tmp_path / "does_not_exist.parquet"))
    return IOConfig(path_to_source_yaml=str(yaml_path), env_identifier="LOCAL", dynamic_vars=constants).get(source_key="OPTIONAL_PARQUET")


class TestMissingOk:
    @pytest.mark.unit
    def test_missing_ok_defaults_to_false(self, tmp_path):
        # Given / When
        io = ReadOptionalParquetIO(source_config=source_config(tmp_path))

        # Then
        assert io.missing_ok is False

    @pytest.mark.unit
    def test_read_of_a_missing_file_returns_none_when_missing_ok_is_true(self, tmp_path):
        # Given
        config = source_config(tmp_path)

        # When
        df = ReadOptionalParquetIO(source_config=config, missing_ok=True).read()

        # Then
        assert df is None

    @pytest.mark.unit
    def test_async_read_of_a_missing_file_returns_none_when_missing_ok_is_true(self, tmp_path):
        # Given
        config = source_config(tmp_path)

        # When
        df = asyncio.run(ReadOptionalParquetIO(source_config=config, missing_ok=True).async_read())

        # Then
        assert df is None

    @pytest.mark.unit
    def test_missing_ok_in_a_resource_definition_is_not_a_valid_environment(self, tmp_path):
        # Given
        yaml_path = tmp_path / "input.yaml"
        yaml_path.write_text(BINDING.format(file_path=tmp_path / "x.parquet").replace("OPTIONAL_PARQUET:\n", "OPTIONAL_PARQUET:\n  missing_ok: true\n"))

        # When / Then
        with pytest.raises(Exception):
            IOConfig(path_to_source_yaml=str(yaml_path), env_identifier="LOCAL", dynamic_vars=constants).get(source_key="OPTIONAL_PARQUET")

    @pytest.mark.unit
    def test_read_of_a_missing_file_raises_when_missing_ok_is_false(self, tmp_path):
        # Given
        config = source_config(tmp_path)

        # When / Then
        with pytest.raises(FileNotFoundError):
            ReadOptionalParquetIO(source_config=config).read()

    @pytest.mark.unit
    @pytest.mark.parametrize("error", [ObjectNotFoundError("missing key"), NoObjectsFoundError("empty prefix")], ids=["s3_missing_key", "s3_empty_prefix"])
    def test_read_returns_none_for_the_s3_missing_source_errors_when_missing_ok_is_true(self, tmp_path, error):
        # Given
        config = source_config(tmp_path)

        # When
        with patch.object(ReadOptionalParquetIO, "_read_from_local", side_effect=error):
            df = ReadOptionalParquetIO(source_config=config, missing_ok=True).read()

        # Then
        assert df is None

    @pytest.mark.unit
    def test_read_raises_other_errors_when_missing_ok_is_true(self, tmp_path):
        # Given
        config = source_config(tmp_path)

        # When / Then
        with patch.object(ReadOptionalParquetIO, "_read_from_local", side_effect=PermissionError("access denied")):
            with pytest.raises(PermissionError):
                ReadOptionalParquetIO(source_config=config, missing_ok=True).read()
