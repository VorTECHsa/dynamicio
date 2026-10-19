# pylint: disable=no-member, missing-docstring, too-few-public-methods, protected-access
import os
from unittest.mock import patch

import pytest

# Application Imports
from dynamicio.config import IOConfig
from dynamicio.config.pydantic.io_resources import AthenaDataSubSection
from dynamicio.mixins.with_s3 import _shared_boto3_session
from tests import constants
from tests.mocking.io import ReadAthenaIO


@pytest.mark.unit
class TestAthenaIO:
    @pytest.mark.unit
    @patch("dynamicio.mixins.with_athena.wr.athena.read_sql_query")
    def test_read_from_athena_with_query(self, mock_wr_read, test_df):
        # Given
        mock_wr_read.return_value = test_df
        athena_config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_ATHENA")

        # When
        df = ReadAthenaIO(source_config=athena_config, query="SELECT * FROM dummy").read()

        # Then
        mock_wr_read.assert_called_once()
        assert df.equals(test_df)

    @pytest.mark.unit
    def test_read_from_athena_raises_when_query_missing_raises_error(self):
        # Given
        athena_config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_ATHENA")

        # When / Then
        with pytest.raises(ValueError, match="A 'query' must be provided for Athena reads"):
            ReadAthenaIO(source_config=athena_config).read()

    @pytest.mark.unit
    @patch("dynamicio.mixins.with_athena.wr.athena.read_sql_query")
    def test_read_from_athena_with_query_and_valid_options(self, mock_wr_read, test_df):
        # Given
        mock_wr_read.return_value = test_df
        athena_config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_ATHENA")

        # When
        df = ReadAthenaIO(source_config=athena_config, query="SELECT * FROM dummy", ctas_approach=True).read()

        # Then
        _, kwargs = mock_wr_read.call_args
        assert "ctas_approach" in kwargs and kwargs["ctas_approach"] is True
        assert df.equals(test_df)

    @pytest.mark.unit
    @patch("dynamicio.mixins.with_athena.wr.athena.read_sql_query")
    def test_read_from_athena_filters_invalid_options(self, mock_wr_read, test_df):
        # Given
        mock_wr_read.return_value = test_df
        athena_config = IOConfig(
            path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/input.yaml"),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="READ_FROM_ATHENA")

        # When
        df = ReadAthenaIO(source_config=athena_config, query="SELECT * FROM dummy", foo="bar").read()

        # Then
        call_kwargs = mock_wr_read.call_args.kwargs
        assert "foo" not in call_kwargs
        assert df.equals(test_df)


def _athena_config(source_key):
    yaml_file = "athena_legacy.yaml" if source_key == "READ_FROM_ATHENA_LEGACY_CONFIG" else "input.yaml"
    return IOConfig(
        path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions", yaml_file),
        env_identifier="CLOUD",
        dynamic_vars=constants,
    ).get(source_key=source_key)


@pytest.mark.unit
class TestAthenaWranglerArguments:
    """The arguments that actually reach `wr.athena.read_sql_query` (not only that it was called)."""

    @patch("dynamicio.mixins.with_athena.wr.athena.read_sql_query")
    def test_sql_database_s3_output_and_workgroup_reach_wrangler_by_keyword(self, mock_wr_read, test_df):
        mock_wr_read.return_value = test_df

        ReadAthenaIO(source_config=_athena_config("READ_FROM_ATHENA"), query="SELECT 1").read()

        args, kwargs = mock_wr_read.call_args
        assert args == ("SELECT 1",)
        assert kwargs["database"] == "test_db"
        assert kwargs["s3_output"] == "s3://test-staging/"
        assert kwargs["workgroup"] == "test_wg"

    @patch("dynamicio.mixins.with_athena.wr.athena.read_sql_query")
    def test_database_defaults_to_athenas_default_database(self, mock_wr_read, test_df):
        mock_wr_read.return_value = test_df

        ReadAthenaIO(source_config=_athena_config("READ_FROM_ATHENA_LEGACY_CONFIG")).read()

        kwargs = mock_wr_read.call_args.kwargs
        assert kwargs["database"] == "default"
        assert "workgroup" not in kwargs

    @patch("dynamicio.mixins.with_athena.wr.athena.read_sql_query")
    def test_options_override_the_resource_config(self, mock_wr_read, test_df):
        mock_wr_read.return_value = test_df

        ReadAthenaIO(source_config=_athena_config("READ_FROM_ATHENA"), query="SELECT 1", database="other_db", workgroup="other_wg").read()

        kwargs = mock_wr_read.call_args.kwargs
        assert kwargs["database"] == "other_db"
        assert kwargs["workgroup"] == "other_wg"

    @patch("dynamicio.mixins.with_athena.wr.athena.read_sql_query")
    def test_legacy_s3_staging_dir_region_and_query_still_work(self, mock_wr_read, test_df):
        mock_wr_read.return_value = test_df
        config = _athena_config("READ_FROM_ATHENA_LEGACY_CONFIG")

        ReadAthenaIO(source_config=config).read()

        args, kwargs = mock_wr_read.call_args
        assert config.athena.s3_output == "s3://test-staging-legacy/"
        assert args == ("SELECT * FROM from_yaml",)
        assert kwargs["s3_output"] == "s3://test-staging-legacy/"
        assert kwargs["boto3_session"].region_name == "eu-west-1"

    @patch("dynamicio.mixins.with_athena.wr.athena.read_sql_query")
    def test_without_region_the_shared_boto3_session_is_reused(self, mock_wr_read, test_df):
        mock_wr_read.return_value = test_df

        ReadAthenaIO(source_config=_athena_config("READ_FROM_ATHENA"), query="SELECT 1").read()

        assert mock_wr_read.call_args.kwargs["boto3_session"] is _shared_boto3_session()

    def test_s3_staging_dir_is_an_alias_of_s3_output(self):
        assert AthenaDataSubSection(s3_staging_dir="s3://a/").s3_output == "s3://a/"

    def test_conflicting_s3_staging_dir_and_s3_output_are_rejected(self):
        with pytest.raises(ValueError, match="both set"):
            AthenaDataSubSection(s3_staging_dir="s3://a/", s3_output="s3://b/")

    def test_missing_output_location_is_rejected(self):
        with pytest.raises(ValueError):
            AthenaDataSubSection()

    def test_athena_writes_are_not_supported(self, test_df):
        with pytest.raises(NotImplementedError):
            ReadAthenaIO(source_config=_athena_config("READ_FROM_ATHENA"))._write_to_athena(test_df)  # pylint: disable=protected-access
