# pylint: disable=no-member, missing-docstring, too-few-public-methods, protected-access
import os
from datetime import datetime
from io import StringIO
from unittest.mock import patch

import boto3
import pandas as pd
import pytest
from botocore.stub import Stubber

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
    yaml_file = "input.yaml"
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
    def test_ctas_is_off_by_default_and_can_be_enabled(self, mock_wr_read, test_df):
        mock_wr_read.return_value = test_df
        config = _athena_config("READ_FROM_ATHENA")

        ReadAthenaIO(source_config=config, query="SELECT 1").read()
        assert mock_wr_read.call_args.kwargs["ctas_approach"] is False

        ReadAthenaIO(source_config=config, query="SELECT 1", ctas_approach=True).read()
        assert mock_wr_read.call_args.kwargs["ctas_approach"] is True

    @patch("dynamicio.mixins.with_athena.wr.athena.read_sql_query")
    def test_database_defaults_to_athenas_default_database(self, mock_wr_read, test_df):
        mock_wr_read.return_value = test_df

        ReadAthenaIO(source_config=_athena_config("READ_FROM_ATHENA_NO_DB"), query="SELECT 1").read()

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
    def test_region_and_query_from_the_resource_config_are_used(self, mock_wr_read, test_df):
        mock_wr_read.return_value = test_df

        ReadAthenaIO(source_config=_athena_config("READ_FROM_ATHENA_WITH_REGION")).read()

        args, kwargs = mock_wr_read.call_args
        assert args == ("SELECT * FROM from_yaml",)
        assert kwargs["s3_output"] == "s3://test-staging-region/"
        assert kwargs["boto3_session"].region_name == "eu-west-1"

    @patch("dynamicio.mixins.with_athena.wr.athena.read_sql_query")
    def test_without_region_the_shared_boto3_session_is_reused(self, mock_wr_read, test_df):
        mock_wr_read.return_value = test_df

        ReadAthenaIO(source_config=_athena_config("READ_FROM_ATHENA"), query="SELECT 1").read()

        assert mock_wr_read.call_args.kwargs["boto3_session"] is _shared_boto3_session()

    def test_removed_s3_staging_dir_fails_with_a_message_saying_what_to_change(self):
        with pytest.raises(ValueError, match="rename `s3_staging_dir` to `s3_output`"):
            AthenaDataSubSection(s3_staging_dir="s3://a/")

    def test_removed_s3_staging_dir_is_rejected_even_next_to_s3_output(self):
        with pytest.raises(ValueError, match="was removed in dynamicio 9.0"):
            AthenaDataSubSection(s3_staging_dir="s3://a/", s3_output="s3://a/")

    def test_missing_output_location_is_rejected(self):
        with pytest.raises(ValueError):
            AthenaDataSubSection()

    def test_athena_writes_are_not_supported(self, test_df):
        with pytest.raises(NotImplementedError):
            ReadAthenaIO(source_config=_athena_config("READ_FROM_ATHENA"))._write_to_athena(test_df)  # pylint: disable=protected-access

    def test_a_resource_definition_still_using_s3_staging_dir_fails_to_load_with_the_fix(self, tmp_path):
        yaml_file = tmp_path / "athena.yaml"
        yaml_file.write_text("OLD:\n  CLOUD:\n    type: athena\n    athena:\n      s3_staging_dir: s3://a/\n      region_name: eu-west-1\n")

        with pytest.raises(ValueError, match="rename `s3_staging_dir` to `s3_output`"):
            IOConfig(path_to_source_yaml=str(yaml_file), env_identifier="CLOUD", dynamic_vars=constants).get(source_key="OLD")


class ReadAthenaDecodedIO(ReadAthenaIO):
    """Declares dtypes for mixed-case Athena columns, as a consumer's resource schema would."""

    schema = {"Id": "int64", "Name": "object", "IsActive": "bool", "Amount": "float64", "CreatedOn": "datetime64[ns]"}


_ATHENA_COLUMNS = [("Id", "bigint"), ("Name", "varchar"), ("IsActive", "boolean"), ("Amount", "double"), ("CreatedOn", "date")]
_QUOTE_ALL_CSV = '"Id","Name","IsActive","Amount","CreatedOn"\n' '"1","alpha","true","1.5","2024-01-02"\n' '"2","","false","","2024-02-03"\n' '"3","gamma","true","3.25","2024-03-04"\n'


def _athena_session_returning(csv_text, columns):
    """A boto3 session whose Athena client is stubbed (public API shapes), and whose result CSV is parsed with wrangler's own read kwargs.

    Only the transport is faked: awswrangler still decides the dtypes, converters, NA handling and date parsing it applies to the result file.
    """
    athena = boto3.Session(region_name="eu-west-1", aws_access_key_id="x", aws_secret_access_key="x").client("athena")  # nosec
    stubber = Stubber(athena)
    stubber.add_response("get_work_group", {"WorkGroup": {"Name": "primary", "Configuration": {"EnforceWorkGroupConfiguration": False}}})
    stubber.add_response("start_query_execution", {"QueryExecutionId": "q1"})
    stubber.add_response(
        "get_query_execution",
        {
            "QueryExecution": {
                "QueryExecutionId": "q1",
                "Status": {"State": "SUCCEEDED", "SubmissionDateTime": datetime(2024, 1, 1)},
                "ResultConfiguration": {"OutputLocation": "s3://test-staging-nodb/q1.csv"},
            }
        },
    )
    stubber.add_response(
        "get_query_results",
        {
            "ResultSet": {
                "Rows": [],
                "ResultSetMetadata": {"ColumnInfo": [{"Name": name, "Type": athena_type} for name, athena_type in columns]},
            }
        },
    )
    stubber.activate()

    session = boto3.Session(region_name="eu-west-1", aws_access_key_id="x", aws_secret_access_key="x")
    session.client = lambda service_name, **_: athena

    def read_csv(path, boto3_session=None, use_threads=None, **pandas_kwargs):  # pylint: disable=unused-argument
        assert path == ["s3://test-staging-nodb/q1.csv"]
        return pd.read_csv(StringIO(csv_text), **pandas_kwargs)

    return session, stubber, read_csv


@pytest.mark.unit
class TestAthenaResultDecoding:
    """`.read()` end to end over awswrangler's real result decoding, with only the AWS transport faked."""

    @staticmethod
    def _read(io_class, csv_text, columns=None, **options):
        session, stubber, read_csv = _athena_session_returning(csv_text, columns or _ATHENA_COLUMNS)
        with patch("awswrangler.s3.read_csv", side_effect=read_csv), patch("awswrangler.s3.delete_objects"):
            df = io_class(source_config=_athena_config("READ_FROM_ATHENA_NO_DB"), query="SELECT 1", boto3_session=session, **options).read()
        stubber.assert_no_pending_responses()
        return df

    def test_columns_keep_the_case_used_in_the_query(self):
        df = self._read(ReadAthenaDecodedIO, _QUOTE_ALL_CSV)

        assert list(df.columns) == ["Id", "Name", "IsActive", "Amount", "CreatedOn"]

    def test_values_nulls_and_declared_dtypes(self):
        df = self._read(ReadAthenaDecodedIO, _QUOTE_ALL_CSV)

        assert df["Id"].tolist() == [1, 2, 3]
        assert df["IsActive"].tolist() == [True, False, True]
        assert df["Name"].tolist()[0] == "alpha" and pd.isna(df["Name"].tolist()[1])
        assert df["Amount"].isna().tolist() == [False, True, False]
        assert df["Amount"].dropna().tolist() == [1.5, 3.25]
        assert df["CreatedOn"].dt.strftime("%Y-%m-%d").tolist() == ["2024-01-02", "2024-02-03", "2024-03-04"]
        assert str(df["Id"].dtype) == "int64"
        assert str(df["IsActive"].dtype) == "bool"
        assert str(df["Amount"].dtype) == "float64"
        assert str(df["CreatedOn"].dtype) == "datetime64[ns]"

    def test_an_empty_result_keeps_the_declared_columns(self):
        df = self._read(ReadAthenaDecodedIO, '"Id","Name","IsActive","Amount","CreatedOn"\n')

        assert df.empty
        assert list(df.columns) == ["Id", "Name", "IsActive", "Amount", "CreatedOn"]

    @pytest.mark.parametrize("chunksize", [True, 1000])
    def test_chunked_reads_are_rejected_instead_of_returning_an_iterator(self, chunksize):
        with pytest.raises(ValueError, match="'chunksize' option is not supported"):
            ReadAthenaDecodedIO(source_config=_athena_config("READ_FROM_ATHENA_NO_DB"), query="SELECT 1", chunksize=chunksize).read()

    def test_wrangler_returns_nullable_dtypes_which_the_declared_schema_casts(self):
        session, _, read_csv = _athena_session_returning(_QUOTE_ALL_CSV, _ATHENA_COLUMNS)
        with patch("awswrangler.s3.read_csv", side_effect=read_csv), patch("awswrangler.s3.delete_objects"):
            raw = ReadAthenaDecodedIO(
                source_config=_athena_config("READ_FROM_ATHENA_NO_DB"), query="SELECT 1", boto3_session=session
            )._read_from_athena()  # pylint: disable=protected-access

        assert str(raw["Id"].dtype) == "Int64"
        assert str(raw["IsActive"].dtype) == "boolean"
