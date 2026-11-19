# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import logging
import types
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest
import yaml

from dynamicio.config import IOConfig
from dynamicio.engines import EngineSetting
from dynamicio.errors import ColumnsDataTypeError, EngineResourceError, NoObjectsFoundError

pl = pytest.importorskip("polars")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")
polars_files = pytest.importorskip("tests.test_engines.test_polars_files")
PolarsScans = pytest.importorskip("dynamicio.engines.polars_scans").PolarsScans
PolarsS3 = pytest.importorskip("dynamicio.engines.polars_s3").PolarsS3
Files, FilesIO, Pair = polars_files.Files, polars_files.FilesIO, polars_files.Pair


@pytest.fixture(autouse=True)
def default_engine():
    EngineSetting.reset()
    yield
    EngineSetting.reset()


class TestScanOfLocalFiles:
    @pytest.mark.unit
    @pytest.mark.parametrize("file_type, options", Files.CASES)
    def test_a_scan_collects_to_the_frame_of_a_read(self, tmp_path, file_type, options):
        # Given
        config = Files.config(tmp_path, file_type, options)
        Files.polars(config).write(Files.frame())

        # When
        scanned = Files.polars(config).scan()

        # Then
        assert isinstance(scanned, pl.LazyFrame)
        assert scanned.collect().equals(Files.polars(config).read())
        assert scanned.collect().schema == Files.frame().schema

    @pytest.mark.unit
    @pytest.mark.parametrize("file_type, options", Files.CASES)
    def test_a_scan_of_a_file_of_the_pandas_engine_collects_to_the_frame_of_the_model(self, tmp_path, file_type, options):
        # Given
        config = Files.config(tmp_path, file_type, options)
        Files.write_with_pandas(config)

        # When
        result = Files.polars(config).scan().collect()

        # Then
        assert result.equals(Files.frame())

    @pytest.mark.unit
    @pytest.mark.parametrize("file_type", ["parquet", "csv"])
    def test_a_scan_has_the_columns_of_the_schema_only(self, tmp_path, file_type):
        # Given
        config = Files.config(tmp_path, file_type)
        getattr(pd.DataFrame({"extra": [9], "b": ["x"], "a": [1]}), f"to_{file_type}")(tmp_path / f"data.{file_type}", index=False)

        # When
        scanned = Files.polars(config, Pair).scan()

        # Then: parquet gives the order of the schema, and CSV the order of the file, as the eager read and `usecols` on pandas
        expected = {"a": pl.Int64, "b": pl.String} if file_type == "parquet" else {"b": pl.String, "a": pl.Int64}
        assert scanned.collect_schema() == pl.Schema(expected)
        assert scanned.collect().equals(Files.polars(config, Pair).read())

    @pytest.mark.unit
    def test_a_filter_and_a_selection_reach_the_parquet_scan(self, tmp_path):
        # Given
        config = Files.config(tmp_path, "parquet")
        Files.polars(config, Pair).write(pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}))

        # When
        plan = Files.polars(config, Pair).scan().filter(pl.col("a") > 1).select("b").explain()

        # Then
        scan_node = plan[plan.index("Parquet SCAN") :]
        assert "SELECTION" in scan_node and "PROJECT" in scan_node

    @pytest.mark.unit
    def test_the_cast_of_the_model_is_in_the_plan(self, tmp_path):
        # Given
        pq.write_table(pa.table({"a": pa.array([1, 2], pa.int32()), "b": ["x", None]}), tmp_path / "data.parquet")

        # When
        scanned = Files.polars(Files.config(tmp_path, "parquet"), Pair).scan()

        # Then
        assert scanned.collect_schema() == pl.Schema({"a": pl.Int64, "b": pl.String})
        assert scanned.collect().to_dicts() == [{"a": 1, "b": "x"}, {"a": 2, "b": None}]

    @pytest.mark.unit
    def test_a_failed_cast_raises_when_the_frame_is_collected(self, tmp_path):
        # Given
        pd.DataFrame({"a": ["one"], "b": ["x"]}).to_parquet(tmp_path / "data.parquet")
        scanned = Files.polars(Files.config(tmp_path, "parquet"), Pair).scan()

        # When / Then
        with pytest.raises(pl.exceptions.InvalidOperationError):
            scanned.collect()

    @pytest.mark.unit
    def test_a_scan_without_a_schema_column_raises_at_the_scan(self, tmp_path):
        # Given
        pd.DataFrame({"a": [1]}).to_parquet(tmp_path / "data.parquet")

        # When / Then
        with pytest.raises(ColumnsDataTypeError):
            Files.polars(Files.config(tmp_path, "parquet"), Pair).scan()

    @pytest.mark.unit
    def test_a_csv_scan_keeps_the_text_of_a_string_that_looks_like_a_number(self, tmp_path):
        # Given
        (tmp_path / "data.csv").write_text("a,b\n1,007\n2,NA\n")

        # When
        result = Files.polars(Files.config(tmp_path, "csv"), Pair).scan().collect()

        # Then
        assert result.to_dicts() == [{"a": 1, "b": "007"}, {"a": 2, "b": None}]

    @pytest.mark.unit
    def test_the_csv_read_options_reach_the_scan(self, tmp_path):
        # Given
        (tmp_path / "data.csv").write_text("a;b\n1;x\n2;MISSING\n")

        # When
        result = Files.polars(Files.config(tmp_path, "csv", {"sep": ";", "na_values": "MISSING"}), Pair).scan().collect()

        # Then
        assert result.to_dicts() == [{"a": 1, "b": "x"}, {"a": 2, "b": None}]

    @pytest.mark.unit
    def test_a_json_epoch_reads_as_a_utc_instant_in_a_scan(self, tmp_path):
        # Given
        (tmp_path / "data.json").write_text('[{"id": 1, "flag": true, "created_at": 1704067200000}]')
        annotations = {"id": int, "flag": bool, "created_at": polars_files.Annotated[pl.Datetime, "ns", "UTC"]}
        model = type("Epochs", (polars_files.DataModel,), {"__annotations__": annotations}, dataset="epochs")

        # When
        result = Files.polars(Files.config(tmp_path, "json", {"orient": "records"}), model).scan().collect()

        # Then
        assert result["created_at"].to_list() == [datetime(2024, 1, 1, tzinfo=timezone.utc)]


class TestScanOfLocalBatch:
    @pytest.mark.unit
    @pytest.mark.parametrize("file_type", ["parquet", "csv"])
    def test_the_files_scan_as_one_lazy_frame_in_sorted_order(self, tmp_path, file_type):
        # Given
        (tmp_path / "batch").mkdir()
        for name, rows in (("2", {"a": [3], "b": ["z"]}), ("1", {"a": [1, 2], "b": ["x", None]})):
            getattr(pd.DataFrame(rows), f"to_{file_type}")(tmp_path / "batch" / f"{name}.{file_type}", index=False)
        config = Files.config(tmp_path, file_type, source="local_batch", path_prefix=f"{tmp_path}/batch/")

        # When
        scanned = Files.polars(config, Pair).scan()

        # Then
        assert isinstance(scanned, pl.LazyFrame)
        assert scanned.collect().to_dicts() == [{"a": 1, "b": "x"}, {"a": 2, "b": None}, {"a": 3, "b": "z"}]
        assert scanned.collect().equals(Files.polars(config, Pair).read())

    @pytest.mark.unit
    def test_a_file_that_misses_a_column_has_nulls_in_it(self, tmp_path):
        # Given
        (tmp_path / "batch").mkdir()
        pd.DataFrame({"a": [2]}).to_parquet(tmp_path / "batch" / "1.parquet")
        pd.DataFrame({"a": [1], "b": ["x"]}).to_parquet(tmp_path / "batch" / "2.parquet")
        config = Files.config(tmp_path, "parquet", source="local_batch", path_prefix=f"{tmp_path}/batch/")

        # When
        result = Files.polars(config, Pair).scan().collect()

        # Then
        assert result.to_dicts() == [{"a": 2, "b": None}, {"a": 1, "b": "x"}]

    @pytest.mark.unit
    def test_json_files_scan_as_one_lazy_frame(self, tmp_path):
        # Given
        (tmp_path / "batch").mkdir()
        (tmp_path / "batch" / "1.json").write_text('[{"a": 1, "b": "x"}]')
        (tmp_path / "batch" / "2.json").write_text('[{"a": 2, "b": null}]')
        config = Files.config(tmp_path, "json", {"orient": "records"}, source="local_batch", path_prefix=f"{tmp_path}/batch/")

        # When
        result = Files.polars(config, Pair).scan().collect()

        # Then
        assert result.to_dicts() == [{"a": 1, "b": "x"}, {"a": 2, "b": None}]

    @pytest.mark.unit
    def test_a_prefix_without_files_returns_none_with_missing_ok(self, tmp_path):
        # Given
        (tmp_path / "batch").mkdir()
        config = Files.config(tmp_path, "csv", source="local_batch", path_prefix=f"{tmp_path}/batch/")

        # When / Then
        with pytest.raises(NoObjectsFoundError):
            Files.polars(config, Pair).scan()
        assert Files.polars(config, Pair, missing_ok=True).scan() is None


class TestScanOfMissingFiles:
    @pytest.mark.unit
    @pytest.mark.parametrize("file_type, options", Files.CASES)
    def test_a_missing_file_raises_file_not_found_at_the_scan(self, tmp_path, file_type, options):
        with pytest.raises(FileNotFoundError):
            Files.polars(Files.config(tmp_path, file_type, options)).scan()

    @pytest.mark.unit
    @pytest.mark.parametrize("file_type, options", Files.CASES)
    def test_a_missing_file_returns_none_with_missing_ok(self, tmp_path, file_type, options, caplog):
        # When
        with caplog.at_level(logging.WARNING):
            result = Files.polars(Files.config(tmp_path, file_type, options), missing_ok=True).scan()

        # Then
        assert result is None
        assert "does not exist and missing_ok is set" in caplog.text


class TestScanSteps:
    @pytest.mark.unit
    @pytest.mark.parametrize("flag", ["apply_schema_validations", "log_schema_metrics"])
    def test_a_scan_with_validations_or_metrics_logs_that_it_does_not_run_them(self, tmp_path, flag, caplog):
        # Given
        config = Files.config(tmp_path, "parquet")
        Files.polars(config, Pair).write(pl.DataFrame({"a": [1], "b": ["x"]}))

        # When
        with caplog.at_level(logging.WARNING):
            scanned = Files.polars(config, Pair, **{flag: True}).scan()

        # Then
        assert isinstance(scanned, pl.LazyFrame)
        assert "does not validate or log metrics" in caplog.text

    @pytest.mark.unit
    def test_a_scan_without_validations_or_metrics_logs_no_warning(self, tmp_path, caplog):
        # Given
        config = Files.config(tmp_path, "parquet")
        Files.polars(config, Pair).write(pl.DataFrame({"a": [1], "b": ["x"]}))

        # When
        with caplog.at_level(logging.WARNING):
            Files.polars(config, Pair).scan()

        # Then
        assert "does not validate" not in caplog.text

    @pytest.mark.unit
    def test_a_resource_that_is_not_a_file_is_not_scanned_yet(self, tmp_path):
        # Given
        definition = {"X": {"LOCAL": {"type": "postgres", "postgres": {"db_host": "h", "db_port": "5432", "db_name": "d", "db_user": "u", "db_password": "p"}}}}
        (tmp_path / "pg.yaml").write_text(yaml.safe_dump(definition))
        config = IOConfig(str(tmp_path / "pg.yaml"), env_identifier="LOCAL", dynamic_vars=FilesIO).get(source_key="X")

        # When / Then
        with pytest.raises(NotImplementedError, match="does not scan postgres resources yet"):
            Files.polars(config, Pair, sql_query="select 1").scan()

    @pytest.mark.unit
    def test_an_option_that_changes_after_the_build_is_refused_at_a_scan(self, tmp_path):
        # Given
        io = Files.polars(Files.config(tmp_path, "csv"), Pair)
        io.options["dtype"] = {"a": str}

        # When / Then
        with pytest.raises(EngineResourceError, match="no csv option dtype"):
            io.scan()


class TestWidestType:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "dtypes, expected",
        [
            ([pl.Int32, pl.Int64], pl.Int64),
            ([pl.Int8, pl.UInt8], pl.Int16),
            ([pl.Int32, pl.Float32], pl.Float64),
            ([pl.Float32, pl.Float64], pl.Float64),
            ([pl.Null, pl.String], pl.String),
            ([pl.Null, pl.Null], pl.Null),
            ([pl.Categorical(), pl.String], pl.String),
            ([pl.Categorical(), pl.Categorical()], pl.Categorical()),
            ([pl.Datetime("us"), pl.Datetime("ns")], pl.Datetime("ns")),
            ([pl.Datetime("ms", "UTC"), pl.Datetime("us", "UTC")], pl.Datetime("us", "UTC")),
            ([pl.Boolean, pl.Null, pl.Boolean], pl.Boolean),
        ],
    )
    def test_the_footer_types_give_the_type_of_pd_concat(self, dtypes, expected):
        assert PolarsScans.widest(types.SimpleNamespace(name="x"), "c", dtypes) == expected

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "dtypes",
        [
            [pl.String, pl.Int64],
            [pl.Int64, pl.UInt64],
            [pl.Boolean, pl.Int64],
            [pl.Datetime("us", "UTC"), pl.Datetime("us")],
            [pl.Datetime("us"), pl.Date],
            [pl.Duration("us"), pl.Int64],
        ],
    )
    def test_types_that_do_not_upcast_raise_a_columns_data_type_error(self, dtypes, caplog):
        # When
        with pytest.raises(ColumnsDataTypeError):
            PolarsScans.widest(types.SimpleNamespace(name="x"), "c", dtypes)

        # Then
        assert "for the column 'c', which do not upcast to one type" in caplog.text


class Sessions:
    @staticmethod
    def patch(monkeypatch, credentials, region="eu-west-1", endpoint="https://s3.eu-west-1.amazonaws.com"):
        session = types.SimpleNamespace(get_credentials=lambda: credentials)
        client = types.SimpleNamespace(meta=types.SimpleNamespace(region_name=region, endpoint_url=endpoint))
        monkeypatch.setattr("dynamicio.mixins.with_s3._shared_boto3_session", lambda: session)
        monkeypatch.setattr("dynamicio.mixins.with_s3._shared_s3_client", lambda: client)

    @staticmethod
    def credentials(token=None, expiry=None):
        frozen = types.SimpleNamespace(access_key="AK", secret_key="SK", token=token)
        credentials = types.SimpleNamespace(get_frozen_credentials=lambda: frozen)
        if expiry:
            credentials._expiry_time = expiry  # pylint: disable=protected-access
        return credentials


class TestS3Credentials:
    @pytest.mark.unit
    def test_the_credentials_of_the_boto3_session_reach_polars(self, monkeypatch):
        # Given
        Sessions.patch(monkeypatch, Sessions.credentials())

        # When / Then
        assert PolarsS3.credentials() == ({"aws_access_key_id": "AK", "aws_secret_access_key": "SK"}, None)

    @pytest.mark.unit
    def test_a_session_token_and_an_expiry_reach_polars(self, monkeypatch):
        # Given
        expiry = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(hours=1)
        Sessions.patch(monkeypatch, Sessions.credentials(token="T", expiry=expiry))

        # When
        values, expires = PolarsS3.credentials()

        # Then
        assert values["aws_session_token"] == "T"
        assert expires == int(expiry.timestamp())

    @pytest.mark.unit
    def test_a_session_without_credentials_raises_the_error_of_botocore(self, monkeypatch):
        # Given
        from botocore.exceptions import NoCredentialsError  # pylint: disable=import-outside-toplevel

        Sessions.patch(monkeypatch, None)

        # When / Then
        with pytest.raises(NoCredentialsError):
            PolarsS3.credentials()

    @pytest.mark.unit
    def test_an_aws_endpoint_is_left_to_polars(self, monkeypatch):
        # Given
        Sessions.patch(monkeypatch, Sessions.credentials())

        # When
        arguments = PolarsS3.arguments()

        # Then
        assert arguments["storage_options"] == {"aws_region": "eu-west-1"}
        assert arguments["credential_provider"] is PolarsS3.credentials

    @pytest.mark.unit
    @pytest.mark.parametrize("endpoint, allow_http", [("http://localhost:4566", True), ("https://minio.example.org", False)])
    def test_another_endpoint_of_the_boto3_client_reaches_polars(self, monkeypatch, endpoint, allow_http):
        # Given
        Sessions.patch(monkeypatch, Sessions.credentials(), region="", endpoint=endpoint)

        # When
        options = PolarsS3.arguments()["storage_options"]

        # Then
        assert options["aws_endpoint_url"] == endpoint
        assert ("aws_allow_http" in options) is allow_http
        assert "aws_region" not in options


class TestPrefixOptions:
    @pytest.mark.unit
    @pytest.mark.parametrize("option", [{"no_disk_space": True}, {"partition_cols": ["a"]}, {"mode": "overwrite"}, {"allow_delete": True}])
    def test_the_options_of_a_prefix_are_accepted_on_a_prefix(self, tmp_path, option):
        # Given
        definition = {"X": {"LOCAL": {"type": "s3_path_prefix", "s3": {"bucket": "b", "path_prefix": "p/", "file_type": "parquet"}, "options": option}}}
        (tmp_path / "prefix.yaml").write_text(yaml.safe_dump(definition))
        config = IOConfig(str(tmp_path / "prefix.yaml"), env_identifier="LOCAL", dynamic_vars=FilesIO).get(source_key="X")

        # When / Then
        Files.polars(config, Pair)

    @pytest.mark.unit
    def test_the_options_of_a_prefix_are_refused_on_a_file(self, tmp_path):
        with pytest.raises(EngineResourceError, match="no parquet option no_disk_space"):
            Files.polars(Files.config(tmp_path, "parquet", {"no_disk_space": True}), Pair)

    @pytest.mark.unit
    def test_an_hdf_prefix_is_refused_when_the_resource_is_built(self, tmp_path):
        # Given
        definition = {"X": {"LOCAL": {"type": "s3_path_prefix", "s3": {"bucket": "b", "path_prefix": "p/", "file_type": "hdf"}}}}
        (tmp_path / "prefix.yaml").write_text(yaml.safe_dump(definition))
        config = IOConfig(str(tmp_path / "prefix.yaml"), env_identifier="LOCAL", dynamic_vars=FilesIO).get(source_key="X")

        # When / Then
        with pytest.raises(EngineResourceError, match="not hdf files"):
            Files.polars(config, Pair)
