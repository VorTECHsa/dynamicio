# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import uuid

import pandas as pd
import pytest
from botocore.exceptions import ClientError

# Application Imports
from dynamicio.engines import EngineSetting
from dynamicio.errors import S3KeyNotFoundError, SchemaValidationError
from tests.integration.resources import Resources
from tests.integration.test_s3 import S3Objects

pl = pytest.importorskip("polars")
polars_files = pytest.importorskip("tests.test_engines.test_polars_files")
Files, FilesIO, Pair, PandasPair = polars_files.Files, polars_files.FilesIO, polars_files.Pair, polars_files.PandasPair

pytestmark = [pytest.mark.integration, pytest.mark.floci]

# The default orient of an S3 JSON object is records, as on the pandas engine
CASES = [("parquet", None), ("csv", None), ("csv", {"index": False}), ("json", None), ("json", {"lines": True, "orient": "records"})]


@pytest.fixture(autouse=True)
def default_engine():
    EngineSetting.reset()
    yield
    EngineSetting.reset()


class S3Bodies:
    @staticmethod
    def get(floci, key):
        return floci.s3_client().get_object(Bucket=floci.bucket, Key=key)["Body"].read()


class TestPolarsEngineOnS3:
    @pytest.mark.parametrize("file_type, options", CASES)
    def test_a_frame_written_to_s3_reads_back_equal(self, floci, prefix, tmp_path, file_type, options):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/data.{file_type}", file_type, options)

        # When
        Files.polars(config).write(Files.frame())
        result = Files.polars(config).read()

        # Then
        assert result.equals(Files.frame())

    @pytest.mark.parametrize("file_type, options", CASES)
    def test_the_object_has_the_bytes_of_the_pandas_engine(self, floci, prefix, tmp_path, file_type, options):
        # Given
        key = f"{prefix}/data.{file_type}"
        config = Resources.s3_file(tmp_path, floci.bucket, key, file_type, options)

        # When
        Files.polars(config).write(Files.frame())
        polars_body = S3Bodies.get(floci, key)
        Files.write_with_pandas(config)

        # Then
        assert polars_body == S3Bodies.get(floci, key)

    @pytest.mark.parametrize("file_type, options", CASES)
    def test_the_polars_engine_reads_an_object_of_the_pandas_engine(self, floci, prefix, tmp_path, file_type, options):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/data.{file_type}", file_type, options)
        Files.write_with_pandas(config)

        # When
        result = Files.polars(config).read()

        # Then
        assert result.equals(Files.frame())

    def test_a_frame_that_fails_validation_is_not_written(self, floci, prefix, tmp_path):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/data.parquet", "parquet")
        frame = Files.frame().with_columns(pl.lit(None, dtype=pl.Boolean).alias("flag"))

        # When
        with pytest.raises(SchemaValidationError):
            Files.polars(config, apply_schema_validations=True).write(frame)

        # Then
        assert not S3Objects.keys(floci, prefix)


class TestPolarsEngineOnMissingS3Objects:
    @pytest.mark.parametrize("file_type", ["parquet", "csv", "json"])
    def test_a_missing_key_raises_the_error_of_the_pandas_engine(self, floci, prefix, tmp_path, file_type):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/missing.{file_type}", file_type)
        EngineSetting.reset("pandas")
        with pytest.raises(FileNotFoundError) as pandas_error:
            FilesIO(source_config=config, schema=PandasPair).read()

        # When
        with pytest.raises(FileNotFoundError) as polars_error:
            Files.polars(config, Pair).read()

        # Then
        assert type(polars_error.value) is type(pandas_error.value)  # pylint: disable=unidiomatic-typecheck

    @pytest.mark.parametrize("file_type", ["parquet", "csv", "json"])
    def test_a_missing_key_returns_none_with_missing_ok(self, floci, prefix, tmp_path, file_type):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/missing.{file_type}", file_type)

        # When / Then
        assert Files.polars(config, Pair, missing_ok=True).read() is None

    def test_a_missing_parquet_key_has_the_code_of_s3(self, floci, prefix, tmp_path):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/missing.parquet", "parquet")

        # When / Then
        with pytest.raises(S3KeyNotFoundError) as raised:
            Files.polars(config, Pair).read()
        assert raised.value.response["Error"]["Code"] == "NoSuchKey"

    @pytest.mark.usefixtures("floci")
    def test_a_missing_bucket_is_not_a_missing_key_even_with_missing_ok(self, tmp_path):
        # Given
        config = Resources.s3_file(tmp_path, f"dynamicio-missing-{uuid.uuid4().hex[:8]}", "data.parquet", "parquet")

        # When / Then
        with pytest.raises(ClientError) as raised:
            Files.polars(config, Pair, missing_ok=True).read()
        assert not isinstance(raised.value, S3KeyNotFoundError)

    def test_a_prefix_key_is_not_read_yet(self, floci, prefix, tmp_path):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/data/", "parquet")

        # When / Then
        with pytest.raises(NotImplementedError, match="objects under an S3 prefix"):
            Files.polars(config, Pair).read()

    def test_a_parquet_write_to_a_prefix_key_is_refused(self, floci, prefix, tmp_path):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/data/", "parquet")

        # When / Then
        with pytest.raises(ValueError, match="must be a file, not a directory"):
            Files.polars(config, Pair).write(pl.DataFrame({"a": [1], "b": ["x"]}))
        assert not S3Objects.keys(floci, prefix)


class TestPandasFrameOfPolarsObjects:
    # The pandas engine of 11.1.0 reads an epoch of a JSON file as a naive datetime, and then cannot cast it to the UTC column of the model
    @pytest.mark.parametrize("file_type, options", CASES[:3])
    def test_the_pandas_engine_reads_an_object_of_the_polars_engine_as_its_own_frame(self, floci, prefix, tmp_path, file_type, options):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/data.{file_type}", file_type, options)
        Files.polars(config).write(Files.frame())

        # When
        result = Files.read_with_pandas(config)

        # Then
        pd.testing.assert_frame_equal(result, Files.pandas_frame(config))
