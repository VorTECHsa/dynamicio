# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, redefined-outer-name
import json
import logging

import pandas as pd
import pytest

# Application Imports
import dynamicio
from dynamicio.engines import EngineSetting
from dynamicio.errors import ModelEngineMismatchError, SchemaValidationError, WrongFrameError
from tests.integration.resources import Resources
from tests.integration.test_kafka import Broker
from tests.integration.test_s3 import S3Objects
from tests.mocking.io import ReadPostgresIO, WriteKafkaIO, WritePostgresIO, WriteS3IO
from tests.test_engines.ios import EngineCheckIO

pytestmark = [pytest.mark.integration, pytest.mark.floci]


@pytest.fixture(autouse=True)
def default_engine():
    EngineSetting.reset()
    yield
    EngineSetting.reset()


class Metrics:
    @staticmethod
    def logged(caplog):
        return [json.loads(record.getMessage()) for record in caplog.records if record.getMessage().startswith('{"message": "METRIC"')]


class TestPandasEngineOnS3:
    @pytest.mark.parametrize("file_type", ["parquet", "csv", "json"])
    def test_a_validated_write_reads_back_with_the_same_metrics(self, floci, prefix, tmp_path, caplog, file_type):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/data.{file_type}", file_type)
        df = pd.DataFrame({"a": [3, 1, 2]})
        io = EngineCheckIO.make(tmp_path, source_config=config, apply_schema_validations=True, log_schema_metrics=True)

        # When
        with caplog.at_level(logging.INFO):
            io.write(df)
            result = io.read()

        # Then
        pd.testing.assert_frame_equal(result, df)
        assert [(m["dataset"], m["column"], m["metric"], m["value"]) for m in Metrics.logged(caplog)] == [("POSITIVE", "a", "Min", 1.0)] * 2

    def test_a_frame_that_fails_validation_is_not_written(self, floci, prefix, tmp_path):
        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/data.parquet", "parquet")

        # When
        with pytest.raises(SchemaValidationError):
            EngineCheckIO.make(tmp_path, source_config=config, apply_schema_validations=True).write(pd.DataFrame({"a": [0]}))

        # Then
        assert not S3Objects.keys(floci, prefix)

    def test_a_polars_frame_is_refused_before_s3_is_called(self, floci, prefix, tmp_path):
        pl = pytest.importorskip("polars")

        # Given
        config = Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/data.parquet", "parquet")

        # When
        with pytest.raises(WrongFrameError):
            WriteS3IO(source_config=config).write(pl.DataFrame({"col_1": [1], "col_2": ["a"]}))

        # Then
        assert not S3Objects.keys(floci, prefix)

    def test_a_write_after_a_switch_to_polars_is_refused_before_s3_is_called(self, floci, prefix, tmp_path):
        pytest.importorskip("polars")

        # Given
        io = WriteS3IO(source_config=Resources.s3_file(tmp_path, floci.bucket, f"{prefix}/data.parquet", "parquet"))
        dynamicio.configure(engine="polars")

        # When
        with pytest.raises(ModelEngineMismatchError):
            io.write(pd.DataFrame({"col_1": [1], "col_2": ["a"]}))

        # Then
        assert not S3Objects.keys(floci, prefix)


class TestPandasEngineOnPostgres:
    def test_a_validated_write_reads_back_the_same_rows(self, pg_config):
        # Given
        df = pd.DataFrame({"id": ["1", "2"], "foo": ["a", None], "bar": [1, 2], "baz": ["x", "y"]})

        # When
        WritePostgresIO(source_config=pg_config, create_table=True, apply_schema_validations=True, log_schema_metrics=True).write(df)
        result = ReadPostgresIO(source_config=pg_config, apply_schema_validations=True).read()

        # Then
        pd.testing.assert_frame_equal(result.sort_values("bar").reset_index(drop=True), df)


class TestPandasEngineOnKafka:
    def test_a_validated_write_reaches_the_broker(self, floci, topic, tmp_path):
        # Given
        df = pd.DataFrame({"id": ["a", "b"], "foo": ["x", None], "bar": [1, 2], "baz": [None, "q"]})

        # When
        WriteKafkaIO(Resources.kafka(tmp_path, floci, topic), flush_timeout=30, apply_schema_validations=True).write(df)

        # Then
        assert len(Broker.consume(floci, topic, len(df))) == len(df)
