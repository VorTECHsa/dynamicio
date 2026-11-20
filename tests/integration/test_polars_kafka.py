# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import time
from unittest.mock import patch

import pandas as pd
import pytest

# Application Imports
from dynamicio.engines import EngineSetting
from tests.integration.resources import Resources
from tests.integration.test_kafka import Broker
from tests.mocking.io import WriteKafkaIO, WriteKeyedKafkaIO

pl = pytest.importorskip("polars")
kafka = pytest.importorskip("tests.test_engines.test_polars_kafka")
PolarsKafka = pytest.importorskip("dynamicio.engines.polars_kafka").PolarsKafka
FilesIO, Messages, PandasMessages, Plain, Topics = kafka.FilesIO, kafka.Messages, kafka.PandasMessages, kafka.Plain, kafka.Topics

pytestmark = [pytest.mark.integration, pytest.mark.floci]


@pytest.fixture(autouse=True)
def default_engine():
    EngineSetting.reset()
    yield
    EngineSetting.reset()


class Writes:
    @staticmethod
    def write(engine, config, model, frame, **options):
        EngineSetting.reset(engine)
        FilesIO(source_config=config, schema=model, flush_timeout=30, **options).write(frame)


class TestPolarsKafkaWrite:
    def test_the_broker_gets_the_bytes_of_the_pandas_engine(self, floci, topic, tmp_path):
        # Given
        frame = Topics.frame(6).drop("flag", "at", "took")
        config = Resources.kafka(tmp_path, floci, topic)

        # When
        Writes.write("pandas", config, kafka.PandasPlain, Topics.pandas_frame(frame), key_column="key")
        Writes.write("polars", config, Plain, frame, key_column="key")

        # Then
        messages = Broker.consume(floci, topic, 2 * frame.height)
        assert messages[: frame.height] == messages[frame.height :]
        assert len(messages) == 12 and messages[1][0] is None

    def test_a_nanosecond_time_reaches_the_broker_with_a_custom_serializer(self, floci, topic, tmp_path):
        # Given
        frame = Topics.frame(2)
        config = Resources.kafka(tmp_path, floci, topic)

        # When
        Writes.write("pandas", config, PandasMessages, Topics.pandas_frame(frame), key_column="key", value_serializer=Topics.repr_serializer)
        Writes.write("polars", config, Messages, frame, key_column="key", value_serializer=Topics.repr_serializer)

        # Then
        messages = Broker.consume(floci, topic, 4)
        assert messages[:2] == messages[2:]
        assert b"01:02:03.123456789" in messages[0][1]

    def test_the_chunks_reach_the_broker_in_the_order_of_the_rows(self, floci, topic, tmp_path):
        # Given
        frame = Topics.frame(10).drop("flag", "at", "took")

        # When
        with patch.object(PolarsKafka, "CHUNK_ROWS", 3):
            Writes.write("polars", Resources.kafka(tmp_path, floci, topic), Plain, frame, key_column="key")

        # Then
        assert [key for key, _ in Broker.consume(floci, topic, 10)] == [None if i == 1 else f"k{i}".encode() for i in range(10)]

    def test_a_full_queue_raises_a_timeout_error_after_flush_timeout_when_the_broker_does_not_answer(self, floci, topic, tmp_path):
        # Given a local queue of 1 message and a broker that does not answer
        EngineSetting.reset("polars")
        options = {"key_column": "key", "flush_timeout": 3, "queue.buffering.max.messages": 1, "message.timeout.ms": 600000}
        io = FilesIO(source_config=Resources.kafka(tmp_path, floci, topic), schema=Plain, **options)

        # When / Then
        with Broker.paused(floci):
            start = time.monotonic()
            with pytest.raises(TimeoutError, match="had no room for 3s"):
                io.write(Topics.frame(5).drop("flag", "at", "took"))
            assert 3 <= time.monotonic() - start < 15

    def test_a_pandas_write_with_key_column_gives_the_bytes_of_set_index_on_the_broker(self, floci, topic, tmp_path):
        # Given
        frame = pd.DataFrame({"key": ["a", "b", None], "id": ["x", "y", "z"], "foo": ["f", None, "h"], "bar": [1, 2, 3], "baz": [None, "q", "r"]})
        config = Resources.kafka(tmp_path, floci, topic)

        # When
        WriteKafkaIO(config, flush_timeout=30).write(frame.set_index("key"))
        WriteKeyedKafkaIO(config, flush_timeout=30, key_column="key").write(frame)

        # Then
        messages = Broker.consume(floci, topic, 6)
        assert messages[:3] == messages[3:]
