# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import subprocess
import time
import uuid
from contextlib import contextmanager
from unittest.mock import patch

import pandas as pd
import pytest
from confluent_kafka import Consumer

# Application Imports
from tests.integration.resources import Resources
from tests.mocking.io import MockKafkaProducer, WriteKafkaIO

pytestmark = [pytest.mark.integration, pytest.mark.floci]


class Broker:
    @staticmethod
    def consume(floci, topic, count, timeout=30):
        consumer = Consumer({"bootstrap.servers": floci.kafka_bootstrap, "group.id": f"it-{uuid.uuid4().hex[:8]}", "auto.offset.reset": "earliest"})
        consumer.subscribe([topic])
        messages, deadline = [], time.monotonic() + timeout
        try:
            while len(messages) < count and time.monotonic() < deadline:
                message = consumer.poll(1)
                if message is not None and not message.error():
                    messages.append((message.key(), message.value()))
        finally:
            consumer.close()
        return messages

    @staticmethod
    @contextmanager
    def paused(floci):
        subprocess.run(["docker", "pause", floci.kafka_container], check=True, capture_output=True)
        try:
            yield
        finally:
            subprocess.run(["docker", "unpause", floci.kafka_container], check=True, capture_output=True)


class TestKafkaWrite:
    DF = pd.DataFrame({"id": ["a", "b", "c"], "foo": ["x", None, "z"], "bar": [1, 2, 3], "baz": [None, "q", "r"]})

    def test_the_broker_gets_the_same_bytes_as_the_mock_producer_of_the_unit_tests(self, floci, topic, tmp_path):
        # Given
        config = Resources.kafka(tmp_path, floci, topic)
        mock = MockKafkaProducer()
        with patch("dynamicio.mixins.with_kafka.Producer", return_value=mock):
            WriteKafkaIO(config).write(self.DF)

        # When
        WriteKafkaIO(config, flush_timeout=30).write(self.DF)

        # Then
        assert Broker.consume(floci, topic, len(self.DF)) == [(message["key"], message["value"]) for message in mock.my_stream]

    def test_a_full_queue_raises_a_timeout_error_after_flush_timeout_when_the_broker_does_not_answer(self, floci, topic, tmp_path):
        # Given a local queue of 1 message and a broker that does not answer
        config = Resources.kafka(tmp_path, floci, topic)
        io_obj = WriteKafkaIO(config, flush_timeout=3, **{"queue.buffering.max.messages": 1, "message.timeout.ms": 600000})

        # When / Then
        with Broker.paused(floci):
            start = time.monotonic()
            with pytest.raises(TimeoutError, match="had no room for 3s"):
                io_obj.write(pd.concat([self.DF] * 5, ignore_index=True))
            assert 3 <= time.monotonic() - start < 15
