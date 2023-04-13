# flake8: noqa: I101

from unittest.mock import MagicMock, call, patch

import pytest

from dynamicio.handlers.kafka import KafkaConfig, KafkaHandler


@pytest.fixture
def mocked_kafka_producer():
    mocked_kafka_producer = MagicMock()
    with patch("dynamicio.handlers.kafka.KafkaProducer") as kafka_producer:
        kafka_producer.return_value = mocked_kafka_producer
        yield mocked_kafka_producer


@pytest.fixture
def kafka_handler() -> KafkaHandler:
    config = KafkaConfig(topic="test_topic", server="test_server")
    return KafkaHandler(config)


def test_kafka_resource_write(sample_df, kafka_handler, mocked_kafka_producer):
    kafka_handler.write(sample_df)
    mocked_kafka_producer.send.assert_has_calls(
        [
            call("test_topic", key=0, value={"a": 1, "b": 4}),
            call("test_topic", key=1, value={"a": 2, "b": 5}),
            call("test_topic", key=2, value={"a": 3, "b": 6}),
        ]
    )


def test_kafka_resource_read(kafka_handler):
    with pytest.raises(NotImplementedError):
        kafka_handler.read()
