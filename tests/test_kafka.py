# flake8: noqa: I101

from unittest.mock import MagicMock, call, patch

import pytest

from dynamicio import KafkaConfig, KafkaResource


@pytest.fixture
def mocked_kafka_producer():
    mocked_kafka_producer = MagicMock()
    with patch("dynamicio.io.kafka.KafkaProducer") as kafka_producer:
        kafka_producer.return_value = mocked_kafka_producer
        yield mocked_kafka_producer


@pytest.fixture
def kafka_resource() -> KafkaResource:
    config = KafkaConfig(topic="test_topic", server="test_server")
    return KafkaResource(config)


def test_kafka_resource_write(sample_df, kafka_resource, mocked_kafka_producer):
    kafka_resource.write(sample_df)
    mocked_kafka_producer.send.assert_has_calls(
        [
            call("test_topic", key=0, value={"a": 1, "b": 4}),
            call("test_topic", key=1, value={"a": 2, "b": 5}),
            call("test_topic", key=2, value={"a": 3, "b": 6}),
        ]
    )


def test_kafka_resource_read(kafka_resource):
    with pytest.raises(NotImplementedError):
        kafka_resource.read()
