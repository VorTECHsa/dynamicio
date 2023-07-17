# flake8: noqa: I101

from unittest.mock import MagicMock, call, patch

import pytest

from dynamicio import KafkaResource
from dynamicio.inject import InjectionError


@pytest.fixture
def mocked_kafka_producer():
    mocked_kafka_producer = MagicMock()
    with patch("dynamicio.io.kafka.KafkaProducer") as kafka_producer:
        kafka_producer.return_value = mocked_kafka_producer
        yield mocked_kafka_producer


@pytest.fixture
def kafka_resource() -> KafkaResource:
    return KafkaResource(topic="test_topic", server="test_server")


def test_kafka_resource_write(test_df, kafka_resource, mocked_kafka_producer):
    kafka_resource.write(test_df)
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


def test_kafka_inject_success(kafka_resource, passing_injections, test_df, mocked_kafka_producer):
    kafka_resource.topic = "{var1}"
    kafka_resource.server = "{var2}"
    kafka_resource = kafka_resource.inject(**passing_injections)
    assert kafka_resource.topic == passing_injections["var1"]
    assert kafka_resource.server == passing_injections["var2"]
    kafka_resource.write(test_df)
    mocked_kafka_producer.send.assert_has_calls(
        [
            call(passing_injections["var1"], key=0, value={"a": 1, "b": 4}),
            call(passing_injections["var1"], key=1, value={"a": 2, "b": 5}),
            call(passing_injections["var1"], key=2, value={"a": 3, "b": 6}),
        ]
    )


def test_kafka_inject_fail(kafka_resource, failing_injections, test_df, mocked_kafka_producer):
    kafka_resource.topic = "{var1}"
    kafka_resource.server = "{var2}"
    kafka_resource = kafka_resource.inject(**failing_injections)
    with pytest.raises(InjectionError):
        kafka_resource.write(test_df)
