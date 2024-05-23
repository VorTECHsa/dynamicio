# pylint: disable=no-member, missing-module-docstring, missing-class-docstring, missing-function-docstring, too-many-public-methods, too-few-public-methods, protected-access, C0103, C0302, R0801
import os
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest
from confluent_kafka import Producer

import dynamicio.mixins.with_kafka
from dynamicio.config import IOConfig
from dynamicio.mixins import WithKafka
from tests import constants
from tests.mocking.io import (
    MockKafkaProducer,
    WriteKafkaIO,
)


class TestKafkaIO:
    @pytest.mark.unit
    def test_write_to_kafka_is_called_for_writing_an_iterable_of_dicts_with_env_as_cloud_kafka(self, input_messages_df):
        # Given
        def rows_generator(_df, chunk_size):
            _chunk = []
            for _, row in _df.iterrows():
                _chunk.append(row.to_dict())
                if len(_chunk) == chunk_size:
                    yield pd.DataFrame(_chunk)
                    _chunk.clear()

        df = input_messages_df

        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")

        # Create the MockKafkaProducer instance before patching
        mock_kafka_producer_instance = MockKafkaProducer()

        # When
        with patch('dynamicio.mixins.with_kafka.Producer', return_value=mock_kafka_producer_instance):
            write_kafka_io = WriteKafkaIO(kafka_cloud_config)
            for chunk in rows_generator(_df=df, chunk_size=2):
                write_kafka_io.write(chunk)

            # Then
            assert mock_kafka_producer_instance.produce_call_count == len(input_messages_df)

    @pytest.mark.unit
    @patch.object(dynamicio.mixins.with_kafka, Producer)
    @patch.object(MockKafkaProducer, "send")
    def test_write_to_kafka_is_called_with_document_transformer_if_provided_for_writing_an_iterable_of_dicts_with_env_as_cloud_kafka(
            self, mock__kafka_producer, mock__kafka_producer_send, input_messages_df
    ):
        # Given
        def rows_generator(_df, chunk_size):
            _chunk = []
            for _, row in df.iterrows():
                _chunk.append(row.to_dict())
                if len(_chunk) == chunk_size:
                    yield pd.DataFrame(_chunk)
                    _chunk.clear()

        df = input_messages_df.iloc[[0]]

        mock__kafka_producer.return_value = MockKafkaProducer()
        mock__kafka_producer_send.return_value = MagicMock()

        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")

        # When
        for chunk in rows_generator(_df=df, chunk_size=2):
            WriteKafkaIO(kafka_cloud_config, document_transformer=lambda v: dict(**v, worked=True)).write(chunk)
            # Then
            mock__kafka_producer_send.assert_called_once_with(
                {
                    "id": "message01",
                    "foo": "xxxxxxxx",
                    "bar": 0,
                    "baz": ["a", "b", "c"],
                    "worked": True,
                }
            )

    @pytest.mark.unit
    def test_kafka_producer_default_value_serialiser_is_used_unless_alternative_is_given(self, test_df):
        # Given
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")
        write_kafka_io = WriteKafkaIO(kafka_cloud_config)

        # When
        with patch.object(dynamicio.mixins.with_kafka, Producer) as mock__kafka_producer, patch.object(MockKafkaProducer, "send") as mock__kafka_producer_send:
            mock__kafka_producer.DEFAULT_CONFIG = WithKafka.VALID_CONFIG_KEYS
            mock__kafka_producer.return_value = MockKafkaProducer()
            mock__kafka_producer_send.return_value = MagicMock()
            write_kafka_io.write(test_df)

        # Then
        value_serializer = write_kafka_io._WithKafka__kafka_config.pop("value_serializer")
        assert "WithKafka._default_value_serializer" in str(value_serializer)

    @pytest.mark.unit
    def test_kafka_producer_default_key_serialiser_is_used_unless_alternative_is_given(self, test_df):
        # Given
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")
        write_kafka_io = WriteKafkaIO(kafka_cloud_config)

        # When
        with patch.object(dynamicio.mixins.with_kafka, Producer) as mock__kafka_producer, patch.object(MockKafkaProducer, "send") as mock__kafka_producer_send:
            mock__kafka_producer.DEFAULT_CONFIG = WithKafka.VALID_CONFIG_KEYS
            mock__kafka_producer.return_value = MockKafkaProducer()
            mock__kafka_producer_send.return_value = MagicMock()
            write_kafka_io.write(test_df)

        # Then
        key_serializer = write_kafka_io._WithKafka__kafka_config.pop("key_serializer")
        assert "WithKafka._default_key_serializer" in str(key_serializer)

    @pytest.mark.unit
    @patch.object(MockKafkaProducer, "send")
    @patch.object(dynamicio.mixins.with_kafka, Producer)
    def test_kafka_producer_default_compression_type_is_snappy(self, mock__kafka_producer, mock__kafka_producer_send, test_df):
        # Given
        mock__kafka_producer.DEFAULT_CONFIG = WithKafka.VALID_CONFIG_KEYS
        mock__kafka_producer.return_value = MockKafkaProducer()
        mock__kafka_producer_send.return_value = MagicMock()
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")
        write_kafka_io = WriteKafkaIO(kafka_cloud_config)

        # When
        write_kafka_io.write(test_df)

        # Then
        write_kafka_io._WithKafka__kafka_config.pop("value_serializer")  # Removed as it returns a unique function identifier
        write_kafka_io._WithKafka__kafka_config.pop("key_serializer")  # Removed as it returns a unique function identifier
        assert write_kafka_io._WithKafka__kafka_config == {"bootstrap_servers": "mock-kafka-server", "compression_type": "snappy"}

    @pytest.mark.unit
    @patch.object(MockKafkaProducer, "send")
    @patch.object(dynamicio.mixins.with_kafka, Producer)
    def test_kafka_producer_options_are_replaced_by_the_user_options(self, mock__kafka_producer, mock__kafka_producer_send, test_df):
        # Given
        mock__kafka_producer.DEFAULT_CONFIG = WithKafka.VALID_CONFIG_KEYS
        mock__kafka_producer.return_value = MockKafkaProducer()
        mock__kafka_producer_send.return_value = MagicMock()
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")
        write_kafka_io = WriteKafkaIO(kafka_cloud_config, compression_type="lz4", acks=2)

        # When
        write_kafka_io.write(test_df)

        # Then
        value_serializer = write_kafka_io._WithKafka__kafka_config.pop("value_serializer")  # Removed as it returns a unique function identifier
        write_kafka_io._WithKafka__kafka_config.pop("key_serializer")  # Removed as it returns a unique function identifier
        assert write_kafka_io._WithKafka__kafka_config == {
            "acks": 2,
            "bootstrap_servers": "mock-kafka-server",
            "compression_type": "lz4",
        } and "WithKafka._default_value_serializer" in str(value_serializer)

    @pytest.mark.unit
    def test_producer_send_method_sends_messages_with_index_as_key_by_default_if_a_keygen_is_not_provided(self, test_df):
        # Given
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")
        write_kafka_io = WriteKafkaIO(kafka_cloud_config)

        # When
        with patch.object(dynamicio.mixins.with_kafka, Producer) as mock__kafka_producer:
            mock__kafka_producer.DEFAULT_CONFIG = WithKafka.VALID_CONFIG_KEYS
            mock_producer = MockKafkaProducer()
            mock__kafka_producer.return_value = mock_producer
            write_kafka_io.write(test_df)

        # Then
        assert mock_producer.my_stream == [
            {"key": 0, "value": {"bar": 1000, "baz": "ABC", "foo": "id_1", "id": "cm_1"}},
            {"key": 1, "value": {"bar": 1000, "baz": "ABC", "foo": "id_2", "id": "cm_2"}},
            {"key": 2, "value": {"bar": 1000, "baz": "ABC", "foo": "id_3", "id": "cm_3"}},
        ]

    @pytest.mark.unit
    def test_producer_send_method_can_send_keyed_messages_using_a_custom_key_generator(self, test_df):
        # Given
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")
        write_kafka_io = WriteKafkaIO(kafka_cloud_config, key_generator=lambda _, message: "XXX")

        # When
        with patch.object(dynamicio.mixins.with_kafka, Producer) as mock__kafka_producer:
            mock__kafka_producer.DEFAULT_CONFIG = WithKafka.VALID_CONFIG_KEYS
            mock_producer = MockKafkaProducer()
            mock__kafka_producer.return_value = mock_producer
            write_kafka_io.write(test_df)

        # Then
        assert mock_producer.my_stream == [
            {"key": "XXX", "value": {"bar": 1000, "baz": "ABC", "foo": "id_1", "id": "cm_1"}},
            {"key": "XXX", "value": {"bar": 1000, "baz": "ABC", "foo": "id_2", "id": "cm_2"}},
            {"key": "XXX", "value": {"bar": 1000, "baz": "ABC", "foo": "id_3", "id": "cm_3"}},
        ]

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "key, encoded_key",
        [
            (None, None),
            ("cacik", b"cacik"),
        ],
    )
    def test_default_key_serialiser_returns_none_if_key_is_not_provided_and_an_encoded_string_otherwise(self, key, encoded_key):
        # Given/When/Then
        assert encoded_key == WithKafka._default_key_serializer(key)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "value, encoded_value",
        [
            (None, b"null"),
            ({"a": 1, "b": "cacik"}, b'{"a": 1, "b": "cacik"}'),
            ({"a": 1, "b": None}, b'{"a": 1, "b": null}'),
        ],
    )
    def test_default_value_serialiser_returns_encoded_mapping_if_key_is_not_provided_and_an_encoded_string_otherwise(self, value, encoded_value):
        # Given/When/Then
        assert encoded_value == WithKafka._default_value_serializer(value)

    @pytest.mark.unit
    def test_default_key_generator_and_transformer_are_used_if_none_are_provided_by_the_user(self):
        # Given
        keyed_test_df = pd.DataFrame.from_records(
            [
                ["key-01", "cm_1", "id_1", 1000, "ABC"],
                ["key-01", "cm_2", "id_2", 1000, "ABC"],  # <-- index is non-unique
                ["key-02", "cm_3", "id_3", 1000, "ABC"],
            ],
            columns=["key", "id", "foo", "bar", "baz"],
        ).set_index("key")
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")
        write_kafka_io = WriteKafkaIO(kafka_cloud_config)

        # When
        with patch.object(dynamicio.mixins.with_kafka, Producer) as mock__kafka_producer:
            mock__kafka_producer.DEFAULT_CONFIG = WithKafka.VALID_CONFIG_KEYS
            mock_producer = MockKafkaProducer()
            mock__kafka_producer.return_value = mock_producer

            # When
            write_kafka_io.write(keyed_test_df)
            assert (write_kafka_io._WithKafka__key_generator("idx", "value") == "idx") and (write_kafka_io._WithKafka__document_transformer("value") == "value")

    @pytest.mark.unit
    def test_custom_key_generator_and_transformer_are_used_if_they_are_provided_by_the_user(self):
        # Given
        keyed_test_df = pd.DataFrame.from_records(
            [
                ["key-01", "cm_1", "id_1", 1000, "ABC"],
                ["key-01", "cm_2", "id_2", 1000, "ABC"],
                ["key-02", "cm_3", "id_3", 1000, "ABC"],
            ],
            columns=["key", "id", "foo", "bar", "baz"],
        ).set_index("key")
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")
        write_kafka_io = WriteKafkaIO(kafka_cloud_config, key_generator=lambda idx, _: "xxx", document_transformer=lambda _: "xxx")

        # When
        with patch.object(dynamicio.mixins.with_kafka, Producer) as mock__kafka_producer:
            mock__kafka_producer.DEFAULT_CONFIG = WithKafka.VALID_CONFIG_KEYS
            mock_producer = MockKafkaProducer()
            mock__kafka_producer.return_value = mock_producer

            # When
            write_kafka_io.write(keyed_test_df)
            assert (write_kafka_io._WithKafka__key_generator("idx", "value") == "xxx") and (write_kafka_io._WithKafka__document_transformer("value") == "xxx")
