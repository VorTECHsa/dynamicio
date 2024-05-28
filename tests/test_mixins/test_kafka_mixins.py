# pylint: disable=no-member, missing-module-docstring, missing-class-docstring, missing-function-docstring, too-many-public-methods, too-few-public-methods, protected-access, C0103, C0302, R0801
import os
from unittest.mock import patch

import pandas as pd
import pytest
import simplejson

from dynamicio.config import IOConfig
from dynamicio.mixins import WithKafka
from tests import constants
from tests.mocking.io import MockKafkaProducer, WriteKafkaIO


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
        with patch("dynamicio.mixins.with_kafka.Producer", return_value=mock_kafka_producer_instance):
            write_kafka_io = WriteKafkaIO(kafka_cloud_config)
            for chunk in rows_generator(_df=df, chunk_size=2):
                write_kafka_io.write(chunk)

            # Then
            assert mock_kafka_producer_instance.produce_call_count == len(input_messages_df)

    @pytest.mark.unit
    def test_write_to_kafka_is_called_with_document_transformer_if_provided_for_writing_an_iterable_of_dicts_with_env_as_cloud_kafka(self, input_messages_df):
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
        with patch("dynamicio.mixins.with_kafka.Producer", return_value=mock_kafka_producer_instance):
            write_kafka_io = WriteKafkaIO(kafka_cloud_config, document_transformer=lambda v: dict(**v, worked=True))
            for chunk in rows_generator(_df=df, chunk_size=2):
                write_kafka_io.write(chunk)

            # Debug: Print the contents of my_stream to trace the issue
            print("my_stream contents:", mock_kafka_producer_instance.my_stream)

            # Then
            for i in range(len(df)):
                assert len(mock_kafka_producer_instance.my_stream) > 0, "No messages were produced"
                assert mock_kafka_producer_instance.my_stream[i]["value"] == simplejson.dumps(dict(**df.iloc[i].to_dict(), worked=True), ignore_nan=True).encode("utf-8")

    @pytest.mark.unit
    def test_kafka_producer_default_value_serialiser_is_used_unless_alternative_is_given(self, test_df):
        # Given
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")

        # Create the MockKafkaProducer instance before patching
        mock_kafka_producer_instance = MockKafkaProducer()

        write_kafka_io = WriteKafkaIO(kafka_cloud_config)

        # When
        with patch("dynamicio.mixins.with_kafka.Producer", return_value=mock_kafka_producer_instance):
            write_kafka_io.write(test_df)

        # Then (excuse me for resorting to private attributes, but it's the only way to test this)
        assert write_kafka_io._WithKafka__value_serializer == write_kafka_io._default_value_serializer  # pylint: disable=comparison-with-callable

    @pytest.mark.unit
    def test_kafka_producer_default_key_serialiser_is_used_unless_alternative_is_given(self, test_df):
        # Given
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")

        # Create the MockKafkaProducer instance before patching
        mock_kafka_producer_instance = MockKafkaProducer()

        write_kafka_io = WriteKafkaIO(kafka_cloud_config)

        # When
        with patch("dynamicio.mixins.with_kafka.Producer", return_value=mock_kafka_producer_instance):
            write_kafka_io.write(test_df)

        # Then (excuse me for resorting to private attributes, but it's the only way to test this)
        assert write_kafka_io._WithKafka__key_serializer == write_kafka_io._default_key_serializer  # pylint: disable=comparison-with-callable

    @pytest.mark.unit
    def test_kafka_producer_default_compression_type_is_snappy(self, test_df):
        # Given
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")

        # Create the MockKafkaProducer instance before patching
        mock_kafka_producer_instance = MockKafkaProducer()

        write_kafka_io = WriteKafkaIO(kafka_cloud_config)

        # When
        with patch("dynamicio.mixins.with_kafka.Producer", return_value=mock_kafka_producer_instance):
            write_kafka_io.write(test_df)

        # Then
        # Remove serializers from config for assertion as they are function references
        kafka_config = write_kafka_io._WithKafka__kafka_config.copy()
        kafka_config.pop("value_serializer", None)  # Use .pop with default value to avoid KeyError
        kafka_config.pop("key_serializer", None)  # Use .pop with default value to avoid KeyError

        # Check that default options are correctly set
        assert kafka_config == {"bootstrap.servers": "mock-kafka-server", "compression.type": "snappy"}

    @pytest.mark.unit
    def test_kafka_producer_options_are_replaced_by_the_user_options(self, test_df):
        # Given
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")

        # Create the MockKafkaProducer instance before patching
        mock_kafka_producer_instance = MockKafkaProducer()

        write_kafka_io = WriteKafkaIO(kafka_cloud_config, **{"compression.type": "lz4", "acks": 2})

        # When
        with patch("dynamicio.mixins.with_kafka.Producer", return_value=mock_kafka_producer_instance):
            write_kafka_io.write(test_df)

        # Then
        # Remove serializers from config for assertion as they are function references
        kafka_config = write_kafka_io._WithKafka__kafka_config.copy()
        kafka_config.pop("value_serializer", None)  # Use .pop with default value to avoid KeyError
        kafka_config.pop("key_serializer", None)  # Use .pop with default value to avoid KeyError

        # Check that user options are correctly set
        assert kafka_config == {
            "acks": 2,
            "bootstrap.servers": "mock-kafka-server",
            "compression.type": "lz4",
        }
        assert write_kafka_io._WithKafka__kafka_config == kafka_config

    @pytest.mark.unit
    def test_kafka_producer_options_are_replaced_by_the_user_options_from_resource_definition(self, test_df):
        # Given
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON_WITH_OPTIONS")

        # Create the MockKafkaProducer instance before patching
        mock_kafka_producer_instance = MockKafkaProducer()

        write_kafka_io = WriteKafkaIO(kafka_cloud_config)

        # When
        with patch("dynamicio.mixins.with_kafka.Producer", return_value=mock_kafka_producer_instance):
            write_kafka_io.write(test_df)

        # Then
        # Remove serializers from config for assertion as they are function references
        kafka_config = write_kafka_io._WithKafka__kafka_config.copy()
        kafka_config.pop("value_serializer", None)  # Use .pop with default value to avoid KeyError
        kafka_config.pop("key_serializer", None)  # Use .pop with default value to avoid KeyError

        # Check that user options are correctly set
        assert kafka_config == {
            "batch.size": 20000000,
            "bootstrap.servers": "mock-kafka-server",
            "compression.type": "gzip",
            "linger.ms": 3000,
            "max.in.flight.requests.per.connection": 10,
            "message.send.max.retries": 3,
            "request.timeout.ms": 60000,
            "retry.backoff.ms": 100,
        }

        assert write_kafka_io._WithKafka__kafka_config == kafka_config

    @pytest.mark.unit
    def test_producer_send_method_sends_messages_with_index_as_key_by_default_if_a_keygen_is_not_provided(self, test_df):
        # Given
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")

        # Create the MockKafkaProducer instance before patching
        mock_kafka_producer_instance = MockKafkaProducer()

        write_kafka_io = WriteKafkaIO(kafka_cloud_config)

        # When
        with patch("dynamicio.mixins.with_kafka.Producer", return_value=mock_kafka_producer_instance):
            write_kafka_io.write(test_df)

        # Then
        def sort_dict(d):
            return {k: d[k] for k in sorted(d)}

        expected_stream = [
            {"key": b"0", "value": simplejson.dumps(sort_dict({"bar": 1000, "baz": "ABC", "foo": "id_1", "id": "cm_1"}), ignore_nan=True).encode("utf-8")},
            {"key": b"1", "value": simplejson.dumps(sort_dict({"bar": 1000, "baz": "ABC", "foo": "id_2", "id": "cm_2"}), ignore_nan=True).encode("utf-8")},
            {"key": b"2", "value": simplejson.dumps(sort_dict({"bar": 1000, "baz": "ABC", "foo": "id_3", "id": "cm_3"}), ignore_nan=True).encode("utf-8")},
        ]
        actual = []
        for message in mock_kafka_producer_instance.my_stream:
            actual.append({"key": message["key"], "value": simplejson.dumps(sort_dict(simplejson.loads(message["value"])), ignore_nan=True).encode("utf-8")})

        assert actual == expected_stream

    @pytest.mark.unit
    def test_producer_send_method_can_send_keyed_messages_using_a_custom_key_generator(self, test_df):
        # Given
        kafka_cloud_config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")
        write_kafka_io = WriteKafkaIO(kafka_cloud_config, key_generator=lambda _, message: "XXX")

        # Create the MockKafkaProducer instance before patching
        mock_kafka_producer_instance = MockKafkaProducer()

        # When
        with patch("dynamicio.mixins.with_kafka.Producer", return_value=mock_kafka_producer_instance):
            write_kafka_io.write(test_df)

        # Then
        def sort_dict(d):
            return {k: d[k] for k in sorted(d)}

        expected_stream = [
            {"key": b"XXX", "value": simplejson.dumps(sort_dict({"bar": 1000, "baz": "ABC", "foo": "id_1", "id": "cm_1"}), ignore_nan=True).encode("utf-8")},
            {"key": b"XXX", "value": simplejson.dumps(sort_dict({"bar": 1000, "baz": "ABC", "foo": "id_2", "id": "cm_2"}), ignore_nan=True).encode("utf-8")},
            {"key": b"XXX", "value": simplejson.dumps(sort_dict({"bar": 1000, "baz": "ABC", "foo": "id_3", "id": "cm_3"}), ignore_nan=True).encode("utf-8")},
        ]
        actual = []
        for message in mock_kafka_producer_instance.my_stream:
            actual.append({"key": message["key"], "value": simplejson.dumps(sort_dict(simplejson.loads(message["value"])), ignore_nan=True).encode("utf-8")})

        assert actual == expected_stream

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

        # Create the MockKafkaProducer instance before patching
        mock_kafka_producer_instance = MockKafkaProducer()

        # When
        with patch("dynamicio.mixins.with_kafka.Producer", return_value=mock_kafka_producer_instance):
            write_kafka_io.write(keyed_test_df)

        # Then
        assert write_kafka_io._WithKafka__key_generator("idx", "value") == "idx"
        assert write_kafka_io._WithKafka__document_transformer({"value": "value"}) == {"value": "value"}

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

        # Create the MockKafkaProducer instance before patching
        mock_kafka_producer_instance = MockKafkaProducer()

        # When
        with patch("dynamicio.mixins.with_kafka.Producer", return_value=mock_kafka_producer_instance):
            write_kafka_io.write(keyed_test_df)

        # Then
        assert write_kafka_io._WithKafka__key_generator("idx", "value") == "xxx"
        assert write_kafka_io._WithKafka__document_transformer("value") == "xxx"

    @pytest.mark.unit
    def test_raise_exception_on_single_message_failure(self, input_messages_df):
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

        def mock_produce(*args, **kwargs):  # pylint: disable=unused-argument
            if mock_kafka_producer_instance.produce_call_count == 1:
                raise Exception("Mock message delivery failure")
            mock_kafka_producer_instance.produce_call_count += 1

        # When
        with patch("dynamicio.mixins.with_kafka.Producer", return_value=mock_kafka_producer_instance):
            with patch.object(mock_kafka_producer_instance, "produce", side_effect=mock_produce):
                write_kafka_io = WriteKafkaIO(kafka_cloud_config)
                with pytest.raises(Exception, match="Mock message delivery failure"):
                    for chunk in rows_generator(_df=df, chunk_size=2):
                        write_kafka_io.write(chunk)

                # Ensure only one message is sent successfully before the failure
                assert mock_kafka_producer_instance.produce_call_count == 1
