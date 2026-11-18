# pylint: disable=no-member, missing-module-docstring, missing-class-docstring, missing-function-docstring, too-many-public-methods, too-few-public-methods, protected-access, C0103, C0302, R0801, broad-exception-raised
import itertools
import os
from unittest.mock import patch

import pandas as pd
import pytest
import simplejson

# Application Imports
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
            write_kafka_io = WriteKafkaIO(kafka_cloud_config, document_transformer=lambda v: {"worked": True, **v})
            for chunk in rows_generator(_df=df, chunk_size=2):
                write_kafka_io.write(chunk)

            # Debug: Print the contents of my_stream to trace the issue
            print("my_stream contents:", mock_kafka_producer_instance.my_stream)

            # Then
            for i in range(len(df)):
                assert len(mock_kafka_producer_instance.my_stream) > 0, "No messages were produced"
                assert mock_kafka_producer_instance.my_stream[i]["value"] == simplejson.dumps({"worked": True, **df.iloc[i].to_dict()}, ignore_nan=True).encode("utf-8")

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
                raise RuntimeError("Mock message delivery failure")

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


class TestKafkaDelivery:
    """How a write hands its messages to the producer: queue full, delivery callbacks, flush, and the options it keeps."""

    class Producer(MockKafkaProducer):
        """A producer whose local queue is full for the first `full` sends, holds `queued` messages, has `undelivered` messages left after a flush
        (one count for each flush when it is a list), and on each poll delivers up to `delivered` queued messages and serves `errors` error events."""

        def __init__(self, full: int = 0, undelivered: int = 0, queued: int = 1, delivered: int = 0, errors: int = 0, **kwargs):
            super().__init__(**kwargs)
            self.full, self.queued, self.delivered, self.errors = full, queued, delivered, errors
            self.undelivered = iter(undelivered) if isinstance(undelivered, list) else itertools.repeat(undelivered)
            self.polls, self.flushes = [], []

        def __len__(self):
            return self.queued

        def produce(self, topic, key, value, on_delivery):
            if self.full:
                self.full -= 1
                raise BufferError("Local: Queue full")
            super().produce(topic, key, value, on_delivery)

        def poll(self, timeout=None):
            self.polls.append(timeout)
            delivered = min(self.delivered, self.queued)
            self.queued -= delivered
            return delivered + self.errors

        def flush(self, *args):
            self.flushes.append(args)
            return next(self.undelivered)

    @staticmethod
    def write(producer, df, **options):
        config = IOConfig(
            path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml")),
            env_identifier="CLOUD",
            dynamic_vars=constants,
        ).get(source_key="WRITE_TO_KAFKA_JSON")
        io_obj = WriteKafkaIO(config, **options)
        with patch("dynamicio.mixins.with_kafka.Producer", return_value=producer) as producer_class:
            io_obj.write(df)
        return io_obj, producer_class

    @pytest.mark.unit
    def test_a_full_queue_is_polled_and_the_message_is_sent_again(self, test_df):
        # Given
        producer = self.Producer(full=2)

        # When
        self.write(producer, test_df)

        # Then
        assert [message["key"] for message in producer.my_stream] == [b"0", b"1", b"2"]
        assert producer.polls.count(1) == 2

    @pytest.mark.unit
    def test_a_message_that_does_not_fit_in_an_empty_queue_raises(self, test_df):
        # Given a queue that is empty and still refuses the message
        producer = self.Producer(full=10**6, queued=0)

        # When / Then
        with pytest.raises(BufferError, match="queue.buffering.max"):
            self.write(producer, test_df)

        assert not producer.polls

    @pytest.mark.unit
    def test_each_send_serves_the_delivery_callbacks(self, test_df):
        # Given
        producer = self.Producer()

        # When
        self.write(producer, test_df)

        # Then
        assert producer.polls == [0, 0, 0]

    @pytest.mark.unit
    def test_the_flush_waits_without_a_timeout_by_default(self, test_df):
        # Given
        producer = self.Producer()

        # When
        self.write(producer, test_df)

        # Then
        assert producer.flushes == [()]

    @pytest.mark.unit
    def test_a_flush_that_delivers_nothing_for_flush_timeout_seconds_raises_a_timeout_error(self, test_df):
        # Given 2 queued messages that the flush does not deliver, and a clock that moves 10 s on each reading
        producer = self.Producer(queued=2, undelivered=2)

        # When / Then
        with patch("dynamicio.mixins.with_kafka.time.monotonic", side_effect=itertools.count(0, 10)), pytest.raises(TimeoutError, match="2 messages"):
            self.write(producer, test_df, flush_timeout=30)
        assert producer.flushes == [(1,)] * 3

    @pytest.mark.unit
    def test_a_slow_flush_that_keeps_delivering_does_not_raise_a_timeout_error(self, test_df):
        # Given 5 queued messages that the flush delivers one at a time, and a clock that moves 10 s on each reading
        producer = self.Producer(queued=5, undelivered=[4, 3, 2, 1, 0])

        # When
        with patch("dynamicio.mixins.with_kafka.time.monotonic", side_effect=itertools.count(0, 10)):
            self.write(producer, test_df, flush_timeout=30)

        # Then
        assert producer.flushes == [(1,)] * 5

    @pytest.mark.unit
    @pytest.mark.parametrize("flush_timeout", [-1, -0.5, float("nan"), "30", True])
    def test_a_flush_timeout_that_is_not_a_number_of_0_or_more_raises_before_the_first_message(self, test_df, flush_timeout):
        # Given
        producer = self.Producer()

        # When / Then
        with pytest.raises(ValueError, match="flush_timeout"):
            self.write(producer, test_df, flush_timeout=flush_timeout)
        assert not producer.my_stream and not producer.flushes

    @pytest.mark.unit
    def test_a_flush_timeout_of_zero_flushes_once_without_waiting(self, test_df):
        # Given
        producer = self.Producer(queued=2, undelivered=2)

        # When / Then
        with pytest.raises(TimeoutError, match="flush_timeout=0s"):
            self.write(producer, test_df, flush_timeout=0)
        assert producer.flushes == [(0,)]

    @pytest.mark.unit
    def test_a_queue_with_no_room_for_flush_timeout_seconds_raises_a_timeout_error(self, test_df):
        # Given a queue that stays full, and a clock that moves 10 s on each reading
        producer = self.Producer(full=10**6)

        # When / Then
        with patch("dynamicio.mixins.with_kafka.time.monotonic", side_effect=itertools.count(0, 10)), pytest.raises(TimeoutError, match="no room for 30s"):
            self.write(producer, test_df, flush_timeout=30)
        assert producer.polls == [1, 1]

    @pytest.mark.unit
    def test_a_full_queue_that_keeps_delivering_does_not_raise_a_timeout_error(self, test_df):
        # Given a queue that is full for 5 sends but delivers a message on each poll, and a clock that moves 10 s on each reading
        producer = self.Producer(full=5, queued=6, delivered=1)

        # When
        with patch("dynamicio.mixins.with_kafka.time.monotonic", side_effect=itertools.count(0, 10)):
            self.write(producer, test_df, flush_timeout=30)

        # Then
        assert [message["key"] for message in producer.my_stream] == [b"0", b"1", b"2"]

    @pytest.mark.unit
    def test_a_full_queue_that_only_serves_error_events_raises_a_timeout_error(self, test_df):
        # Given a full queue that delivers nothing and serves an error event on each poll, as with a broker that refuses connections
        producer = self.Producer(full=50, errors=1)

        # When / Then
        with patch("dynamicio.mixins.with_kafka.time.monotonic", side_effect=itertools.count(0, 10)), pytest.raises(TimeoutError, match="no room for 30s"):
            self.write(producer, test_df, flush_timeout=30)
        assert not producer.my_stream

    @pytest.mark.unit
    def test_a_full_queue_waits_no_longer_than_a_flush_timeout_below_1s(self, test_df):
        # Given a queue that is full for 1 send and delivers on each poll
        producer = self.Producer(full=1, queued=2, delivered=1)

        # When
        self.write(producer, test_df, flush_timeout=0.25)

        # Then
        assert producer.polls[0] == 0.25

    @pytest.mark.unit
    def test_a_falsy_callable_option_is_kept(self, test_df):
        # Given a serializer that is falsy, as a wrapper over an empty registry can be
        class FalsySerializer:
            def __bool__(self):
                return False

            def __call__(self, value):
                return b"falsy"

        producer = self.Producer()

        # When
        self.write(producer, test_df, value_serializer=FalsySerializer())

        # Then
        assert [message["value"] for message in producer.my_stream] == [b"falsy"] * 3

    @pytest.mark.unit
    def test_the_write_keeps_the_dynamicio_options_and_does_not_give_them_to_the_producer(self, test_df):
        # Given
        options = {"key_generator": lambda idx, _: f"k{idx}", "document_transformer": lambda doc: doc, "flush_timeout": 30}

        # When
        io_obj, producer_class = self.write(self.Producer(), test_df, **options)

        # Then
        assert {name: io_obj.options[name] for name in options} == options
        assert not set(options) & producer_class.call_args.kwargs.keys()
