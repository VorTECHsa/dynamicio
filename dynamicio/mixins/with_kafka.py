# pylint: disable=no-member, protected-access, too-few-public-methods

"""This module provides mixins that are providing Kafka I/O support."""


from typing import Any, Callable, Iterable, Mapping, MutableMapping, Optional

import pandas as pd  # type: ignore
import simplejson
from kafka import KafkaProducer  # type: ignore
from magic_logger import logger


from dynamicio.config.pydantic import DataframeSchema, KafkaDataEnvironment
from dynamicio.mixins import utils


class WithKafka:
    """Handles I/O operations for Kafka.

    Args:
        - options:
            - Standard: Keyword-arguments passed to the KafkaProducer constructor (see `KafkaProducer.DEFAULT_CONFIG.keys()`).
             - Additional Options:

                - `key_generator: Callable[[Any, Mapping], T]`: defines the keying policy to be used for sending keyed-messages to Kafka. It is a `Callable` that takes a
                `tuple(idx, row)` and returns a string that will serve as the message's key, invoked prior to serialising the key. It defaults to the dataframe's index
                (which may not be composed of unique values or string type keys). It goes hand in hand with the default `key-serialiser`, which assumes that the keys
                are strings and encode's them as such.

                - `key_serializer: Callable[T, bytes]`: Custom key serialiser; if not provided, a default key-serializer will be used, applied on a string-key (unless key is None).

                N.B. Providing a custom key-generator that generates a non-string key is best provided alongside a custom key-serializer best suited to handle the custom key-type.

                - `document_transformer: Callable[[Mapping[Any, Any]`: Manipulates the messages/rows sent to Kafka as values. It is  a `Callable` taking a `Mapping` as its only
                argument and return a `Mapping`, then this callable will be invoked prior to serializing each document. This can be used, for example, to add metadata to each
                document that will be written to the target  Kafka topic.

                - `value_serializer: Callable[Mapping, bytes]`: Custom value serialiser; if not provided, a default value-serializer will be used applied on a Mapping..

    Example:
        >>> # Given
        >>> keyed_test_df = pd.DataFrame.from_records(
        >>>     [
        >>>         ["key-01", "cm_1", "id_1", 1000, "ABC"],
        >>>         ["key-02", "cm_2", "id_2", 1000, "ABC"],
        >>>         ["key-03", "cm_3", "id_3", 1000, "ABC"],
        >>>     ],
        >>>     columns=["key", "id", "foo", "bar", "baz"],
        >>> ).set_index("key")
        >>>
        >>> kafka_cloud_config = IOConfig(
        >>>     path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "processed.yaml")),
        >>>     env_identifier="CLOUD",
        >>>     dynamic_vars=constants,
        >>> ).get(source_key="WRITE_TO_KAFKA_JSON")
        >>>
        >>> write_kafka_io = WriteKafkaIO(kafka_cloud_config, key_generator=lambda key, _: key, document_transformer=lambda doc: doc["new_field"]="new_value")
        >>>
        >>> # When
        >>> with patch.object(mixins, "KafkaProducer") as mock__kafka_producer:
        >>>     mock__kafka_producer.DEFAULT_CONFIG = KafkaProducer.DEFAULT_CONFIG
        >>>     mock_producer = MockKafkaProducer()
        >>>     mock__kafka_producer.return_value = mock_producer
        >>>     write_kafka_io.write(keyed_test_df)
        >>>
        >>> # Then
        >>> assert mock_producer.my_stream == [
        >>>     {"key": "key-01", "value": {"bar": 1000, "baz": "ABC", "foo": "id_1", "id": "cm_1", "new_field": "new_value"}},
        >>>     {"key": "key-02", "value": {"bar": 1000, "baz": "ABC", "foo": "id_2", "id": "cm_2", "new_field": "new_value"}},
        >>>     {"key": "key-03", "value": {"bar": 1000, "baz": "ABC", "foo": "id_3", "id": "cm_3", "new_field": "new_value"}},
        >>> ]
    """

    sources_config: KafkaDataEnvironment
    schema: DataframeSchema
    options: MutableMapping[str, Any]
    __kafka_config: Optional[Mapping] = None
    __producer: Optional[KafkaProducer] = None
    __key_generator: Optional[Callable[[Any, Mapping[Any, Any]], Optional[str]]] = None
    __document_transformer: Optional[Callable[[Mapping[Any, Any]], Mapping[Any, Any]]] = None

    def _write_to_kafka(self, df: pd.DataFrame) -> None:
        """Given a dataframe where each row is a message to be sent to a Kafka Topic, iterate through all rows and send them to a Kafka topic.

         The topic is defined in `self.sources_config["kafka"]` and using a kafka producer, which is flushed at the
         end of this process.

        Args:
            df: A dataframe where each row is a message to be sent to a Kafka Topic.
        """
        if self.__key_generator is None:
            self.__key_generator = lambda idx, __: idx  # default key generator uses the dataframe's index
            if self.options.get("key_generator") is not None:
                self.__key_generator = self.options.pop("key_generator")

        if self.__document_transformer is None:
            self.__document_transformer = lambda value: value
            if self.options.get("document_transformer") is not None:
                self.__document_transformer = self.options.pop("document_transformer")

        if self.__producer is None:
            self.__producer = self._get_producer(self.sources_config.kafka.kafka_server, **self.options)

        self._send_messages(df=df, topic=self.sources_config.kafka.kafka_topic)

    @utils.allow_options(KafkaProducer.DEFAULT_CONFIG.keys())
    def _get_producer(self, server: str, **options: MutableMapping[str, Any]) -> KafkaProducer:
        """Generate and return a Kafka Producer.

        Default options are used to generate the producer. Specifically:
            - `bootstrap_servers`: Passed on through the source_config
            - `value_serializer`: Uses a default_value_serializer defined in this mixin

        More options can be added to the producer by passing them as keyword arguments, through valid options.

        These can also override the default options.

        Args:
            server: The host name.
            **options: Keyword arguments to pass to the KafkaProducer.

        Returns:
            A Kafka producer instance.
        """
        self.__kafka_config = {
            **{
                "bootstrap_servers": server,
                "compression_type": "snappy",
                "key_serializer": self._default_key_serializer,
                "value_serializer": self._default_value_serializer,
            },
            **options,
        }
        return KafkaProducer(**self.__kafka_config)

    def _send_messages(self, df: pd.DataFrame, topic: str) -> None:
        logger.info(f"Sending {len(df)} messages to Kafka topic:{topic}.")

        messages = df.reset_index(drop=True).to_dict("records")
        for idx, message in zip(df.index.values, messages):
            self.__producer.send(topic, key=self.__key_generator(idx, message), value=self.__document_transformer(message))  # type: ignore

        self.__producer.flush()  # type: ignore

    @staticmethod
    def _default_key_serializer(key: Optional[str]) -> Optional[bytes]:
        if key:
            return key.encode("utf-8")
        return None

    @staticmethod
    def _default_value_serializer(value: Mapping) -> bytes:
        return simplejson.dumps(value, ignore_nan=True).encode("utf-8")

    def _read_from_kafka(self) -> Iterable[Mapping]:  # type: ignore
        """Read messages from a Kafka Topic and convert them to separate dataframes.

        Returns:
            Multiple dataframes, one per message read from the Kafka topic of interest.
        """
        # TODO: Implement kafka reader
