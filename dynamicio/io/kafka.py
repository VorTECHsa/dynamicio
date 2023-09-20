"""I/O functions and Resource class for kafka targeted operations."""
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Mapping, Optional, Type, Literal

import pandas as pd  # type: ignore
import simplejson
from kafka import KafkaProducer  # type: ignore
from pandera import SchemaModel
from pydantic import Field

from dynamicio.io.resource import BaseResource
from dynamicio.io.serde import BaseSerde, JsonSerde


class KafkaResource(BaseResource):
    # Required
    topic: str
    server: str

    # Defaults
    key_generator: Callable[[Any, Mapping[Any, Any]], Optional[str]] = Field(
        lambda idx, _: idx,
        description="""Gets called with dataframe's (idx, row). Defaults to `idx`.""",
    )
    key_serializer: Callable[[Any], bytes] = lambda key: key.encode("utf-8") if key else None
    value_serializer: Callable[[Mapping], bytes] = lambda val: simplejson.dumps(val, ignore_nan=True).encode("utf-8")
    document_transformer: Callable[[Mapping[Any, Any]], Mapping[Any, Any]] = lambda value: value
    # TODO: Give descriptions to all these callables that describe what they're being called with

    # Options
    kafka_producer: Optional[KafkaProducer] = None  # gets instantiated in get_kafka_producer
    compression_type: Literal["gzip", "snappy", "lz4", "zstd"] = "snappy"  # type: ignore
    producer_kwargs: Dict[str, Any] = {}

    # Resource
    injectables: List[str] = ["topic", "server"]
    pa_schema: Optional[Type[SchemaModel]] = None
    test_path: Optional[str] = None

    def get_kafka_producer(self) -> KafkaProducer:
        """Get a KafkaProducer instance."""
        if self.kafka_producer is None:
            return KafkaProducer(
                bootstrap_servers=self.server,
                compression_type=self.compression_type,
                key_serializer=self.key_serializer,
                value_serializer=self.value_serializer,
                **self.producer_kwargs,
            )
        return self.kafka_producer

    def _write(self, df: pd.DataFrame) -> None:
        """Handles Write operations for Kafka."""
        kafka_producer = self.get_kafka_producer()

        logging.info(f"Sending {len(df)} messages to Kafka topic:{self.topic}")

        messages = df.reset_index(drop=True).to_dict("records")

        for idx, message in zip(df.index.values, messages):
            kafka_producer.send(
                self.topic,
                key=self.key_generator(idx, message),
                value=self.document_transformer(message),
            )  # type: ignore

        kafka_producer.flush()  # type: ignore

    def _read(self) -> pd.DataFrame:
        raise NotImplementedError

    def cache_key(self):
        """Return the path to the fixture file."""
        if self.test_path is not None:
            return Path(self.test_path)
        return Path(f"kafka/{self.topic}.json")  # Should server be added here?

    @property
    def serde_class(self) -> Type[BaseSerde]:
        return JsonSerde

    class Config:
        """Pydantic Config class."""

        arbitrary_types_allowed = True
        validate_assignment = True
