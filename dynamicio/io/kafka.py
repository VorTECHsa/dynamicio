"""I/O functions and Resource class for kafka targeted operations."""
from __future__ import annotations

from copy import deepcopy
from enum import Enum
from typing import Any, Callable, Dict, Mapping, Optional, Type

import pandas as pd  # type: ignore
import simplejson
from kafka import KafkaProducer  # type: ignore
from magic_logger import logger
from pandera import SchemaModel
from pydantic import BaseModel, Field

from dynamicio.inject import check_injections, inject


class CompressionType(str, Enum):
    """Compression types for Kafka."""

    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"


class KafkaResource(BaseModel):
    """Kafka Resource.

    This class is used to write to Kafka topics. Reading is not yet supported.
    Only requires a `topic` and `server` to be initialized.
    """

    topic: str
    server: str
    key_generator: Callable[[Any, Mapping[Any, Any]], Optional[str]] = Field(
        lambda idx, _: idx,
        description="""Gets called with dataframe's (idx, row). Defaults to `idx`.""",
    )
    key_serializer: Callable[[Any], bytes] = lambda key: key.encode("utf-8") if key else None
    value_serializer: Callable[[Mapping], bytes] = lambda val: simplejson.dumps(val, ignore_nan=True).encode("utf-8")
    document_transformer: Callable[[Mapping[Any, Any]], Mapping[Any, Any]] = lambda value: value
    # TODO: Give descriptions to all these callables that describe what they're being called with

    kafka_producer: Optional[KafkaProducer] = None

    compression_type: CompressionType = "snappy"  # type: ignore
    producer_kwargs: Dict[str, Any] = {}
    pa_schema: Optional[Type[SchemaModel]] = None

    def inject(self, **kwargs) -> "KafkaResource":
        """Inject variables into topic and server. Immutable."""
        clone = deepcopy(self)
        clone.topic = inject(clone.topic, **kwargs)
        clone.server = inject(clone.server, **kwargs)
        return clone

    def check_injections(self) -> None:
        """Check that all injections have been completed."""
        check_injections(self.topic)
        check_injections(self.server)

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

    class Config:
        """Pydantic Config class."""

        arbitrary_types_allowed = True
        validate_assignment = True

    def write(self, df: pd.DataFrame) -> None:
        """Handles Write operations for Kafka."""
        kafka_producer = self.get_kafka_producer()

        logger.info(f"Sending {len(df)} messages to Kafka topic:{self.topic}")

        messages = df.reset_index(drop=True).to_dict("records")

        for idx, message in zip(df.index.values, messages):
            kafka_producer.send(
                self.topic,
                key=self.key_generator(idx, message),
                value=self.document_transformer(message),
            )  # type: ignore

        kafka_producer.flush()  # type: ignore

    def read(self) -> pd.DataFrame:
        """Not yet implemented."""
        raise NotImplementedError("No kafka reader implemented")
