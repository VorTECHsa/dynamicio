"""I/O functions and Resource class for kafka targeted operations."""

from enum import Enum
from typing import Any, Callable, Dict, Mapping, Optional

import pandas as pd  # type: ignore
import simplejson
from kafka import KafkaProducer  # type: ignore
from magic_logger import logger
from pydantic import Field

from dynamicio.base import BaseResource


class CompressionType(str, Enum):
    """Compression types for Kafka."""

    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"


class KafkaResource(BaseResource):
    """Kafka Resource class.

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

    def _resource_write(self, df: pd.DataFrame) -> None:
        """Handles Write operations for Kafka."""
        if self.kafka_producer is None:
            kafka_producer = KafkaProducer(
                bootstrap_servers=self.server,
                compression_type=self.compression_type,
                key_serializer=self.key_serializer,
                value_serializer=self.value_serializer,
                **self.producer_kwargs,
            )
        else:
            kafka_producer = self.kafka_producer

        logger.info(f"Sending {len(df)} messages to Kafka topic:{self.topic}")

        messages = df.reset_index(drop=True).to_dict("records")

        for idx, message in zip(df.index.values, messages):
            kafka_producer.send(
                self.topic, key=self.key_generator(idx, message), value=self.document_transformer(message)
            )  # type: ignore

        kafka_producer.flush()  # type: ignore

    def _resource_read(self) -> pd.DataFrame:
        """TODO: ...."""
        raise NotImplementedError("No kafka reader implemented")
