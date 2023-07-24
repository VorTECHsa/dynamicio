"""I/O functions and Resource class for kafka targeted operations."""
from __future__ import annotations

import logging
from copy import deepcopy
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Type

import pandas as pd  # type: ignore
import simplejson
from kafka import KafkaProducer  # type: ignore
from pandera import SchemaModel
from pydantic import BaseModel, Field
from uhura import Writable

from dynamicio.inject import check_injections, inject
from dynamicio.serde import JsonSerde, ParquetSerde

logger = logging.getLogger(__name__)


class CompressionType(str, Enum):
    """Compression types for Kafka."""

    GZIP = "gzip"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"


class KafkaConfig(BaseModel):
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

    class Config:
        """Pydantic Config class."""

        arbitrary_types_allowed = True
        validate_assignment = True


class KafkaResource(KafkaConfig):
    """Kafka Resource.

    This class is used to write to Kafka topics. Reading is not yet supported.
    Only requires a `topic` and `server` to be initialized.
    """

    def inject(self, **kwargs) -> "KafkaResource":
        """Inject variables into topic and server. Immutable."""
        clone = deepcopy(self)
        clone.topic = str(clone.topic).format(**kwargs)
        clone.server = str(clone.server).format(**kwargs)
        if clone.test_path is not None:
            clone.test_path = str(clone.test_path).format(**kwargs)
        return clone

    def write(self, df: pd.DataFrame) -> None:
        """Write the dataframe to Kafka."""
        df = self.validate(df)
        KafkaWriter(fixture_path=self.fixture_path, **self.dict()).write(df)

    def read(self) -> pd.DataFrame:
        """Read from Kafka."""
        raise NotImplementedError("Reading from Kafka is not yet supported.")

    @property
    def fixture_path(self) -> Path:
        """Return the path to the fixture file."""
        return self.test_path or Path(f"kafka/{self.topic}")


class KafkaWriter(KafkaConfig, Writable[pd.DataFrame]):
    fixture_path: Path

    def write(self, df: pd.DataFrame) -> None:
        """Handles Write operations for Kafka."""
        df = self.validate(df)

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

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        if schema := self.pa_schema:
            df = schema.validate(df)  # type: ignore
        return df

    def cache_key(self):
        return self.fixture_path

    def get_serde(self):
        return JsonSerde()
