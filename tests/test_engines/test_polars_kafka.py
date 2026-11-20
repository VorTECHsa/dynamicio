# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
from datetime import datetime, timedelta
from typing import Annotated
from unittest.mock import patch

import pandas as pd
import pandera.pandas as pa
import pytest
import simplejson
from pandera.typing import Series

# Application Imports
from dynamicio.engines import EngineSetting
from dynamicio.errors import EngineResourceError
from dynamicio.schema import Field
from tests.integration.resources import Resources
from tests.mocking.io import MockKafkaProducer, WriteKafkaIO, WriteKeyedKafkaIO

pl = pytest.importorskip("polars")
DataModel = pytest.importorskip("dynamicio.schema.polars").DataModel
PolarsKafka = pytest.importorskip("dynamicio.engines.polars_kafka").PolarsKafka
FilesIO = pytest.importorskip("tests.test_engines.test_polars_files").FilesIO


class Messages(DataModel, dataset="messages"):
    key: str = Field(nullable=True)
    id: int
    price: float = Field(nullable=True)
    name: str = Field(nullable=True)
    flag: bool
    n: int = Field(nullable=True, pandas_dtype="Int64")
    at: Annotated[pl.Datetime, "ns", None] = Field(nullable=True)
    took: Annotated[pl.Duration, "ns"] = Field(nullable=True)


class PandasMessages(pa.DataFrameModel):
    __dataset_name__ = "messages"

    key: Series[str] = Field(nullable=True)
    id: Series[int]
    price: Series[float] = Field(nullable=True)
    name: Series[str] = Field(nullable=True)
    flag: Series[bool]
    n: Series[pd.Int64Dtype] = Field(nullable=True)
    at: Series[pd.Timestamp] = Field(nullable=True)
    took: Series[pd.Timedelta] = Field(nullable=True)


class Plain(DataModel, dataset="plain"):
    key: str = Field(nullable=True)
    id: int
    price: float = Field(nullable=True)
    name: str = Field(nullable=True)
    n: int = Field(nullable=True, pandas_dtype="Int64")


class PandasPlain(pa.DataFrameModel):
    __dataset_name__ = "plain"

    key: Series[str] = Field(nullable=True)
    id: Series[int]
    price: Series[float] = Field(nullable=True)
    name: Series[str] = Field(nullable=True)
    n: Series[pd.Int64Dtype] = Field(nullable=True)


@pytest.fixture(autouse=True)
def default_engine():
    EngineSetting.reset()
    yield
    EngineSetting.reset()


class Stack:
    kafka_bootstrap = "localhost:9092"


class Topics:
    """The resources, the frames and the mock producer of the Kafka tests."""

    @staticmethod
    def config(tmp_path):
        return Resources.kafka(tmp_path, Stack, "topic")

    @staticmethod
    def frame(rows: int = 4) -> "pl.DataFrame":
        return pl.DataFrame(
            {
                "key": [None if i == 1 else f"k{i}" for i in range(rows)],
                "id": list(range(rows)),
                "price": [None if i % 2 else i + 0.25 for i in range(rows)],
                "name": [None if i % 3 == 2 else f"n{i}" for i in range(rows)],
                "flag": [i % 2 == 0 for i in range(rows)],
                "n": [None if i % 2 == 0 else i * 10**15 + 1 for i in range(rows)],
                "at": pl.Series([None if i == 3 else datetime(2024, 1, 1, 1, 2, 3) for i in range(rows)], dtype=pl.Datetime("ns")),
                "took": pl.Series([None if i == 2 else timedelta(seconds=i) for i in range(rows)], dtype=pl.Duration("ns")),
            }
        ).with_columns(pl.col("at") + pl.duration(nanoseconds=123456789), pl.col("took") + pl.duration(nanoseconds=7))

    @staticmethod
    def pandas_frame(frame: "pl.DataFrame") -> pd.DataFrame:
        dtypes = {"n": "Int64", "at": "datetime64[ns]", "took": "timedelta64[ns]"}
        return frame.to_pandas().astype({column: dtype for column, dtype in dtypes.items() if column in frame.columns})

    @staticmethod
    def send(engine: str, config, model, frame, **options):
        """The messages that a write sends to the mock producer, and the producer."""
        EngineSetting.reset(engine)
        producer = MockKafkaProducer()
        with patch("dynamicio.mixins.with_kafka.Producer", return_value=producer):
            FilesIO(source_config=config, schema=model, **options).write(frame)
        return [(message["key"], message["value"]) for message in producer.my_stream]

    @staticmethod
    def repr_serializer(value) -> bytes:
        return simplejson.dumps(value, ignore_nan=True, default=repr, sort_keys=True).encode("utf-8")


class TestCheck:
    @pytest.mark.unit
    def test_a_polars_kafka_resource_without_key_column_raises_engine_resource_error_when_it_is_built(self, tmp_path):
        # Given
        EngineSetting.reset("polars")

        # When / Then
        with pytest.raises(EngineResourceError, match="needs the option 'key_column'"):
            FilesIO(source_config=Topics.config(tmp_path), schema=Plain)

    @pytest.mark.unit
    @pytest.mark.parametrize("key_column", ["missing", 1, ["key"]])
    def test_a_key_column_that_is_not_a_column_of_the_model_raises_engine_resource_error(self, tmp_path, key_column):
        # Given
        EngineSetting.reset("polars")

        # When / Then
        with pytest.raises(EngineResourceError, match="is not a column of its model"):
            FilesIO(source_config=Topics.config(tmp_path), schema=Plain, key_column=key_column)

    @pytest.mark.unit
    def test_a_pandas_kafka_resource_without_key_column_is_built(self, tmp_path):
        # When
        io = FilesIO(source_config=Topics.config(tmp_path), schema=PandasPlain)

        # Then
        assert "key_column" not in io.options


class TestKeyColumn:
    @pytest.mark.unit
    def test_the_polars_messages_are_the_bytes_of_the_pandas_engine_with_the_default_serializers(self, tmp_path):
        # Given
        frame = Topics.frame().drop("flag", "at", "took")

        # When
        polars = Topics.send("polars", Topics.config(tmp_path), Plain, frame, key_column="key")
        pandas = Topics.send("pandas", Topics.config(tmp_path), PandasPlain, Topics.pandas_frame(frame), key_column="key")

        # Then
        assert polars == pandas
        assert polars[0] == (b"k0", b'{"id": 0, "price": 0.25, "name": "n0", "n": null}')
        assert polars[1][0] is None

    @pytest.mark.unit
    def test_the_pandas_key_column_gives_the_bytes_of_set_index(self, tmp_path):
        # Given
        frame = pd.DataFrame({"key": ["a", "b", None], "id": ["x", "y", "z"], "foo": ["f", None, "h"], "bar": [1, 2, 3], "baz": [None, "q", "r"]})
        indexed, keyed = MockKafkaProducer(), MockKafkaProducer()

        # When
        with patch("dynamicio.mixins.with_kafka.Producer", return_value=indexed):
            WriteKafkaIO(Topics.config(tmp_path)).write(frame.set_index("key"))
        with patch("dynamicio.mixins.with_kafka.Producer", return_value=keyed):
            WriteKeyedKafkaIO(Topics.config(tmp_path), key_column="key").write(frame)

        # Then
        assert keyed.my_stream == indexed.my_stream
        assert [message["key"] for message in keyed.my_stream] == [b"a", b"b", None]

    @pytest.mark.unit
    def test_a_key_column_that_the_frame_does_not_have_raises_value_error_before_the_first_message(self, tmp_path):
        # Given
        producer = MockKafkaProducer()
        io = WriteKafkaIO(Topics.config(tmp_path), key_column="key")

        # When / Then
        with patch("dynamicio.mixins.with_kafka.Producer", return_value=producer):
            with pytest.raises(ValueError, match="The `key_column` 'key' is not a column of the frame"):
                io.write(pd.DataFrame({"id": ["x"], "foo": ["f"], "bar": [1], "baz": ["b"]}))
        assert not producer.my_stream

    @pytest.mark.unit
    def test_a_pandas_key_column_that_is_not_a_name_raises_value_error(self, tmp_path):
        # Given
        io = WriteKafkaIO(Topics.config(tmp_path), key_column=["id"])

        # When / Then
        with patch("dynamicio.mixins.with_kafka.Producer", return_value=MockKafkaProducer()):
            with pytest.raises(ValueError, match="must be the name of a column"):
                io.write(pd.DataFrame({"id": ["x"], "foo": ["f"], "bar": [1], "baz": ["b"]}))

    @pytest.mark.unit
    def test_the_key_column_is_not_in_the_message_values(self, tmp_path):
        # When
        messages = Topics.send("polars", Topics.config(tmp_path), Plain, Topics.frame(2).drop("flag", "at", "took"), key_column="key")

        # Then
        assert all(b'"key"' not in value for _, value in messages)


class TestUserCallables:
    @staticmethod
    def calls(engine, config, model, frame):
        """The arguments of each user callable, as ``repr`` text, so that a NaN, a ``None`` and the types compare."""
        calls = []

        def key_generator(key, row):
            calls.append(("key_generator", repr(key), repr(row), {name: type(value).__name__ for name, value in row.items()}))
            return key

        def document_transformer(row):
            calls.append(("document_transformer", repr(row)))
            return {**row, "extra": 1}

        def key_serializer(key):
            calls.append(("key_serializer", repr(key)))
            return None if key is None else key.encode("utf-8")

        messages = Topics.send(
            engine,
            config,
            model,
            frame,
            key_column="key",
            key_generator=key_generator,
            document_transformer=document_transformer,
            key_serializer=key_serializer,
            value_serializer=Topics.repr_serializer,
        )
        return calls, messages

    @pytest.mark.unit
    def test_the_callables_get_the_same_arguments_and_give_the_same_bytes_on_both_engines(self, tmp_path):
        # Given
        frame = Topics.frame()

        # When
        polars_calls, polars = self.calls("polars", Topics.config(tmp_path), Messages, frame)
        pandas_calls, pandas = self.calls("pandas", Topics.config(tmp_path), PandasMessages, Topics.pandas_frame(frame))

        # Then
        assert polars_calls == pandas_calls
        assert polars == pandas
        assert [call[0] for call in polars_calls[:3]] == ["key_generator", "document_transformer", "key_serializer"]

    @pytest.mark.unit
    def test_the_rows_have_the_python_types_of_the_pandas_engine(self, tmp_path):
        # When
        calls, _ = self.calls("polars", Topics.config(tmp_path), Messages, Topics.frame())

        # Then
        types = [call[3] for call in calls if call[0] == "key_generator"]
        assert types[0] == {"id": "int", "price": "float", "name": "str", "flag": "bool", "n": "NoneType", "at": "Timestamp", "took": "Timedelta"}
        assert types[1]["price"] == "float" and types[1]["n"] == "int"
        assert types[2]["name"] == "NoneType" and types[3]["at"] == "NaTType"

    @pytest.mark.unit
    def test_a_nanosecond_time_keeps_its_nanoseconds(self, tmp_path):
        # When
        polars = Topics.send("polars", Topics.config(tmp_path), Messages, Topics.frame(1), key_column="key", value_serializer=Topics.repr_serializer)
        pandas = Topics.send("pandas", Topics.config(tmp_path), PandasMessages, Topics.pandas_frame(Topics.frame(1)), key_column="key", value_serializer=Topics.repr_serializer)

        # Then
        assert polars == pandas
        assert b"2024-01-01 01:02:03.123456789" in polars[0][1]
        assert b"0 days 00:00:00.000000007" in polars[0][1]


class TestChunks:
    @pytest.mark.unit
    def test_the_chunks_keep_the_order_of_the_rows_and_the_producer_flushes_once(self, tmp_path):
        # Given
        EngineSetting.reset("polars")
        frame = Topics.frame(7).drop("flag", "at", "took")
        producer = MockKafkaProducer()
        flushes = []
        producer.flush = lambda timeout=None: flushes.append(timeout) or 0

        # When
        with patch.object(PolarsKafka, "CHUNK_ROWS", 3), patch("dynamicio.mixins.with_kafka.Producer", return_value=producer):
            FilesIO(source_config=Topics.config(tmp_path), schema=Plain, key_column="key").write(frame)

        # Then
        assert [message["key"] for message in producer.my_stream] == [None if i == 1 else f"k{i}".encode() for i in range(7)]
        assert flushes == [None]

    @pytest.mark.unit
    def test_a_frame_without_rows_sends_no_message(self, tmp_path):
        # When
        messages = Topics.send("polars", Topics.config(tmp_path), Plain, Topics.frame(0).drop("flag", "at", "took"), key_column="key")

        # Then
        assert not messages

    @pytest.mark.unit
    def test_a_lazy_frame_is_collected_and_sent(self, tmp_path):
        # Given
        frame = Topics.frame(3).drop("flag", "at", "took")

        # When
        lazy = Topics.send("polars", Topics.config(tmp_path), Plain, frame.lazy(), key_column="key")
        eager = Topics.send("polars", Topics.config(tmp_path), Plain, frame, key_column="key")

        # Then
        assert lazy == eager and len(lazy) == 3
