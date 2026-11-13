"""A module for defining sql_alchemy models."""

# pylint: disable=too-few-public-methods, R0801, C0104
# pylint: disable=missing-class-docstring
import pandera.pandas as pa
from pandera.typing import Series
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

from dynamicio.schema import Metric, metrics

Base = declarative_base()


class ERModel(Base):
    """
    Sql_alchemy model for example table

    """

    __tablename__ = "example"

    id = Column(String, primary_key=True)
    foo = Column(String)
    bar = Column(Integer)
    baz = Column(String)


clsdict = {
    "clsname": "PgModel",
    "__tablename__": "pg",
    "id": Column(String(64), primary_key=True, nullable=False),
    "foo": Column(String(64)),
    "bar": Column(Integer()),
    "baz": Column(String(64)),
}
PgModel = type(clsdict["clsname"], (Base,), clsdict)


# Pandera models for the IO classes in `tests.mocking.io`. Columns are declared `nullable=True`: the validations a test exercises are
# declared explicitly, the rest only fix the dtype dynamicio casts to.
class IdOnly(pa.DataFrameModel):
    __dataset_name__ = "id_only"

    id: Series[int] = pa.Field(nullable=True)


class JsonData(pa.DataFrameModel):
    __dataset_name__ = "json_data"

    data: Series[object] = pa.Field(nullable=True)


class ReleaseTimestamp(pa.DataFrameModel):
    __dataset_name__ = "release_timestamp"

    release: Series[object] = pa.Field(nullable=True)
    timestamp: Series[int] = pa.Field(nullable=True)


class IdFooNameBar(pa.DataFrameModel):
    __dataset_name__ = "id_foo_name_bar"

    id: Series[int] = pa.Field(nullable=True)
    foo_name: Series[object] = pa.Field(nullable=True)
    bar: Series[int] = pa.Field(nullable=True)


class ReadS3ParquetIO(IdFooNameBar):
    __dataset_name__ = "read_s3_parquet_io"


class IdFooNameFloatBar(pa.DataFrameModel):
    __dataset_name__ = "id_foo_name_float_bar"

    id: Series[int] = pa.Field(nullable=True)
    foo_name: Series[object] = pa.Field(nullable=True)
    bar: Series[float] = pa.Field(nullable=True)


class IdFooName(pa.DataFrameModel):
    __dataset_name__ = "id_foo_name"

    id: Series[int] = pa.Field(nullable=True)
    foo_name: Series[object] = pa.Field(nullable=True)


class ReadS3DataWithFalseTypes(pa.DataFrameModel):
    id: Series[float] = pa.Field(nullable=True)
    foo_name: Series[object] = pa.Field(nullable=True)

    __dataset_name__ = "read_s3_data_with_false_types"


class MessedOrderOfColumns(pa.DataFrameModel):
    __dataset_name__ = "messed_order_of_columns"

    bar: Series[int] = pa.Field(nullable=True)
    foo_name: Series[object] = pa.Field(nullable=True)
    a_number: Series[int] = pa.Field(nullable=True)
    b_number: Series[int] = pa.Field(nullable=True)
    bar_type: Series[object] = pa.Field(nullable=True)


class IdFooNameStatus(pa.DataFrameModel):
    __dataset_name__ = "id_foo_name_status"

    id: Series[int] = pa.Field(nullable=True)
    foo_name: Series[object] = pa.Field(nullable=True)
    status: Series[object] = pa.Field(nullable=True)


class ObjectIdBar(pa.DataFrameModel):
    __dataset_name__ = "object_id_bar"

    id: Series[object] = pa.Field(nullable=True)
    bar: Series[int] = pa.Field(nullable=True)


class Col1Col2(pa.DataFrameModel):
    __dataset_name__ = "col1_col2"

    col_1: Series[int] = pa.Field(nullable=True)
    col_2: Series[object] = pa.Field(nullable=True)


class ExternalEvents(pa.DataFrameModel):
    __dataset_name__ = "external_events"

    bar: Series[int] = pa.Field(nullable=True)
    event_type: Series[object] = pa.Field(nullable=True)
    id: Series[int] = pa.Field(nullable=True)
    end_odometer: Series[int] = pa.Field(nullable=True)
    foo_name: Series[object] = pa.Field(nullable=True)


class PgRows(pa.DataFrameModel):
    """The `pg` table: the Postgres counterpart of the old `pg.yaml` schema."""

    __dataset_name__ = "pg_rows"

    id: Series[object] = pa.Field(nullable=True)
    foo: Series[object] = pa.Field(nullable=True)
    bar: Series[int] = pa.Field(nullable=True)
    baz: Series[object] = pa.Field(nullable=True)

    __table_name__ = "pg"


class ExtendedPgRows(pa.DataFrameModel):
    __dataset_name__ = "extended_pg_rows"

    id: Series[object] = pa.Field(nullable=True)
    foo: Series[object] = pa.Field(nullable=True)
    bar: Series[int] = pa.Field(nullable=True)
    start_date: Series[pa.DateTime] = pa.Field(nullable=True)
    active: Series[bool] = pa.Field(nullable=True)
    net: Series[float] = pa.Field(nullable=True)

    __table_name__ = "extended_pg"


class KafkaMessages(pa.DataFrameModel):
    __dataset_name__ = "kafka_messages"

    id: Series[object] = pa.Field(nullable=True)
    foo: Series[object] = pa.Field(nullable=True)
    bar: Series[int] = pa.Field(nullable=True)
    baz: Series[object] = pa.Field(nullable=True)


class KeyedKafkaMessages(pa.DataFrameModel):
    __dataset_name__ = "keyed_kafka_messages"

    key: Series[object] = pa.Field(nullable=True)
    id: Series[object] = pa.Field(nullable=True)
    foo: Series[object] = pa.Field(nullable=True)
    bar: Series[int] = pa.Field(nullable=True)
    baz: Series[object] = pa.Field(nullable=True)


class IdFooNameInt(pa.DataFrameModel):
    __dataset_name__ = "id_foo_name_int"

    id: Series[int] = pa.Field(nullable=True)
    foo_name: Series[int] = pa.Field(nullable=True)
    bar: Series[int] = pa.Field(nullable=True)


class WithSomeBool(pa.DataFrameModel):
    __dataset_name__ = "with_some_bool"

    id: Series[int] = pa.Field(nullable=True)
    foo_name: Series[object] = pa.Field(nullable=True)
    bar: Series[int] = pa.Field(nullable=True)
    bool_col: Series[bool] = pa.Field(nullable=True)


class AthenaRows(pa.DataFrameModel):
    __dataset_name__ = "athena_rows"

    id: Series[object] = pa.Field(nullable=True)
    foo: Series[object] = pa.Field(nullable=True)
    bar: Series[int] = pa.Field(nullable=True)


class ReadFromS3Csv(pa.DataFrameModel):
    """The `read_from_s3_csv` schema: unique ids, a category, bounded integers, and the metrics that go with them."""

    id: Series[int] = pa.Field(nullable=False, unique=True, metadata=metrics(Metric.UNIQUE_COUNTS, Metric.COUNTS))
    foo_name: Series[object] = pa.Field(nullable=False, isin=["class_a", "class_b", "class_c"], metadata=metrics(Metric.COUNTS_PER_LABEL))
    bar: Series[int] = pa.Field(nullable=False, gt=1000, lt=2000, metadata=metrics(Metric.MIN, Metric.MAX, Metric.MEAN, Metric.STD, Metric.VARIANCE))

    __dataset_name__ = "read_from_s3_csv"


class Products(pa.DataFrameModel):
    """A schema that declares no validations and no metrics."""

    __dataset_name__ = "products"

    id: Series[object] = pa.Field(nullable=True)


class OptionalParquet(pa.DataFrameModel):
    __dataset_name__ = "optional_parquet"

    id: Series[int] = pa.Field(nullable=True)


class NameableFile(pa.DataFrameModel):
    __dataset_name__ = "nameable_file"

    id: Series[int] = pa.Field(nullable=True)
    name: Series[object] = pa.Field(nullable=True)


class Partitioned(pa.DataFrameModel):
    __dataset_name__ = "partitioned"

    grp: Series[object] = pa.Field(nullable=True)
    val: Series[int] = pa.Field(nullable=True)


class MixedCaseAthena(pa.DataFrameModel):
    """Dtypes for mixed-case Athena columns, as a consumer's model would declare them."""

    __dataset_name__ = "mixed_case_athena"

    Id: Series[int] = pa.Field(nullable=True)
    Name: Series[object] = pa.Field(nullable=True)
    IsActive: Series[bool] = pa.Field(nullable=True)
    Amount: Series[float] = pa.Field(nullable=True)
    CreatedOn: Series[pa.DateTime] = pa.Field(nullable=True)
