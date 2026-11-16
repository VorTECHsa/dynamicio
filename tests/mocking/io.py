# pylint: disable=missing-class-docstring, missing-module-docstring, missing-function-docstring
from typing import Callable, Optional
from unittest.mock import MagicMock

# Application Imports
from dynamicio import UnifiedIO, WithLocal
from dynamicio.core import DynamicDataIO
from tests.mocking import models


class SubclassMissingMixin(WithLocal, DynamicDataIO):
    schema = models.IdOnly


class ReadS3IO(UnifiedIO):
    schema = models.IdOnly


class ReadS3JsonOrientRecordsIO(UnifiedIO):
    schema = models.JsonData


class ReadS3JsonOrientRecordsAltIO(UnifiedIO):
    schema = models.ReleaseTimestamp


class ReadMockS3CsvIO(UnifiedIO):
    schema = models.ReadFromS3Csv


class TemplatedFile(UnifiedIO):
    schema = models.IdFooNameBar


class ReadLocalParquetTemplated(UnifiedIO):
    schema = models.IdFooNameBar


class ReadS3CsvIO(UnifiedIO):
    schema = models.ReadFromS3Csv


class ReadS3CsvWithWrongSchemaIO(UnifiedIO):
    schema = models.IdFooNameFloatBar


class ReadS3DataWithLessColumnsIO(UnifiedIO):
    schema = models.IdFooName


class ReadS3DataWithFalseTypes(UnifiedIO):
    schema = models.ReadS3DataWithFalseTypes


class ReadS3DataWithLessColumnsAndMessedOrderOfColumnsIO(UnifiedIO):
    schema = models.MessedOrderOfColumns


class ReadS3ParquetIO(UnifiedIO):
    schema = models.ReadS3ParquetIO


class ReadS3ParquetWithLateColumnIO(UnifiedIO):
    schema = models.IdFooNameStatus


class ReadS3ParquetWEmptyFilesIO(UnifiedIO):
    schema = models.ObjectIdBar


class ReadS3ParquetWithLessColumnsIO(UnifiedIO):
    schema = models.IdFooName


class ReadS3HdfIO(UnifiedIO):
    schema = models.IdFooNameBar


class AsyncReadS3HdfIO(UnifiedIO):
    schema = models.Col1Col2


class ReadS3JsonIO(UnifiedIO):
    schema = models.IdFooNameBar


class WriteS3IO(UnifiedIO):
    schema = models.Col1Col2


class WriteS3JsonOrientRecordsIO(UnifiedIO):
    schema = models.ReleaseTimestamp


class WriteS3ParquetExternalIO(UnifiedIO):
    schema = models.ExternalEvents


class WriteS3CsvIO(UnifiedIO):
    schema = models.IdFooNameBar


class WriteS3CsvWithSchema(UnifiedIO):
    schema = models.ReadFromS3Csv


class WriteS3HdfIO(UnifiedIO):
    schema = models.Col1Col2


class WriteS3JsonIO(UnifiedIO):
    schema = models.Col1Col2


class ReadPostgresIO(UnifiedIO):
    schema = models.PgRows


class WritePostgresIO(UnifiedIO):
    schema = models.PgRows


class WriteExtendedPostgresIO(UnifiedIO):
    schema = models.ExtendedPgRows


class WriteKafkaIO(UnifiedIO):
    schema = models.KafkaMessages


class WriteKeyedKafkaIO(UnifiedIO):
    schema = models.KeyedKafkaMessages


class MockKafkaProducer:
    def __init__(self, **kwargs):
        self.my_stream = []
        self.config = kwargs  # Store the config to ensure it can accept any options
        self.produce_call_count = 0

    def produce(self, topic: str, key: Optional[bytes], value: bytes, on_delivery: Callable):  # pylint: disable=unused-argument
        self.my_stream.append({"key": key, "value": value})
        on_delivery(None, MagicMock())
        self.produce_call_count += 1

    def flush(self):
        pass

    def close(self):
        pass


class ReadS3ParquetWithDifferentCastableDTypeIO(UnifiedIO):
    schema = models.IdFooNameBar

    # Input format of some_parquet_to_read.parquet is:
    # id,foo_name,bar
    # 1,foo_a,1
    # 2,foo_b,2
    # ...
    # 15,foo_a,15


class ReadS3ParquetWithDifferentNonCastableDTypeIO(UnifiedIO):
    schema = models.IdFooNameInt

    # Input format of some_parquet_to_read.parquet is:
    # id,foo_name,bar
    # 1,foo_a,1
    # 2,foo_b,2
    # ...
    # 15,foo_a,15


class ReadFromBatchLocalParquet(UnifiedIO):
    schema = models.IdFooNameBar


class ReadFromBatchLocalHdf(UnifiedIO):
    schema = models.IdFooNameBar


class ParquetWithSomeBool(UnifiedIO):
    schema = models.WithSomeBool


class CsvWithSomeBool(UnifiedIO):
    schema = models.WithSomeBool


class HdfWithSomeBool(UnifiedIO):
    schema = models.WithSomeBool


class JsonWithSomeBool(UnifiedIO):
    schema = models.WithSomeBool


class ParquetWithCustomValidate(UnifiedIO):
    schema = models.WithSomeBool

    @staticmethod
    def validate(df):
        if not df["id"].is_unique:
            return False
        if df["bar"].isna().any():
            return False
        return True


class ReadAthenaIO(UnifiedIO):
    schema = models.AthenaRows


class GenericIO(UnifiedIO):
    schema = models.ReadFromS3Csv
