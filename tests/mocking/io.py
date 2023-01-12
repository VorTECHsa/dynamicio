# pylint: disable=missing-class-docstring, missing-module-docstring, missing-function-docstring

from dynamicio import UnifiedIO
from dynamicio.core import SCHEMA_FROM_FILE


class ReadS3IO(UnifiedIO):
    schema = {"id": "int64"}


class ReadMockS3CsvIO(UnifiedIO):
    schema = SCHEMA_FROM_FILE


class TemplatedFile(UnifiedIO):
    schema = {"id": "int64", "foo_name": "object", "bar": "int64"}


class ReadLocalParquetTemplated(UnifiedIO):
    schema = {"id": "int64", "foo_name": "object", "bar": "int64"}


class ReadS3CsvIO(UnifiedIO):
    schema = SCHEMA_FROM_FILE


class ReadS3DataWithLessColumnsIO(UnifiedIO):
    schema = {"id": "int64", "foo_name": "object"}


class ReadS3DataWithFalseTypes(UnifiedIO):
    schema = {"id": "float64", "foo_name": "object"}


class ReadS3DataWithLessColumnsAndMessedOrderOfColumnsIO(UnifiedIO):
    schema = {
        "bar": "int64",
        "foo_name": "object",
        "a_number": "int64",
        "b_number": "int64",
        "bar_type": "object",
    }


class ReadS3ParquetIO(UnifiedIO):
    schema = {"id": "int64", "foo_name": "object", "bar": "int64"}


class ReadS3ParquetWEmptyFilesIO(UnifiedIO):
    schema = {"id": "object", "bar": "int64"}


class ReadS3ParquetWithLessColumnsIO(UnifiedIO):
    schema = {"id": "int64", "foo_name": "object"}


class ReadS3HdfIO(UnifiedIO):
    schema = {"id": "int64", "foo_name": "object", "bar": "int64"}


class AsyncReadS3HdfIO(UnifiedIO):
    schema = {"col_1": "int64", "col_2": "object"}


class ReadS3JsonIO(UnifiedIO):
    schema = {"id": "int64", "foo_name": "object", "bar": "int64"}


class WriteS3ParquetIO(UnifiedIO):
    schema = {"col_1": "int64", "col_2": "object"}


class WriteS3ParquetExternalIO(UnifiedIO):
    schema = {
        "bar": "int64",
        "event_type": "object",
        "id": "int64",
        "end_odometer": "int64",
        "foo_name": "object",
    }


class WriteS3CsvIO(UnifiedIO):
    schema = {"id": "int64", "foo_name": "object", "bar": "int64"}


class WriteS3CsvWithSchema(UnifiedIO):
    schema = SCHEMA_FROM_FILE


class WriteS3HdfIO(UnifiedIO):
    schema = {"col_1": "int64", "col_2": "object"}


class WriteS3JsonIO(UnifiedIO):
    schema = {"col_1": "int64", "col_2": "object"}


class ReadPostgresIO(UnifiedIO):
    schema = {"id": "object", "foo": "object", "bar": "int64", "baz": "object"}


class WritePostgresIO(UnifiedIO):
    schema = {"id": "object", "foo": "object", "bar": "int64", "baz": "object"}


class WriteExtendedPostgresIO(UnifiedIO):
    schema = {"id": "object", "foo": "object", "bar": "int64", "start_date": "datetime64[ns]", "active": "bool", "net": "float64"}


class WriteKafkaIO(UnifiedIO):
    schema = {"id": "object", "foo": "object", "bar": "int64", "baz": "object"}


class WriteKeyedKafkaIO(UnifiedIO):
    schema = {"key": "object", "id": "object", "foo": "object", "bar": "int64", "baz": "object"}


class MockKafkaProducer:
    def __init__(self):
        self.my_stream = []

    def send(self, topic: str, value: dict, key: str = None):  # pylint: disable=unused-argument
        self.my_stream.append({"key": key, "value": value})

    def flush(self):
        pass

    def close(self):
        pass


class ReadS3ParquetWithDifferentCastableDTypeIO(UnifiedIO):
    schema = {"id": "int64", "foo_name": "object", "bar": "int64"}

    # Input format of some_parquet_to_read.parquet is:
    # id,foo_name,bar
    # 1,foo_a,1
    # 2,foo_b,2
    # ...
    # 15,foo_a,15


class ReadS3ParquetWithDifferentNonCastableDTypeIO(UnifiedIO):
    schema = {"id": "int64", "foo_name": "int64", "bar": "int64"}

    # Input format of some_parquet_to_read.parquet is:
    # id,foo_name,bar
    # 1,foo_a,1
    # 2,foo_b,2
    # ...
    # 15,foo_a,15


class ReadFromBatchLocalParquet(UnifiedIO):
    schema = {"id": "int64", "foo_name": "object", "bar": "int64"}


class ReadFromBatchLocalHdf(UnifiedIO):
    schema = {"id": "int64", "foo_name": "object", "bar": "int64"}


class ParquetWithSomeBool(UnifiedIO):
    schema = {"id": "int64", "foo_name": "object", "bar": "int64", "bool_col": "bool"}


class CsvWithSomeBool(UnifiedIO):
    schema = {"id": "int64", "foo_name": "object", "bar": "int64", "bool_col": "bool"}


class HdfWithSomeBool(UnifiedIO):
    schema = {"id": "int64", "foo_name": "object", "bar": "int64", "bool_col": "bool"}


class JsonWithSomeBool(UnifiedIO):
    schema = {"id": "int64", "foo_name": "object", "bar": "int64", "bool_col": "bool"}


class ParquetWithCustomValidate(UnifiedIO):
    schema = {"id": "int64", "foo_name": "object", "bar": "int64", "bool_col": "bool"}

    @staticmethod
    def validate(df):
        if not df["id"].is_unique:
            return False
        if df["bar"].isna().any():
            return False
        return True
