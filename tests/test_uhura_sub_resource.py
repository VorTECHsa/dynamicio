import pandera
from uhura.modes import task_test_mode

from dynamicio import CsvResource, ParquetResource


class MySchemaIn(pandera.SchemaModel):
    id: pandera.typing.Series[int]
    foo_name: pandera.typing.Series[str]
    bar: pandera.typing.Series[int]

    class Config:
        strict = "filter"


class MySchemaOut(pandera.SchemaModel):
    id: pandera.typing.Series[int]
    foo_name: pandera.typing.Series[str]

    class Config:
        strict = "filter"


def test_poc_parquet():
    pq_resource_in = ParquetResource(
        path="resources/data/input/parquet_sample.parquet",
        test_path="resources/data/input/parquet_sample.parquet",
        pa_schema=MySchemaIn,
    )
    pq_resource_out = ParquetResource(
        path="resources/data/output/parquet_sample.parquet",
        test_path="resources/data/input/modified.parquet",
        pa_schema=MySchemaOut,
    )

    with task_test_mode(input_path=".", known_good_path="."):
        df = pq_resource_in.read()
        pq_resource_out.write(df)


def test_old_csv():
    csv_resource_in = CsvResource(
        path="resources/data/input/csv_sample.csv",
        test_path="resources/data/input/csv_sample.csv",
        pa_schema=MySchemaIn,
    )
    csv_resource_out = CsvResource(
        path="resources/data/output/csv_sample.csv",
        test_path="resources/data/input/modified.csv",
        pa_schema=MySchemaOut,
    )

    with task_test_mode(input_path=".", known_good_path="."):
        df = csv_resource_in.read()
        csv_resource_out.write(df)
