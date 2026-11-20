# The polars engine

The polars engine casts, validates and logs the metrics of polars frames. It has the validators and the metrics of the pandas engine, with the same
names, messages and `METRIC` lines. It reads, scans and writes parquet, CSV and JSON files on a `local` or an `s3_file` resource, and reads and
scans a `local_batch` or an `s3_path_prefix` resource. It writes an `s3_path_prefix` resource, and `sink()` writes a `LazyFrame`. It reads and
writes a `postgres` resource, reads an `athena` resource and writes a `kafka` resource. A Kafka read and an Athena write raise the
`NotImplementedError` of the pandas engine.

[ADR 0010](https://github.com/VorTECHsa/dynamicio/pull/140) records the design. The ADR is not final and changes with each release of the engine.

## Set the engine

The examples of this page are one script. The test suite runs them in order, in an empty directory, on `local` files.

An engine is a setting of the process. Set it before the first read or write:

<!-- doctest -->
```python
import dynamicio

dynamicio.configure(engine="polars")
```

The first read or write fixes the engine. After that, `dynamicio.configure()` with another engine raises `EngineConfigurationError`. A read or a write
that the engine refuses does not fix it, for example a `scan()` on the pandas engine.

The polars engine needs polars 2.1 or later. Without polars, or with an older polars, `dynamicio.configure(engine="polars")` raises `ImportError`.
This check reads the version of the installed package and does not import polars. Install polars with `pip install "dynamicio[polars]"`.
The pandas engine and the CLI do not need polars, but `dynamicio migrate --engine polars` needs it.

## Migrate a repo

`dynamicio migrate --engine polars` writes a polars model for each pandas model of a module or a package directory. Run it from the root of the repo,
because it imports the modules by their dotted name:

```bash
dynamicio migrate --engine polars src/resources/schemas -o src/resources/polars_schemas --scan src/resources
```

An output that ends with `.py` gets all the models. Another output is a package directory with one module for each module of the pandas models. Each
polars model has the dataset, the table, the dtypes, the validations, the metrics and the `Config` options of its pandas model. The command compiles
each generated model and compares it with its pandas model. A difference, or a dtype without a polars pair, raises `ValueError`, and the command then
writes no file. A field gets `pandas_dtype` when the reverse map of its polars dtype does not give its pandas dtype, for example `Int64` or `string`.

The command also writes `POLARS_MIGRATION.md` next to the models. The report lists these items:

- The Pandera checks to write again for polars: `str_matches`, a `@pa.check` and a `@pa.dataframe_check`. The polars models do not have them.
- A model with an index. A polars frame has no index, so write the index as a column and declare it in the model.
- The `Series[object]` columns. Their polars models have the `String` dtype. Declare `pl.List` of the item dtype for a column that holds lists, for example `pl.List(pl.String)`.
- The resource definitions under `--scan` that the polars engine refuses, for example an `hdf` file or a Kafka resource without `key_column`.
- The CSV definitions without an `index` option. The polars engine writes the leading index column with the row numbers.

The command does not change the resource definitions, the options that the code gives to an IO object, or the transformation code.

The package `demo/polars_pipeline` of the repo is the demo pipeline after a migration. The command wrote its models from the pandas models of `demo/src`.
Its definitions change the two resources of the report: `FOO` is a parquet file on S3, and the Kafka resource sets `key_column`. Its runners read and
write polars frames, and the transform task uses `scan()` and `sink()`. The pipeline reads the inputs of the pandas demo, and its outputs are equal to the
expected outputs of the pandas demo. Run its tests with `pytest -o addopts="-p no:warnings" --dynamicio-engine=polars demo`.

## Processes and threads

polars can deadlock in a fork of a process that has used polars. A fork of a process that imported polars thus refuses the polars engine, also when the
parent has the pandas engine. Its first polars read or write and `dynamicio.configure(engine="polars")` raise `EngineConfigurationError`. Start the processes with `spawn` or `forkserver`, for example
`multiprocessing.get_context("spawn")`. A spawned process does not get the `configure()` call of its parent, so call `configure()` in the package
`__init__.py` of the repo. A fork can still use the pandas engine, and a fork of a process that did not import polars does not change.

dynamicio does not set the number of polars threads. polars reads `POLARS_MAX_THREADS` when it is first imported, and `import dynamicio` does not import
polars. Thus a value that the repo sets after `import dynamicio` and before the first read or write applies.

## Declare a polars model

A polars model is a `dynamicio.schema.polars.DataModel`. It has the same `dataset` and `table` class keywords as a pandas model, and its columns
use the polars dtypes:

<!-- doctest -->
```python
from typing import Annotated

import polars as pl

from dynamicio.schema import Field, Metric
from dynamicio.schema.polars import DataModel


class Orders(DataModel, dataset="orders"):
    order_id: int = Field(nullable=False, unique=True, gt=0, metrics=[Metric.MIN, Metric.MAX])
    price: float = Field(nullable=True, ge=0.0, max_null_fraction=0.5)
    status: Annotated[pl.Enum, ["open", "closed"]] = Field(nullable=False)
    created_at: Annotated[pl.Datetime, "ms", "UTC"] = Field(nullable=False)
    quantity: int = Field(nullable=True, pandas_dtype="Int64")
```

A model of one engine on the other engine raises `ModelEngineMismatchError` when the IO object is created.

## Dtypes

dynamicio keeps a map between the pandas dtypes and the polars dtypes. Each column of a polars model must have a dtype of the map:

| polars dtype | pandas dtype, without nulls | pandas dtype, with nulls |
|---|---|---|
| `Int8` to `Int64`, `UInt8` to `UInt64` | `int8` to `int64`, `uint8` to `uint64` | `Int8` to `Int64`, `UInt8` to `UInt64` |
| `Float32`, `Float64` | `float32`, `float64` | `float32`, `float64` |
| `Boolean` | `bool` | `boolean` |
| `String` | `object` | `object` |
| `Categorical`, `Enum` | `category` | `category` |
| `Datetime` (`ms`, `us`, `ns`, with or without a time zone) | `datetime64[unit]` or `datetime64[unit, zone]` | the same |
| `Duration` (`ms`, `us`, `ns`) | `timedelta64[unit]` | the same |
| `List` of a dtype of this table, other than `Categorical` and `Enum` | `object` | `object` |

A model with a different dtype raises `ValueError` when it compiles. Examples are `Date`, `Decimal`, `Struct` and a `List` of `Categorical`. polars has no second unit, so a
pandas `datetime64[s]` has no pair.

The compiled schema of a polars model (`io.schema.columns[name].data_type`) holds the pandas dtype. A column takes the pandas dtype "without nulls"
of the table. To set a different pandas dtype, use `Field(pandas_dtype=...)`, for example `"Int64"` for a nullable integer or `"string[pyarrow]"` for a
string. The value must be a pair of the polars dtype of the column, else the model raises `ValueError` when it compiles. A pandas model does not accept
`pandas_dtype`, because its column type is a pandas dtype.

## Read and write files

The polars engine reads and writes these resources:

| Resource | Read | Write |
|---|---|---|
| `local` | parquet, CSV, JSON | parquet, CSV, JSON |
| `local_batch` | parquet, CSV, JSON | no writer, as on the pandas engine |
| `s3_file` | parquet, CSV, JSON | parquet, CSV, JSON |
| `s3_path_prefix` | parquet, CSV, JSON | parquet, CSV, JSON, with the prefix writer of the pandas engine |

A file that the polars engine writes gives the same frame to the pandas engine of 11.1.0:

- The CSV and JSON writers change the frame to pandas, with the pandas dtypes of the model, and call the pandas writers. The bytes are equal to the bytes of the pandas engine.
- The parquet writer calls `pyarrow.parquet.write_table` with the arrow types and the pandas metadata that `DataFrame.to_parquet` gives. The compression is SNAPPY, as on pandas. A `category` column has the code width of pandas: `int8` below 127 categories, `int16` below 32767 and `int32` after that.

The readers are polars readers, and the engine then casts the frame to the model:

- A CSV file uses the null strings of `pandas.read_csv`. A column that is not a number or a Boolean reads as a string, so `007` keeps its text. The cast then parses the datetimes, the durations (for example `0 days 00:00:01`) and the Booleans (`True`, `False`, `true`, `false`, `TRUE`, `FALSE`).
- A CSV file cannot hold a list. A CSV resource of a model with a `List` column raises `EngineResourceError` when the IO object is created. Use a parquet or a JSON file for it.
- A JSON file has the orient records, or JSON lines with `lines: true`. A JSON object with the orient index or columns needs `index_column` (see [JSON keys](#json-keys)). An integer under a `Datetime` or a `Duration` column is an epoch in `date_unit`, which is `ms` by default. An epoch of a `Datetime` with a time zone is a UTC instant. A file without records, as the pandas engine writes a frame without rows, reads as a frame without rows with the columns of the model.
- A `local_batch` resource reads its files in sorted order. A file without a column of the model gives nulls in that column.
- A missing local file raises `FileNotFoundError`. A missing S3 key raises `S3KeyNotFoundError` for parquet and `ObjectNotFoundError` for CSV and JSON, as on the pandas engine. With `missing_ok=True`, `read()` returns `None`.
- An `s3_path_prefix` resource reads all the objects under `path_prefix`, or the objects that match `dynamic_file_path`, in the order of the S3 listing, as on the pandas engine. Keys that end in `/` are left out. A prefix without objects raises `NoObjectsFoundError`.
- An `s3_file` key that ends in `/` raises `NotImplementedError` at read. A parquet write to such a key raises `ValueError`, as on the pandas engine.

`write()` collects a `LazyFrame` before it writes the file.

This example writes and reads a parquet file of the `Orders` model. The resource definitions have a second file, `big_orders.parquet`, for the
examples of `sink()`:

<!-- doctest -->
```python
import pathlib
from datetime import datetime, timezone

from dynamicio import UnifiedIO
from dynamicio.config import IOConfig

pathlib.Path("orders.yaml").write_text(
    """
ORDERS:
  LOCAL:
    type: local
    local:
      file_path: "[[ DATA_DIR ]]/orders.parquet"
      file_type: parquet
BIG_ORDERS:
  LOCAL:
    type: local
    local:
      file_path: "[[ DATA_DIR ]]/big_orders.parquet"
      file_type: parquet
"""
)


class Settings:
    DATA_DIR = "."


class OrdersIO(UnifiedIO):
    schema = Orders


config = IOConfig("orders.yaml", env_identifier="LOCAL", dynamic_vars=Settings)
orders = OrdersIO(source_config=config.get(source_key="ORDERS"))
orders.write(
    pl.DataFrame(
        {
            "order_id": [1, 2, 3],
            "price": [9.5, None, 3.25],
            "status": ["open", "closed", "open"],
            "created_at": [datetime(2026, 1, day, tzinfo=timezone.utc) for day in (1, 2, 3)],
            "quantity": [4, None, 12],
        }
    )
)

frame = orders.read()
assert frame.schema["quantity"] == pl.Int64
assert orders.schema.columns["quantity"].data_type == "Int64"
```

The pandas engine reads the same file with the pandas dtypes of the model, for example `Int64` for `quantity`:

<!-- doctest -->
```python
import pandas as pd

assert pd.read_parquet("orders.parquet").dtypes["quantity"] == "Int64"
```

A parquet file of the polars engine goes to S3 in one `PutObject` up to 64 MiB, and in a multipart upload of 64 MiB parts above that, with 4 parts
at a time. Each upload has the `bucket-owner-full-control` ACL. A failed part aborts the upload, so S3 keeps no part and the old object does not change.

An `s3_path_prefix` write goes to the prefix writer of the pandas engine. `partition_cols`, `mode` and `allow_delete` thus write the same objects and
delete the same old objects. A prefix read on the two engines does not read the `col=value` directories back as a column. A read of a prefix with
`partition_cols` thus raises the `ValueError` of a column that no object has.

## Scan

`scan()` gives a `LazyFrame` with the columns of the model and the casts of the model in its plan. Nothing is read before you collect it:

<!-- doctest -->
```python
lazy = orders.scan()
frame = lazy.filter(pl.col("quantity") > 10).select("order_id").collect()
assert frame["order_id"].to_list() == [3]
```

- A parquet or CSV scan uses `scan_parquet` or `scan_csv`. A filter or a selection then reads only the row groups and the columns that it needs.
- A JSON scan reads the file with the eager reader, then calls `lazy()`. polars has no lazy reader of a JSON array.
- A CSV scan reads the file once for the types of its columns, as the eager read does. The columns are in the order of the file, as on `read()`.
- A failed cast raises when you collect the frame.
- `scan()` does not validate the frame or log its metrics, because a lazy frame has no rows before you collect it. It logs a warning if the IO object asks for validations or metrics. Collect the frame and call `validate_from_schema`, or use `read()`.
- A missing file or key raises the error of `read()`. With `missing_ok=True`, `scan()` returns `None`.
- `scan()` reads a `local`, a `local_batch`, an `s3_file` or an `s3_path_prefix` resource. On another resource, for example `postgres`, it raises `NotImplementedError`.

A parquet prefix is one scan of all its objects:

- The engine reads the footer of each object. A column has the widest type of the footers, as `pd.concat` gives it, for example `Int64` for `Int32` and `Int64`, and `Float64` for an integer and a float. The scan upcasts the narrower objects.
- `Datetime` columns with one time zone, and `Duration` columns, give the finest time unit. The objects with the same `Duration` units are one scan, and a cast gives the finest unit. The rows keep the order of the listing.
- A pair of types that does not upcast, for example `String` and `Int64`, or `Int64` and `UInt64`, raises `ColumnsDataTypeError`.
- When the types of a column do not upcast, the objects without rows do not count, as in the `pd.concat` of the pandas engine. The engine then reads the row count of each footer. If all the objects are empty, the types count.
- An object that lacks a column gets nulls in it, with the warning of the pandas engine. A column that no object has raises the `ValueError` of the pandas engine.

The S3 reads of polars use the credentials and the region of the boto3 session of dynamicio. polars refreshes the credentials when they expire. A boto3 endpoint that is not an AWS endpoint, for example a local S3 stack, also goes to polars.

### Options

The `options` of a resource go beside its `local` or `s3` section. A file type accepts the options of its pandas reader and of its pandas writer that have a polars entry. Examples are `sep`,
`na_values` and `nrows` for CSV, and `compression` and `row_group_size` for parquet. The engine gives the read options to the polars reader with their polars names,
for example `separator` for `sep`, and `decimal_comma` for `decimal: ","`. `quoting: 3` (`QUOTE_NONE`) reads with no quote character, as pandas does.

A CSV `names` option names the columns of the file, as on pandas. Without `header`, the first line is data. With `header: 0`, the names replace the
names of the header line. A file with another number of columns raises the `SchemaError` of polars. pandas then makes an index of the first columns, or
adds columns of nulls.

The polars engine raises `EngineResourceError` for a resource that it cannot read or write:

- a file type other than parquet, CSV and JSON, for example `hdf`
- an option that has no polars entry, for example `dtype`, `parse_dates` or the parquet `filters`. A polars read filters with `scan()` and `filter()`
- the parquet `engine: fastparquet`, or `index: true`, because a polars frame has no index
- a CSV `header` on a line other than the first, a `skiprows` that is not a number, or an encoding other than UTF-8
- a CSV `na_values` map with null strings for each column, or a `decimal` mark other than `.` and `,`
- a CSV `escapechar`, `doublequote: false`, or a `quoting` other than `QUOTE_MINIMAL`, `QUOTE_ALL`, `QUOTE_NONNUMERIC` and `QUOTE_NONE`. The pandas reader also applies these writer options, and the polars reader has no equivalent
- a read of a CSV resource with `header: false` or `header: null` and without `names`. The pandas engine cannot read that file either. A write with `header: false` is accepted
- a JSON orient other than records, index and columns, and the orient index or columns without `index_column`. The default orient is records on an `s3_file` resource and columns on the other resources. A `local` or an `s3_path_prefix` JSON resource must thus set `orient: records` or `index_column`.
- an `index_column` with a different orient or with `lines: true`, or an `index_column` that is not a column of the model

An `s3_path_prefix` resource also accepts `no_disk_space`, `partition_cols`, `mode` and `allow_delete`. The polars engine refuses `no_disk_space`, `partition_cols`
and `allow_delete` on the other resources. On a CSV or a JSON file, `mode` is the file mode of the writer.

The engine checks the resource when you make the IO object, and again at each read and write. Thus it also finds a change of the options after you make the IO object.

## Sink

`sink()` writes a `LazyFrame` with the casts, the validations and the metrics of the model in one `pl.collect_all`:

<!-- doctest -->
```python
big_orders = OrdersIO(source_config=config.get(source_key="BIG_ORDERS"), apply_schema_validations=True, log_schema_metrics=True)
big_orders.sink(orders.scan().filter(pl.col("quantity") > 10))
assert big_orders.read()["order_id"].to_list() == [3]
```

- A parquet sink to a `local` or an `s3_file` resource streams the rows to a temporary file. It then copies the row groups to a file with the arrow
  types, the pandas metadata and the options of `write()`. The file has the footer and the row groups of `write()`.
- A local file goes into place with `os.replace`, from a temporary directory beside it. An S3 file goes up with the upload of `write()`.
- A CSV, a JSON or a prefix sink collects the frame and calls the writer of `write()`, so the bytes and the objects are equal.
- A failed validation raises `SchemaValidationError`, and the file or the objects do not change. The metrics are logged only after the validations pass.
- A failed cast in the plan, or a schema column that is not in the frame, raises `ColumnsDataTypeError`.
- `sink()` writes a `local`, an `s3_file` or an `s3_path_prefix` resource. On another resource, for example `local_batch` or `postgres`, it raises `NotImplementedError`.

`sink()` also takes a list or a tuple of lazy frames, the parts of one frame in their order:

<!-- doctest -->
```python
big_orders.sink([orders.scan(), orders.scan().with_columns(pl.col("order_id") + 3)])
assert big_orders.read()["order_id"].to_list() == [1, 2, 3, 4, 5, 6]
```

Each part gets the schema columns and the casts of the model before the concatenation. The parts can thus have other types of a column, for example
`Int32` and `Int64`, or a column of nulls only. The parts can also have the columns in another order. An empty list raises `ValueError`.

## Footer

`footer()` reads the footer of the parquet file of a `local` or an `s3_file` resource, without its rows:

<!-- doctest -->
```python
import json

footer = big_orders.footer()
assert (footer.rows, footer.row_groups) == (6, 1)
assert str(footer.schema.field("created_at").type) == "timestamp[ms, tz=UTC]"
pandas_dtypes = {column["name"]: column["numpy_type"] for column in json.loads(footer.metadata[b"pandas"])["columns"]}
assert pandas_dtypes["quantity"] == "Int64"
```

The read is the same on the two engines, and does not fix the engine. On S3 it reads the end of the object with ranged `GetObject` calls. Each
call after the first sends the `ETag` of the first in `IfMatch`. If another writer replaces the object between two calls, S3 returns
`PreconditionFailed`, and the read raises its `ClientError`. The footer thus never mixes two versions of the object. A
missing file raises `FileNotFoundError`, or `S3KeyNotFoundError` on S3. With `missing_ok=True`, `footer()` returns `None`. Another resource or
file type raises `EngineResourceError`.

## Postgres

A read runs the query of the pandas engine, so `sql_query` and `params` give the same rows. `params` has the style of the driver, for example
`%(low)s`. The engine then casts the frame to the model. A `pd.read_sql` option that changes the frame, for example `dtype` or `parse_dates`,
raises `EngineResourceError`, because the cast of the model gives the types.

The Postgres, Kafka and Athena examples need a database, a broker or an AWS account, so the test suite does not run them. They use a
`CLOUD` environment of the same `orders.yaml`:

```yaml
ORDERS_DB:
  CLOUD:
    type: "postgres"
    postgres:
      db_host: "[[ DB_HOST ]]"
      db_port: "[[ DB_PORT ]]"
      db_name: "[[ DB_NAME ]]"
      db_user: "[[ DB_USER ]]"
      db_password: "[[ DB_PASS ]]"
```

```python
cloud = IOConfig("orders.yaml", env_identifier="CLOUD", dynamic_vars=Settings)
large = OrdersIO(
    source_config=cloud.get(source_key="ORDERS_DB"),
    sql_query="SELECT * FROM orders WHERE quantity > %(low)s",
    params={"low": 10},
).read()
```

A write needs the `table` class keyword of the model, for example `class Orders(DataModel, dataset="orders", table="sales.orders")`.

A write replaces the rows of the table with the COPY of the pandas engine, in one transaction:

- The frame goes to the COPY in chunks of 100,000 rows. Each chunk changes to pandas with the pandas dtypes of the model.
- The NULL rule and the text of each value are those of the pandas engine. An empty string and a NaN are thus written as NULL.
- A failed chunk rolls back the write, and the table keeps its old rows.
- A polars `Datetime` column has the unit `us`. The pandas engine maps `datetime64[us]` and `datetime64[ms]` to a Postgres `timestamp`.

## JSON keys

`DataFrame.to_json` writes the index of a pandas frame as the keys of a JSON object with the orient index or columns. A polars frame has no index,
so the option `index_column` names the column of the keys:

```yaml
REFERENCE_DATA:
  LOCAL:
    type: "local"
    local:
      file_path: "[[ TEST_RESOURCES ]]/reference_data.json"
      file_type: "json"
    options:
      orient: "index"
      index_column: "label"
```

The file `{"data":{"timestamp":1614268643313,"release":"feb09"}}` then reads as one row, in which `label` is `data`.

- A write takes the keys from the column. The file has the bytes of the pandas engine for the frame with `set_index(index_column)`.
- A read gives the keys in the column. A key is a JSON string, so the engine casts it to a numeric dtype of the model.
- The option works on the two engines. On the pandas engine, a read gives `reset_index(names=index_column)`.
- An `index_column` that is not a column of the model raises `EngineResourceError` when the IO object is created. The orient records and JSON lines have no keys, so `index_column` with them raises `EngineResourceError` on the polars engine and `ValueError` on the pandas engine.

## Kafka

A `kafka` resource of the polars engine needs the option `key_column`, the column of the message keys:

```yaml
WRITE_TO_KAFKA:
  CLOUD:
    type: "kafka"
    kafka:
      kafka_server: "[[ KAFKA_SERVER ]]"
      kafka_topic: "[[ KAFKA_TOPIC ]]"
    options:
      key_column: "order_id"
```

```python
OrdersIO(source_config=cloud.get(source_key="WRITE_TO_KAFKA")).write(frame)
```

- A polars frame has no index. A missing `key_column`, or a `key_column` that is not a column of the model, thus raises `EngineResourceError` at once.
- The column is not in the message values. On the pandas engine, `key_column` gives the bytes of `set_index(key_column)`.
- The frame goes to the producer of the pandas engine in chunks of 100,000 rows, and the producer flushes once.
- Each chunk changes to pandas with the dtypes of the model. `key_generator`, `document_transformer` and the serializers thus get the values and types of the pandas engine. A nanosecond `Datetime` reaches them as a `Timestamp` with its nanoseconds.

A Kafka read raises the `NotImplementedError` of the pandas engine.

## Athena

A read runs the awswrangler query of the pandas engine with its options, and the engine casts the result to the model. The `chunksize` option raises
the `ValueError` of the pandas engine. An Athena write raises the `NotImplementedError` of the pandas engine.

```yaml
ORDERS_ATHENA:
  CLOUD:
    type: athena
    athena:
      s3_output: "s3://[[ ATHENA_BUCKET ]]/dynamicio/"
      database: sales
```

```python
frame = OrdersIO(source_config=cloud.get(source_key="ORDERS_ATHENA"), query="SELECT * FROM orders").read()
```

## Cast

`read()`, `scan()`, `write()` and `sink()` cast a frame to the polars dtypes of the model:

- A `DataFrame` casts one column at a time. A column that does not cast raises `ColumnsDataTypeError`, and the `dynamicio.core` logger names the column.
- A `LazyFrame` gets the casts in its plan. A column that does not cast raises `ColumnsDataTypeError` when the validations or the caller collect the frame.
- A string column parses to a `Datetime` with `str.to_datetime`, in the time unit and the time zone of the model.
- A string column parses to a `Duration` with the parser of `pd.to_timedelta`, and to a `Boolean` from `True`, `False`, `true`, `false`, `TRUE` or `FALSE`. A `DataFrame` with another string raises `ColumnsDataTypeError`. A `LazyFrame` raises the polars `InvalidOperationError` when the caller collects it.
- A float NaN becomes null. In pandas, NaN and None are the two missing values. In polars, null is the only missing value, so the validators of nulls
  count a NaN after the cast.
- A schema column that is not in the frame, or a `columns` option with a column that is not in the schema, raises `ColumnsDataTypeError`.
- With `show_casting_warnings=True`, each column that casts logs its found and its expected dtype.
- The engine ignores `strict_dtype_check`.

<!-- doctest -->
```python
from dynamicio.errors import ColumnsDataTypeError

text = pl.DataFrame({"order_id": ["4"], "price": ["1.5"], "status": ["open"], "created_at": ["2026-02-01 10:00:00"], "quantity": [None]})
assert orders._apply_schema(text).schema["created_at"] == pl.Datetime("ms", "UTC")

try:
    orders._apply_schema(text.with_columns(order_id=pl.lit("x")))
except ColumnsDataTypeError as error:
    print(f"order_id does not cast: {error!r}")
else:
    raise AssertionError("a text that is not a number cast to an integer")
```

## Validations

`validate_from_schema()` runs these checks:

- the 9 validators of the pandas engine, for example `has_unique_values`, `is_in` and `is_between`
- the Pandera column checks that dynamicio has no validator for, for example `str_matches` or a custom `@pa.check`
- the dataframe-level checks of the model: `Config.unique`, `Config.ordered` and each `@pa.dataframe_check`

All of the checks run as lazy queries in one `pl.collect_all`, so the engine reads a `LazyFrame` one time. The failures raise one
`SchemaValidationError`, with one entry for each validation name, as on the pandas engine.

The messages of the column validators are the messages of the pandas engine. The dataframe-level checks have the keys and the message template of the
pandas engine (`multiple_fields_uniqueness`, `column_ordered`, the name of the check). Their counts and examples come from the polars failure cases, so
they are not always the pandas values. For a `@pa.dataframe_check`, the count is the number of failing rows. A check that returns one bool has a count of 1
when it fails. Two dataframe checks with one name both run, and their messages go into one entry. `Config.unique` with a list of column groups checks each
group. As on the pandas engine, the entry gives the first group with repeated rows.

`is_in` accepts a null when the pandas `isin` accepts it. A null of a float column matches NaN only. A null of an integer or a Boolean column matches no
value. A null of the other columns matches None or NaN.

A write with validations raises before it writes the file:

<!-- doctest -->
```python
from dynamicio.errors import SchemaValidationError

try:
    big_orders.write(orders.read().with_columns(order_id=pl.lit(1)))
except SchemaValidationError as error:
    assert "has_unique_values" in str(error)
else:
    raise AssertionError("a repeated order_id passed the validations")
assert big_orders.read().height == 6
```

`PolarsValidations.validator` registers a polars validator, as `@validator` of `dynamicio.schema.validations` does for the pandas engine. A validator
returns a `PolarsValidations.Pending`: the lazy queries of its result, and a function that makes a `ValidationResult` of their collected frames. The engine
collects the queries of all validators in one `pl.collect_all`:

<!-- doctest -->
```python
from dynamicio.schema.polars_validations import PolarsValidations
from dynamicio.schema.validations import ValidationResult


@PolarsValidations.validator
def is_even(dataset, frame, column):
    def finish(frames):
        odd = frames[0].item()
        return ValidationResult(valid=odd == 0, message=f"{odd} cell values of {dataset}[{column}] are odd", value=odd)

    return PolarsValidations.Pending((frame.select((pl.col(column) % 2 == 1).sum()),), finish)
```

## Metrics

`log_metrics_from_schema()` computes the metrics of all columns in one `select`, and logs them in the order of the schema. The `METRIC` lines are the
lines of the pandas engine. For an empty or an all-null column, `Min`, `Max`, `Mean`, `Std` and `Variance` give NaN.
`Counts` and `UniqueCounts` count the rows and the distinct values, so they give 0 for an empty column. `CountsPerLabel` does not count nulls. It logs a count of 0 for each category of an `Enum` that is not in the column, as a pandas `category` does. The metrics run after the validations, in a second query.

`PolarsMetrics.metric("Name")` registers a polars metric, as a `Metric` subclass does for the pandas engine. A polars metric is a function of the
column name that returns a polars expression with one value.

<!-- doctest -->
```python
from dynamicio.schema.polars_metrics import PolarsMetrics


@PolarsMetrics.metric("Median")
def median(column):
    return pl.col(column).median()
```
