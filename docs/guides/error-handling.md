# Error handling

All the errors dynamicio defines live in `dynamicio.errors` and derive from `DynamicIOError`. `error.message` is the
first argument the error was raised with (`None` if there was none).

| Error | Raised when |
|---|---|
| `SchemaValidationError` | A validation failed on read or write. `message` is a dict of validation name to details, with every violation collected |
| `ColumnsDataTypeError` | A column cannot be cast to the dtype the schema declares, or a schema column is not in the frame. Up to 11.x, the pandas engine raised `KeyError` for a missing column. A CSV or parquet read without the column still raises the error of its reader |
| `SchemaNotFoundError` | The IO class has no schema (for example `SCHEMA_FROM_ARGUMENT` without a `schema=` argument) |
| `InvalidDatasetTypeError` | The `dynamicio` CLI gets a dataset file with an extension other than `json`, `parquet`, `h5` or `csv` |
| `DatasetNameNotFoundError` | A model has no dataset name. Declare it with `class Orders(DataModel, dataset="orders")` |
| `TableNameNotFoundError` | A model of a `postgres` resource has no table name. Declare it with `table="..."` in the class keywords |
| `EngineResourceError` | The polars engine cannot read or write a resource, for example an `hdf` file or an option with no polars entry. A Kafka resource without `key_column` also raises it. See [The polars engine](polars-engine.md#options) |
| `NoObjectsFoundError` | An S3 prefix (or a parquet `file_path` ending in `/`) holds no objects, or no file matches a `local_batch` source. It is both a `FileNotFoundError` and a `ValueError` |
| `ObjectNotFoundError` | The S3 key of a `csv` or `json` `file_path` does not exist. It is both a `FileNotFoundError` and an awswrangler `NoFilesFound` |
| `S3KeyNotFoundError` | The S3 key of a `parquet` or `hdf` `file_path` does not exist. It is both a `FileNotFoundError` and the botocore `ClientError` that S3 returned |
| `SchemaNotDeclaredError` | An IO class does not set `schema`. It is both a `TypeError` and an `AssertionError` |
| `MissingMixinError` | An IO class has no mixin that reads or writes the type of its resource. It is both a `TypeError` and an `AssertionError` |
| `InvalidS3UrlError` | An S3 operation gets a URL that does not start with `s3://`. It is both a `ValueError` and an `AssertionError` |
| `EngineConfigurationError` | `dynamicio.configure()` gets an engine that dynamicio does not have, or another engine after the first read or write. It is also a `ValueError` |
| `ModelEngineMismatchError` | The Pandera model of an IO object is not for the engine of the process, for example a `pandera.polars` model on the pandas engine. It is also a `TypeError` and a `ValueError` |
| `WrongFrameError` | `write()` or `sink()` gets a frame of the other engine, for example a polars frame on the pandas engine. It is also a `TypeError` |

The full list, with docstrings, is in the [API reference](../reference/index.md#errors).

## Validation failures

Catch `SchemaValidationError` and inspect `message`:

```python
from dynamicio.errors import SchemaValidationError

try:
    orders.write(df)
except SchemaValidationError as error:
    for validation, details in error.message.items():
        print(validation, details)
```

On a write, dynamicio keeps only the schema's columns, casts, then validates, then logs metrics. On a read it casts, then
validates, then logs metrics. Up to 11.x, a pandas write validated and logged the metrics before the cast.

## Missing sources

A source that may legitimately not exist yet, such as state written by a previous run, can be read with
`missing_ok=True` in the constructor of the IO class (see
[Resource definitions](resource-definitions.md#optional-sources-missing_ok)). `read()` then returns `None` instead of
raising `FileNotFoundError`. Because `NoObjectsFoundError`, `ObjectNotFoundError` and `S3KeyNotFoundError` are `FileNotFoundError`s, code can
also treat any missing source uniformly:

```python
try:
    df = orders.read()
except FileNotFoundError:
    df = None
```

## Options that are not accepted

An option in a resource definition that the reader or writer does not accept is dropped with a logged warning. If an
option seems to have no effect, check the logs of the run.
