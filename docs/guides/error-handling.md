# Error handling

All the errors dynamicio defines live in `dynamicio.errors` and derive from `DynamicIOError`. `error.message` is the
first argument the error was raised with (`None` if there was none).

| Error | Raised when |
|---|---|
| `SchemaValidationError` | A validation failed on read or write. `message` is a dict of validation name to details, with every violation collected |
| `ColumnsDataTypeError` | A column cannot be cast to the dtype the schema declares |
| `SchemaNotFoundError` | The IO class has no schema (for example `SCHEMA_FROM_ARGUMENT` without a `schema=` argument) |
| `MissingSchemaDefinition` | The resource definition has no schema where one is required |
| `InvalidDatasetTypeError` | `file_type` is not one of `parquet`, `json`, `csv`, `h5` |
| `NoObjectsFoundError` | An S3 prefix (or a parquet `file_path` ending in `/`) holds no objects. It is both a `FileNotFoundError` and a `ValueError` |
| `ObjectNotFoundError` | The S3 key of a `csv` or `json` `file_path` does not exist. It is both a `FileNotFoundError` and an awswrangler `NoFilesFound` |

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

On a write, dynamicio keeps only the schema's columns, validates, logs metrics, then casts. On a read it casts, then
validates, then logs metrics.

## Missing sources

A source that may legitimately not exist yet, such as state written by a previous run, can be read with
`missing_ok=True` in the constructor of the IO class (see
[Resource definitions](resource-definitions.md#optional-sources-missing_ok)). `read()` then returns `None` instead of
raising `FileNotFoundError`. Because `NoObjectsFoundError` and `ObjectNotFoundError` are `FileNotFoundError`s, code can
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
