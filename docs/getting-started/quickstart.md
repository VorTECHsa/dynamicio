# Quickstart

This page builds a complete read and write of one dataset. It uses a `LOCAL` resource, so it needs no cloud account.
Every code block on this page is run by the test suite, in order, in an empty directory.

## 1. Describe where the data lives

A resource definition maps a name to a location *per environment*. Write it to `orders.yaml`:

<!-- doctest -->
```python
import pathlib

pathlib.Path("orders.yaml").write_text(
    """
ORDERS:
  LOCAL:
    type: local
    local:
      file_path: "[[ DATA_DIR ]]/orders.parquet"
      file_type: parquet
"""
)
```

`[[ DATA_DIR ]]` is a dynamic variable. Its value comes from a Python object you pass as `dynamic_vars`:

<!-- doctest -->
```python
class Settings:
    DATA_DIR = "."
```

## 2. Describe what the data looks like

The schema is a Pandera `DataFrameModel`. Set `nullable=` explicitly (see
[Schemas and validation](../guides/schemas-and-validation.md)):

<!-- doctest -->
```python
import pandera.pandas as pa
from pandera.typing import Series


class Orders(pa.DataFrameModel):
    order_id: Series[int] = pa.Field(nullable=False, unique=True)
    amount: Series[float] = pa.Field(nullable=False, ge=0)
```

## 3. Bind them in an IO class

<!-- doctest -->
```python
from dynamicio import UnifiedIO
from dynamicio.config import IOConfig


class OrdersIO(UnifiedIO):
    schema = Orders


config = IOConfig("orders.yaml", env_identifier="LOCAL", dynamic_vars=Settings)
orders = OrdersIO(
    source_config=config.get(source_key="ORDERS"),
    apply_schema_validations=True,
    log_schema_metrics=True,
)
```

`apply_schema_validations` and `log_schema_metrics` are off by default; without them the schema only selects and casts
columns.

## 4. Write and read

<!-- doctest -->
```python
import pandas as pd

orders.write(pd.DataFrame({"order_id": [1, 2, 3], "amount": [9.5, 20.0, 3.25]}))

df = orders.read()
assert list(df.columns) == ["order_id", "amount"]
assert len(df) == 3
```

## 5. See a validation fail

Data that breaks the schema is rejected before it is written:

<!-- doctest -->
```python
from dynamicio.errors import SchemaValidationError

try:
    orders.write(pd.DataFrame({"order_id": [1, 1], "amount": [1.0, -1.0]}))
except SchemaValidationError as error:
    assert set(error.message) == {"has_unique_values", "is_greater_than_or_equal"}
```

`error.message` maps each failed validation to its details. See [Error handling](../guides/error-handling.md).

## Moving to the cloud

Add a `DEV` or `PROD` entry beside `LOCAL`, for example an `s3` block with `bucket`, `file_path` and `file_type`, and
construct `IOConfig` with a different `env_identifier` (usually read from an environment variable). The task code does
not change. Continue with [Resource definitions](../guides/resource-definitions.md).
