# The polars engine

The polars engine casts, validates and logs the metrics of polars frames. It has the validators and the metrics of the pandas engine, with the same
names, messages and `METRIC` lines. In this release it does not read or write: `read()`, `write()`, `scan()` and `sink()` raise `NotImplementedError`.

[ADR 0010](https://github.com/VorTECHsa/dynamicio/pull/140) records the design. The ADR is not final and changes with each release of the engine.

## Set the engine

An engine is a setting of the process. Set it before the first read or write:

```python
import dynamicio

dynamicio.configure(engine="polars")
```

The first read or write fixes the engine. After that, `dynamicio.configure()` with another engine raises `EngineConfigurationError`. The polars
engine needs polars 2.1 or later. Without polars, it raises `ImportError`.

## Declare a polars model

A polars model is a `dynamicio.schema.polars.DataModel`. It has the same `dataset` and `table` class keywords as a pandas model, and its columns
use the polars dtypes:

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

A model with a different dtype raises `ValueError` when it compiles. Examples are `Date`, `List` and `Decimal`. polars has no second unit, so a
pandas `datetime64[s]` has no pair.

The compiled schema of a polars model (`io.schema.columns[name].data_type`) holds the pandas dtype. A column takes the pandas dtype "without nulls"
of the table. To set a different pandas dtype, use `Field(pandas_dtype=...)`, for example `"Int64"` for a nullable integer or `"string[pyarrow]"` for a
string. The value must be a pair of the polars dtype of the column, else the model raises `ValueError` when it compiles. A pandas model does not accept
`pandas_dtype`, because its column type is a pandas dtype.

## Cast

`_apply_schema()` casts a frame to the polars dtypes of the model:

- A `DataFrame` casts one column at a time. A column that does not cast raises `ColumnsDataTypeError`, and the `dynamicio.core` logger names the column.
- A `LazyFrame` gets the casts in its plan. A column that does not cast raises `ColumnsDataTypeError` when the validations or the caller collect the frame.
- A string column parses to a `Datetime` with `str.to_datetime`, in the time unit and the time zone of the model.
- A float NaN becomes null. In pandas, NaN and None are the two missing values. In polars, null is the only missing value, so the validators of nulls
  count a NaN after the cast.
- A schema column that is not in the frame, or a `columns` option with a column that is not in the schema, raises `ColumnsDataTypeError`.
- With `show_casting_warnings=True`, each column that casts logs its found and its expected dtype.
- The engine ignores `strict_dtype_check`.

## Validations

`validate_from_schema()` runs these checks:

- the 9 validators of the pandas engine, for example `has_unique_values`, `is_in` and `is_between`
- the Pandera column checks that dynamicio has no validator for, for example `str_matches` or a custom `@pa.check`
- the dataframe-level checks of the model: `Config.unique`, `Config.ordered` and each `@pa.dataframe_check`

All of the checks run as lazy queries in one `pl.collect_all`, so the engine reads a `LazyFrame` one time. The failures raise one
`SchemaValidationError`, with one entry for each validation name, as on the pandas engine.

The messages of the column validators are the messages of the pandas engine. The dataframe-level checks have the keys and the message template of the
pandas engine (`multiple_fields_uniqueness`, `column_ordered`, the name of the check). Their counts and examples come from the polars failure cases, so
they are not always the pandas values. For a `@pa.dataframe_check`, the count is the number of failing rows.

`PolarsValidations.validator` registers a polars validator, as `@validator` of `dynamicio.schema.validations` does for the pandas engine.

## Metrics

`log_metrics_from_schema()` computes the metrics of all columns in one `select`, and logs them in the order of the schema. The `METRIC` lines are the
lines of the pandas engine. The metric of an empty or an all-null column is NaN. The metrics run after the validations, in a second query.

`PolarsMetrics.metric("Name")` registers a polars metric, as a `Metric` subclass does for the pandas engine. A polars metric is a function of the
column name that returns a polars expression with one value.
