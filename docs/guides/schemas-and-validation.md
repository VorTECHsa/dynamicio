# Schemas and validation

## Step 4.1. Pandera models

[![Pandera](../images/pandera-banner.png){ width="240" }](https://pandera.readthedocs.io)

A schema is a [Pandera](https://pandera.readthedocs.io) `DataFrameModel` class, and that is the only way to declare one.
Resource definitions stay in YAML; only the schema lives in code. Pandera is the *declaration* layer: dynamicio still runs
the validations and emits the metrics.

```python
import pandera.pandas as pa
from pandera.typing import Series

from demo.src.config.environment import LOWER_THAN_LIMIT
from dynamicio.schema import Metric, metrics


class Foo(pa.DataFrameModel):
    column_a: Series[str] = pa.Field(nullable=True, unique=True, metadata=metrics(Metric.COUNTS))
    column_b: Series[str] = pa.Field(nullable=False, metadata=metrics(Metric.COUNTS_PER_LABEL))
    column_c: Series[float] = pa.Field(nullable=True, gt=1000)
    column_d: Series[float] = pa.Field(nullable=True, lt=LOWER_THAN_LIMIT, metadata=metrics(Metric.MIN, Metric.MAX, Metric.MEAN, Metric.STD, Metric.VARIANCE))
```

Use the model as a class attribute when the IO class serves one dataset (`schema = Foo`), or let one class serve many by
taking the model per instance:

```python
from dynamicio.core import SCHEMA_FROM_ARGUMENT


class InputIO(UnifiedIO):
    schema = SCHEMA_FROM_ARGUMENT


foo_df = InputIO(source_config=staging_input_config.get(source_key="FOO"), schema=Foo, apply_schema_validations=True, log_schema_metrics=True).read()
```

The `schema=` argument takes precedence over the class attribute; omitting it with `SCHEMA_FROM_ARGUMENT` raises `SchemaNotFoundError`.
Any other value of `schema` (a dict, `SCHEMA_FROM_FILE`) raises a `TypeError` when the class is defined.

### Naming the dataset

The dataset name is the label dynamicio puts in its logs and `METRIC` lines (upper-cased: `FOO`). It is resolved in this order:

1. `__dataset_name__ = "foo"` on the model;
2. `Config.name`, if the model sets one (Pandera's own option, still honoured; a name equal to the class name counts as unset);
3. the class name in snake case: `FinalBar` is logged as `FINAL_BAR`.

Most models need nothing. The attribute is a dunder, because Pandera reserves public attributes for columns:

```python
class StagedFooModel(pa.DataFrameModel):
    __dataset_name__ = "staged_foo"  # otherwise the label would be STAGED_FOO_MODEL
```

### Naming the Postgres table

A model used with a Postgres resource must say which table it is, with `__table_name__`. It is the **exact name of the
table in the database**, and dynamicio never guesses it from the dataset name:

```python
class Orders(pa.DataFrameModel):
    __table_name__ = "orders_v2"  # the table in the database; the dataset name (ORDERS) is only a label

    order_id: Series[int] = pa.Field(nullable=False, unique=True)
```

A Postgres resource whose model has no `__table_name__` raises `TableNameNotFoundError` when the IO object is created, and a read of a
table that does not exist logs which `__table_name__` to correct. Other backends ignore the attribute: their location is in the resource definition.

### Validations

How `pa.Field` arguments map onto dynamicio's validations:

| `pa.Field(...)` | dynamicio validation |
|---|---|
| `nullable=False` | `has_no_null_values` |
| `unique=True` | `has_unique_values` |
| `gt` / `ge` / `lt` / `le` | `is_greater_than` / `..._or_equal` / `is_lower_than` / `..._or_equal` |
| `in_range={"min_value", "max_value", "include_min", "include_max"}` | `is_between` |
| `isin=[...]` | `is_in` (the column values must be a subset of the list) |

Two traps when writing models by hand:

- **Always set `nullable=` explicitly.** Pandera's default is `nullable=False`, which adds `has_no_null_values`; dynamicio allowed nulls unless the validation was declared.
- **Always set `include_min` / `include_max` on `in_range`.** Pandera includes the bounds by default; dynamicio's `is_between` excludes them.

### Metrics and the remaining validations: typed metadata

Pandera gives each field one free-form `metadata` dict. dynamicio's part of it is declared with typed helpers, so an editor completes
the names and a typo fails when the module is imported. They combine with `|`:

```python
from dynamicio.schema import Metric, exactly_values, metrics, null_fraction_below, strictly_between

column_d: Series[float] = pa.Field(nullable=True, lt=1000, metadata=metrics(Metric.MIN, Metric.MAX) | null_fraction_below(0.2))
```

| Helper | Effect |
|---|---|
| `metrics(Metric.MIN, ...)` | logs these metrics for the column when `log_schema_metrics=True`: `MIN`, `MAX`, `MEAN`, `STD`, `VARIANCE`, `COUNTS`, `UNIQUE_COUNTS`, `COUNTS_PER_LABEL` |
| `null_fraction_below(0.2)` | fails if 20% or more of the column is null (a fraction strictly between 0 and 1; the validation is named `has_acceptable_percentage_of_nulls`) |
| `exactly_values([...])` | the column holds exactly these values: no others, none missing (`isin=` only forbids others) |
| `strictly_between(lower, upper)` | `is_between` with bounds that are not compared on definition, for limits known at import time (`include_lower=` / `include_upper=` include a bound) |

A raw `metadata={"dynamicio": {...}}` dict is rejected with an error that names the helpers.

The `columns` of a model are used to filter out undesired columns in an optimal manner. This happens on loading for `*.csv` and `*.parquet` files as well
as when interacting with a database, and after loading for `*.h5` and `*.json`. Their dtypes are then validated: if types do not match, `dynamic(i/o)`
attempts to cast them and issues a `WARNING`. If casting does not work either, it raises a `ColumnsDataTypeError`. Validations and metrics can be applied on loading or on writing out.

## Step 4.2. Use the dynamicio cli

The `dynamicio` cli generates a model module for you from a dataset (`json`, `parquet`, `hdf`, `csv`) or a directory of them:

```shell
usage: dynamicio [-h] (-b | -s) -p PATH -o OUTPUT

Generate a Pandera model (a .py module) for each dataset

  -b, --batch           generate multiple models provided a datasets directory.
  -s, --single          generate a model provided a single dataset.
  -p PATH, --path PATH  the path to the dataset/datasets-directory.
  -o OUTPUT, --output OUTPUT
                        the path to the output directory for the generated models.
```

The generated models carry the dtypes only: add validations, metrics and `__table_name__` yourself.

## Upgrading

### From dynamicio 8 (YAML schemas) to 11

Up to version 8, schemas were YAML files (`SCHEMA_FROM_FILE`, `schema:` blocks in resource definitions) or dicts. Version 11 reads neither,
and `dynamicio migrate` takes a project from there in one step:

```shell
dynamicio migrate src/resources/schemas -o src/resources/schemas --scan src
```

It converts every YAML schema under the path into Pandera models, one module per sub-directory of the YAML schemas (so `schemas/stage/*.yaml`
becomes `schemas/stage.py`); pass `-o schemas.py` for a single module. For each model it:

- takes the class name from the file name, and sets `__dataset_name__` to the schema's `name`, so the labels in your logs and metric lines do not change;
- also sets `__table_name__` to that name for every schema that a resource definition under `--scan` binds to a Postgres environment (the old schema name was the table name). Check it is the exact table name;
- writes the typed helpers for metrics and metadata-only validations, and always sets `nullable=` and the range bounds explicitly;
- checks that the model compiles back to a schema equivalent to its source.

It also writes `MIGRATION_PROMPT.md` next to the module, listing the models and every remaining call site (`SCHEMA_FROM_FILE`,
dict schemas, `schema:` blocks in resource definitions, raw metadata dicts) with file and line. Hand it to an LLM, or follow it yourself:
set `schema = <Model>` or `schema = SCHEMA_FROM_ARGUMENT` with `schema=<Model>` on construction, and delete the `schema:` blocks from the definitions.
Dynamic variables (`[[ NAME ]]`) in the YAML become plain names marked `# TODO` in the module, to be replaced by imports.
A YAML `object` column becomes `Series[object]`, because `object` meant "any values" (strings, lists, mixed) and dynamicio never looked inside. Where you know a column only
holds strings, change it to `Series[str]` by hand. Dict-style schemas are not converted automatically.

### From dynamicio 9 or 10

Version 11 removes dict schemas, `SCHEMA_FROM_FILE` and YAML schemas, which 9 and 10 still accepted next to Pandera models, and changes three things in models.
These are small enough to apply by hand, or to ask a coding assistant to apply (give it this page and the [changelog](https://github.com/VorTECHsa/dynamicio/blob/master/CHANGELOG.md)):

1. Replace `metadata={"dynamicio": {"metrics": [...], "validations": {...}}}` with the helpers above.
2. Replace `class Config: name = "x"` with `__dataset_name__ = "x"`, or delete it if `x` is the snake-case class name.
3. Add `__table_name__ = "<exact table name>"` to every model used with a Postgres resource.
4. Convert any remaining dict schema to a model, and delete `schema:` blocks from resource definitions.
