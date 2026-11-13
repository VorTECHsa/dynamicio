# Schemas and validation

## Step 4.1. Declare a model

[![Pandera](../images/pandera-banner.png){ width="240" }](https://pandera.readthedocs.io)

A schema is a `DataModel` class: a [Pandera](https://pandera.readthedocs.io) `DataFrameModel` whose class keywords say what the dataset is, and whose
columns are declared with dynamicio's `Field`. Resource definitions stay in YAML; only the schema lives in code. Pandera is the *declaration* layer:
dynamicio still runs the validations and emits the metrics.

```python
from pandera.typing import Series

from demo.src.config.environment import LOWER_THAN_LIMIT
from dynamicio.schema import DataModel, Field, Metric


class Foo(DataModel, dataset="foo", validate=True, log_metrics=True):
    column_a: Series[str] = Field(nullable=True, unique=True, metrics=[Metric.COUNTS])
    column_b: Series[str] = Field(nullable=False, metrics=[Metric.COUNTS_PER_LABEL])
    column_c: Series[float] = Field(nullable=True, gt=1000)
    column_d: Series[float] = Field(nullable=True, lt=LOWER_THAN_LIMIT, metrics=[Metric.MIN, Metric.MAX, Metric.MEAN, Metric.STD, Metric.VARIANCE])
```

Use the model as a class attribute when the IO class serves one dataset (`schema = Foo`), or let one class serve many by
taking the model per instance:

```python
from dynamicio.core import SCHEMA_FROM_ARGUMENT


class InputIO(UnifiedIO):
    schema = SCHEMA_FROM_ARGUMENT


foo_df = InputIO(source_config=staging_input_config.get(source_key="FOO"), schema=Foo).read()
```

The `schema=` argument takes precedence over the class attribute; omitting it with `SCHEMA_FROM_ARGUMENT` raises `SchemaNotFoundError`.
Any other value of `schema` (a dict, `SCHEMA_FROM_FILE`) raises a `TypeError` when the class is defined.

### Declaring the dataset

The class keywords of a `DataModel` are the whole declaration of the dataset, so the body holds only columns. `cmd+P` on the class shows them:

| Keyword | Meaning |
|---|---|
| `dataset="foo"` | **Required.** The label of the dataset in logs and `METRIC` lines (upper-cased: `FOO`). It is never guessed from the class name |
| `table="foo_v2"` | The **exact name of the table in the database**. Required for a model used with a Postgres resource, ignored by other backends |
| `validate=True` | Run the validations of the model on every read and write: the default of `apply_schema_validations` |
| `log_metrics=True` | Log the metrics of the model: the default of `log_schema_metrics` |
| `show_casting_warnings=True` | Log each column whose dtype had to be cast: the default of `show_casting_warnings` |
| `strict_dtype_check=True` | Inspect every row for mixed Python types in a column being cast (expensive): the default of `strict_dtype_check` |

```python
class Orders(DataModel, dataset="orders", table="orders_v2"):
    order_id: Series[int] = Field(nullable=False, unique=True)  # the dataset (ORDERS) is only a label; the table is orders_v2
```

- A model with no `dataset` raises `DatasetNameNotFoundError` when the IO object is created. A base model that only shares columns may leave it out; every model you
  read or write must name it. Names are never inherited, the switches are.
- A Postgres resource whose model has no `table` raises `TableNameNotFoundError`, and a read of a table that does not exist logs which `table` to correct.
- A switch is only a default. An argument of the IO call overrides it in either direction: `IO(config, schema=Orders, apply_schema_validations=False)`.
  Where neither says, the switch is off. A model used both ways keeps the switches off and the call sites pass them.

`missing_ok` and the reader and writer options (`orient`, `filters`, ...) stay on the resource definition and the IO call: they depend on the backend and the environment, not on the dataset.

A plain `pa.DataFrameModel` works too: declare `__dataset_name__`, `__table_name__`, `__validate__`, `__log_metrics__`, `__show_casting_warnings__` and `__strict_dtype_check__`
as class attributes. The dunders are what the class keywords set, because Pandera reserves public attributes for columns.

### Declaring columns

`Field` is `pa.Field` with dynamicio's metrics and validations as named, documented arguments; `cmd+P` inside it lists what is available:

| `Field(...)` | dynamicio validation |
|---|---|
| `nullable=` | **Required.** `False` is `has_no_null_values`. Pandera's own default is `False`, dynamicio allowed nulls, so the decision is always written down |
| `unique=True` | `has_unique_values` |
| `gt` / `ge` / `lt` / `le` | `is_greater_than` / `..._or_equal` / `is_lower_than` / `..._or_equal` |
| `isin=[...]` | `is_in`: the values must be a subset of the list |
| `exactly=[...]` | `is_in` with `match_all=False`: the values are exactly these, none other and none missing |
| `between=(lower, upper)` | `is_between`. The bounds are excluded unless `include_lower=True` / `include_upper=True`; they are not compared on definition, so they may come from settings |
| `max_null_fraction=0.2` | fails if 20% or more of the column is null (`has_acceptable_percentage_of_nulls`) |
| `metrics=[Metric.MIN, ...]` | logs these metrics for the column when metrics are logged: `MIN`, `MAX`, `MEAN`, `STD`, `VARIANCE`, `COUNTS`, `UNIQUE_COUNTS`, `COUNTS_PER_LABEL` |

Any other `pa.Field` argument (`alias`, `coerce`, `str_matches`, `description`, ...) passes through to Pandera. `metadata=` is refused: use the arguments above.
A raw `metadata={"dynamicio": {...}}` dict on a plain `pa.Field` is rejected with an error that names `Field`.

The `columns` of a model are used to filter out undesired columns in an optimal manner. This happens on loading for `*.csv` and `*.parquet` files as well
as when interacting with a database, and after loading for `*.h5` and `*.json`. Their dtypes are then validated: if types do not match, `dynamic(i/o)`
attempts to cast them and issues a `WARNING`. If casting does not work either, it raises a `ColumnsDataTypeError`. Validations and metrics can be applied on loading or on writing out.

### Plain Pandera models

A plain `pa.DataFrameModel` with `pa.Field(...)` keeps working. Its `metadata=` takes the typed helpers of `dynamicio.schema` (`metrics`, `null_fraction_below`,
`exactly_values`, `strictly_between`, combined with `|`), which `Field` uses underneath. Two traps apply when writing such models by hand: always set `nullable=` explicitly, and
always set `include_min` / `include_max` on `in_range` (Pandera includes the bounds, dynamicio's `is_between` excludes them).

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

The generated models carry the dtypes only: add validations, metrics and `table=` yourself.

## Upgrading

### From dynamicio 8 (YAML schemas) to 11

Up to version 8, schemas were YAML files (`SCHEMA_FROM_FILE`, `schema:` blocks in resource definitions) or dicts. Version 11 reads neither,
and `dynamicio migrate` takes a project from there in one step:

```shell
dynamicio migrate src/resources/schemas -o src/resources/schemas --scan src
```

It converts every YAML schema under the path into Pandera models, one module per sub-directory of the YAML schemas (so `schemas/stage/*.yaml`
becomes `schemas/stage.py`); pass `-o schemas.py` for a single module. For each model it:

- takes the class name from the file name, and sets `dataset=` to the schema's `name`, so the labels in your logs and metric lines do not change;
- also sets `table=` to that name for every schema that a resource definition under `--scan` binds to a Postgres environment (the old schema name was the table name). Check it is the exact table name;
- declares each column with `Field`, always with `nullable=` and, for a range, `between=` with its bounds;
- checks that the model compiles back to a schema equivalent to its source.

It also writes `MIGRATION_PROMPT.md` next to the module, listing the models and every remaining call site (`SCHEMA_FROM_FILE`,
dict schemas, `schema:` blocks in resource definitions, raw metadata dicts) with file and line. Hand it to an LLM, or follow it yourself:
set `schema = <Model>` or `schema = SCHEMA_FROM_ARGUMENT` with `schema=<Model>` on construction, and delete the `schema:` blocks from the definitions.
Dynamic variables (`[[ NAME ]]`) in the YAML become plain names marked `# TODO` in the module, to be replaced by imports.
A YAML `object` column becomes `Series[object]`, because `object` meant "any values" (strings, lists, mixed) and dynamicio never looked inside. Where you know a column only
holds strings, change it to `Series[str]` by hand. Dict-style schemas are not converted automatically.

### From dynamicio 9 or 10

Version 11 removes dict schemas, `SCHEMA_FROM_FILE` and YAML schemas, which 9 and 10 still accepted next to Pandera models, and changes how models are declared.
These are small enough to apply by hand, or to ask a coding assistant to apply (give it this page and the [changelog](https://github.com/VorTECHsa/dynamicio/blob/master/CHANGELOG.md)):

1. Base the model on `DataModel` (`from dynamicio.schema import DataModel, Field, Metric`) and declare its columns with `Field`: `metadata={"dynamicio": {...}}` becomes `metrics=[...]`, `max_null_fraction=`, `exactly=` and `between=`.
2. Name every dataset with `dataset="x"` in the class keywords (`class Config: name = "x"` is no longer read, and the class name is no longer a fallback).
3. Add `table="<exact table name>"` to every model used with a Postgres resource.
4. Move `apply_schema_validations=True, log_schema_metrics=True` from the call sites to the model with `validate=True, log_metrics=True` where every use of the model wants them.
5. Convert any remaining dict schema to a model, and delete `schema:` blocks from resource definitions.
