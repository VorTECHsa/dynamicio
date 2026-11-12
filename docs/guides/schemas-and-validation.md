# Schemas and validation

## Step 4.1. Pandera models

[![Pandera](../images/pandera-banner.png){ width="240" }](https://pandera.readthedocs.io)

Schemas are declared in Python as [Pandera](https://pandera.readthedocs.io) `DataFrameModel` classes. Resource definitions
stay in YAML; only the schema moves into code. Pandera is the *declaration* layer: dynamicio still runs the validations and
emits the metrics, so error payloads and `METRIC` log lines are identical to the YAML schemas'.

```python
import pandera.pandas as pa
from pandera.typing import Series

from demo.src.config.environment import LOWER_THAN_LIMIT


class Foo(pa.DataFrameModel):
    column_a: Series[str] = pa.Field(nullable=True, unique=True, metadata={"dynamicio": {"metrics": ["Counts"]}})
    column_b: Series[str] = pa.Field(nullable=False, metadata={"dynamicio": {"metrics": ["CountsPerLabel"]}})
    column_c: Series[float] = pa.Field(nullable=True, gt=1000)
    column_d: Series[float] = pa.Field(nullable=True, lt=LOWER_THAN_LIMIT, metadata={"dynamicio": {"metrics": ["Min", "Max", "Mean", "Std", "Variance"]}})

    class Config:
        name = "foo"  # the dataset name in metric lines is Config.name upper-cased; without it, the class name is used
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

How `pa.Field` arguments map onto dynamicio's validations:

| `pa.Field(...)` | dynamicio validation |
|---|---|
| `nullable=False` | `has_no_null_values` |
| `unique=True` | `has_unique_values` |
| `gt` / `ge` / `lt` / `le` | `is_greater_than` / `..._or_equal` / `is_lower_than` / `..._or_equal` |
| `in_range={"min_value", "max_value", "include_min", "include_max"}` | `is_between` |
| `isin=[...]` | `is_in` |
| `metadata={"dynamicio": {"validations": {...}, "metrics": [...]}}` | anything else, e.g. `has_acceptable_percentage_of_nulls`, and all metrics |

Two traps when writing models by hand:

- **Always set `nullable=` explicitly.** Pandera's default is `nullable=False`, which adds `has_no_null_values`; the YAML schemas allowed nulls unless the validation was declared.
- **Always set `include_min` / `include_max` on `in_range`.** Pandera includes the bounds by default; dynamicio's `is_between` excludes them.

YAML schemas (`SCHEMA_FROM_FILE`) and dict schemas keep working.

## Step 4.1b. `SCHEMA_FROM_FILE`

`from dynamicio.core import SCHEMA_FROM_FILE` is a unique dynamic(i/o) object used as a placeholder.
It is used to indicate that a schema is provided as part of a _resource definition_.

For example:

```yaml
---
FOO:
  LOCAL:
  ...
  DEV:
  ...
  schema:
    file_path: "[[ RESOURCES ]]/schemas/input/foo.yaml"
```

`foo.yaml` is effectively a schema definition and looks like this:

```yaml
---
name: foo
columns:
  column_a:
    type: "object"
    validations:
      has_unique_values:
        apply: true
        options: {}
    metrics:
      - Counts
  column_b:
    type: "object"
    validations:
      has_no_null_values:
        apply: true
        options: {}
    metrics:
      - CountsPerLabel
  column_c:
    type: float64
    validations:
      is_greater_than:
        apply: true
        options:
          threshold: 1000
    metrics: []
  column_d:
    type: float64
    validations:
      is_lower_than:
        apply: true
        options:
          threshold: 1000
    metrics:
      - Min
      - Max
      - Mean
      - Std
      - Variance
```

The file is quite self-explanatory. The format is:

`DataSet`:

- `Column`
  - `type`
  - `validations`
  - `metrics`

For a dataset, each of the desired columns are dictated here, along with their
designated `dtypes`. The `columns` are used to filter out undesired columns in an optimal manner.
This means that it will happen on loading for `*.csv` and `*.parquet` files as well as when
interacting with a database, but will happen post-loading in the case of `*.h5` or `*.json`.

`dtypes` are then used to validate the types of the columns. If types don't match, `dynamic(i/o)`
will attempt to cast them and will issue a `WARNING`. If casting does not work either, it will
raise a `ColumnsDataTypeError`.

`validations` and `metrics` are there to document the user's expectations of the quality of the dataset.
They can be automatically applied on loading or on writing out.

Specifically, you can use the following **validations**:

- `has_unique_values`  # no options
- `has_no_null_values`  # no options
- `has_acceptable_percentage_of_nulls`
- `is_in`:
  ```yaml
    validations:
      is_in:
        apply: true
        options:
          categorical_values:
            - class_a
            - class_b
            - class_c
          match_all: false # true by default, if false, then the column unique categoricals must be equal to the acceptable ones, else they must be a subset
  ```
- `is_greater_than`
  ```yaml
    validations:
      is_greater_than:
        apply: true
        options:
          threshold: 1000
  ```
- `is_greater_than_or_equal`  # same as `is_greater_than`
- `is_lower_than`  # same as `is_greater_than`
- `is_lower_than_or_equal`  # same as `is_greater_than`
- `is_between`  # same as `is_greater_than`
  ```yaml
    validations:
      is_between:
        apply: true
        options:
          lower: 0
          upper: 1000
          include_left: false
          include_right: true # false by default, so bounds are excluded unless you ask
  ```

and **metrics**:

- `Min`
- `Max`
- `Mean`
- `Std`
- `Variance`
- `Counts`
- `UniqueCounts`
- `CountsPerLabel`

imposed as per below:
```shell
  column_c:
    type: float64
    validations: {}
    metrics:
      - Min
      - Max
      - Mean
      - Std
      - ...
```

Note that you can also use dynamic fields to define validations, e.g. see `LOWER_THAN_LIMIT` in the file below: 
```yaml
---
name: bar
columns:
  column_a:
    type: "object"
    validations:
      has_unique_values:
        apply: true
        options: {}
    metrics:
      - Counts
  column_b:
    type: "object"
    validations:
      has_no_null_values:
        apply: true
        options: {}
    metrics:
      - CountsPerLabel
  column_c:
    type: float64
    validations:
      is_greater_than:
        apply: true
        options:
          threshold: 1000
    metrics: []
  column_d:
    type: float64
    validations:
      is_lower_than:
        apply: true
        options:
          threshold: "[[ LOWER_THAN_LIMIT ]]"
    metrics:
      - Min
      - Max
      - Mean
      - Std
      - Variance
```
Similar to resource definitions, this value needs to be defined in `config/environment.py`

## Step 4.2. Use the dynamicio cli
The `dynamicio` cli can be used to automatically generate schema definitions for you, provided either a path to
a dataset (`json`, `parquet`, `hdf`, `csv`) or to a directory. Here is how you can use it:

```shell
usage: dynamicio [-h] (--batch | --single) -p PATH -o OUTPUT

Generate dataset schemas

optional arguments:
  -h, --help            show this help message and exit
  --batch               used to generate multiple schemas provided a datasets directory.
  --single              used to generate a schema provided a single dataset.
  -p PATH, --path PATH  the path to the dataset/datasets-directory.
  -o OUTPUT, --output OUTPUT
                        the path to the schemas output directory.
```

The generated schema definitions will not have any validations or metrics automatically selected for you.

### Migrating YAML schemas to Pandera models

```shell
dynamicio migrate src/resources/schemas -o src/resources/schemas --scan src
```

This converts every YAML schema under the path into Pandera models, one module per sub-directory of the YAML schemas (so
`schemas/stage/*.yaml` becomes `schemas/stage.py`: the grouping you already had, by runner and input or output, carries over; this is the
recommended layout, with one models module per runner stage or definitions file rather than a single catch-all module). Pass `-o schemas.py` instead to get one module. Class names come from the file names, `Config.name` keeps the
schema's `name`, so metric lines do not change, and checks that each generated model compiles back to a schema equivalent to its source.
It also writes `MIGRATION_PROMPT.md` next to the module, listing the models and every remaining call site (`SCHEMA_FROM_FILE`,
dict schemas, `schema:` blocks in resource definitions) with file and line. Hand that prompt to an LLM, or follow it yourself:
set `schema = <Model>` or `schema = SCHEMA_FROM_ARGUMENT` with `schema=<Model>` on construction, and delete the `schema:` blocks from the
definitions. Dynamic variables (`[[ NAME ]]`) in the YAML become plain names marked `# TODO` in the module, to be replaced by imports.
A YAML `object` column becomes `Series[object]`, because `object` meant "any values" (strings, lists, mixed) and dynamicio never looked inside.
Where you know a column only holds strings, change it to `Series[str]` by hand (or `string[pyarrow]` for a smaller column, which gives `pd.NA`
instead of `NaN` for missing values). Dict-style schemas are not converted automatically. The `demo/` project was migrated this way.
