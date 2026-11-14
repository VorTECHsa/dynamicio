# 0009. Pandera models are the only schema declaration

- Status: Accepted (dynamicio 11.0.0); supersedes parts of [0005](0005-pandera-schema-declaration.md)
- Date: 2026-11-13, revised 2026-11-14 after review of the release candidates

## Context

ADR 0005 added Pandera models next to the two older declarations (YAML files and dicts on the IO class) and kept all three working in v10.
That left three ways to say the same thing, with contracts pinned three times, and exposed rough edges in the new one:

- the dataset name needed a `class Config: name = "..."` block in every model, or fell back to the class name, so the label of logs and `METRIC` lines depended on how a class happened to be called;
- the Postgres table name was that same name, so a label used in logs doubled as the identifier of a database object, and a wrong label failed with "table does not exist";
- dynamicio's metrics and metadata-only validations went through `metadata={"dynamicio": {"metrics": ["Min"]}}`, strings in a nested dict that an editor cannot complete and a typo cannot fail early.

## Decision

1. A Pandera `DataFrameModel` is the only schema declaration. `schema = {...}`, `schema_validations`, `schema_metrics`, `SCHEMA_FROM_FILE`, YAML schema files and the `schema:` block
   of resource definitions are removed (`SafeDynamicSchemaLoader` with them). `SCHEMA_FROM_ARGUMENT` and the `schema=` argument stay. Each removal fails loudly, naming the
   replacement, rather than being ignored: an IO class that still sets `schema_validations` or `schema_metrics` next to a model is refused, and importing a removed name raises an
   `ImportError` that says what replaces it.
2. A model names its dataset and its table, and nothing else about its use. `DataModel` (a `DataFrameModel`) takes them as class keywords, so the body holds only columns:

    ```python
    class Orders(DataModel, dataset="orders", table="orders_v2"):
        order_id: Series[int] = Field(nullable=False, unique=True)
    ```

   - `dataset=` is the label of logs and `METRIC` lines. It is mandatory for a model used for I/O, never inherited (a subclass is another dataset) and never guessed:
     `Config.name` and the class name are not read.
   - `table=` is the exact name of the Postgres table, `schema.table` for one outside the default schema, quoted as Postgres needs it in every statement. There is no fallback:
     a model used with a Postgres resource without it raises `TableNameNotFoundError` when the IO object is created, unless the read passes its own `sql_query`.

   The keywords set the dunders `__dataset_name__` and `__table_name__`, which a plain `DataFrameModel` may set by hand (Pandera reserves public attributes for columns). Two names
   rather than one, because the label may legitimately differ from the table (several models over one table, a label shared with an S3 dataset), and the table name has to be
   exact. Putting the table in the resource definition was rejected: the model is the single source of truth, and a resource per environment would repeat it.
3. Columns are declared with `dynamicio.schema.Field`, the one public way to declare dynamicio's metrics and validations: `pa.Field` with them as named, documented arguments
   (`nullable=` required, `unique`, `gt`/`ge`/`lt`/`le`, `isin`, `exactly`, `max_null_fraction`, `metrics=[Metric.MIN, ...]`). It writes a typed, internal `DynamicioMetadata` into
   the field's `metadata`; the raw dict is rejected with an error that names `Field`. Each concept has one spelling: a range is its two bounds (`ge=1941, le=NEXT_YEAR`), which
   Pandera does not compare on definition, so they may come from settings; the `is_in` option that requires every value is `exact=True`.
4. Whether a read or write validates, logs metrics or checks dtypes stays an argument of the IO call (`apply_schema_validations=True`, ...), as in 10.x. Model-level defaults were
   tried in the release candidates and rejected: two call sites that look alike behaved differently depending on a keyword in another file, and in a pipeline that adopted them
   most call sites kept their arguments anyway, because one model is read with checks in one task and written without them in another.
5. `dynamicio migrate` is the one place that still reads YAML schemas, and the supported path from v8. It emits `DataModel` classes with `dataset=` (and `table=` for schemas that a
   resource definition binds to Postgres) and `Field`, and no `Config.name`. Users on 9 or 10 follow the upgrade notes in the guide.

## Consequences

- Breaking: version 11. Beyond the removals, every model names its dataset, so a model that relied on the class-name label must write it. A migrated YAML `is_between` becomes its
  two bounds, so `SchemaValidationError` reports `is_greater_than`/`is_lower_than` (or their `_or_equal` forms) for such a column, and code calling the `is_in` validator directly
  passes `exact` instead of `match_all`. Generated models drop `Config.name` but keep `strict = "filter"`, which preserves what v8 did with unlisted columns.
- One declaration to document, test and keep the contracts of, with one spelling per concept. The contract tests that were parametrised over three styles run over one, and the
  polars engine has one model API to support.
- Postgres table names are exact and explicit; the cost is one line per Postgres model, in exchange for no guessing.
- The next step, not done here, is the same treatment for resource definitions (YAML and Python front doors over one typed model).
