# 0009. Pandera models are the only schema declaration

- Status: Accepted (dynamicio 11.0.0); supersedes parts of [0005](0005-pandera-schema-declaration.md)
- Date: 2026-11-13

## Context

ADR 0005 added Pandera models next to the two older declarations (YAML files and dicts on the IO class) and kept all three working in v10.
That left three ways to say the same thing, with contracts pinned three times, and exposed rough edges in the new one:

- the dataset name needed a `class Config: name = "..."` block in every model, which is noise when the name is the class name in snake case;
- the Postgres table name was that same name, so a label used in logs doubled as the identifier of a database object, and a wrong label failed with "table does not exist";
- dynamicio's metrics and metadata-only validations went through `metadata={"dynamicio": {"metrics": ["Min"]}}`, strings in a nested dict that an editor cannot complete and a typo cannot fail early.

## Decision

1. A Pandera `DataFrameModel` is the only schema declaration. `schema = {...}`, `schema_validations`, `schema_metrics`, `SCHEMA_FROM_FILE`, YAML schema files and the `schema:` block of resource definitions are removed (`SafeDynamicSchemaLoader` with them). `SCHEMA_FROM_ARGUMENT` and the `schema=` argument stay. Each removal fails loudly, naming the replacement, rather than being ignored.
2. The model names its dataset and its table with two dunder class attributes, which Pandera allows next to columns (public non-field attributes raise):
   - `__dataset_name__`: the label of logs and `METRIC` lines. Resolution: `__dataset_name__`, then `Config.name` (still honoured; equal to the class name counts as unset), then the class name in snake case.
   - `__table_name__`: the exact name of the Postgres table. There is no fallback: a model used with a Postgres resource without it raises `TableNameNotFoundError` when the IO object is created.
   Two attributes rather than one, because the label may legitimately differ from the table (several models over one table, a label shared with an S3 dataset), and the table name has to be exact. Putting it in the resource definition was rejected: the model is the single source of truth, and a resource per environment would repeat it.
3. `metadata` is built with typed helpers from `dynamicio.schema`: `Metric` (an enum), `metrics(...)`, `null_fraction_below`, `exactly_values`, `strictly_between`, combined with `|`. Every validation that Pandera has no `Field` argument for has a helper; the raw dict is rejected with an error that names them. The internal validator names are unchanged, so error payloads and logs are too.
4. `dynamicio migrate` is the one place that still reads YAML schemas, and the supported path from v8. It emits `__dataset_name__` (and `__table_name__` for schemas that a resource definition binds to Postgres) and the helpers, and no longer a `Config`. Users on 9 or 10 follow the upgrade notes in the guide.

## Consequences

- Breaking: version 11. Beyond the removals, the default label of a model without `__dataset_name__` or `Config.name` is now the snake-case class name rather than the class name upper-cased (`FinalBar` logs `FINAL_BAR`, not `FINALBAR`), which changes `METRIC` lines for such models. Generated models drop `Config.name` but keep `strict = "filter"`, which preserves what v8 did with unlisted columns.
- One declaration to document, test and keep the contracts of. The contract tests that were parametrised over three styles run over one.
- Postgres table names are exact and explicit; the cost is one line per Postgres model, in exchange for no guessing.
- The next step, not done here, is the same treatment for resource definitions (YAML and Python front doors over one typed model).
