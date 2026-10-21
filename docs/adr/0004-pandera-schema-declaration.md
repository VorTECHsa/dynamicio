# 0004. Declare schemas as Pandera models; dynamicio keeps validation and metrics

- Status: Accepted (PR #107)
- Date: 2026-10-21

## Context

Schemas lived in YAML files referenced from resource definitions (or as dicts in code). That meant a second language for column
types and validations, no editor or type support, and copy-pasted files. Teams wanted schemas in Python. Downstream dashboards
parse `METRIC` log lines and consumers catch `SchemaValidationError`, so those contracts could not change.

## Decision

1. A schema may be a Pandera `DataFrameModel`. It is the **declaration only**: `dynamicio/pandera_schema.py` compiles it into the existing
   `DataframeSchema`, and dynamicio's own validators and metrics remain the execution engine. Error payloads, collected-not-fail-fast
   behaviour, metric names, logger, dataset naming and lifecycle order are therefore identical across YAML, dict and Pandera styles.
   Pandera's native checks were rejected as the engine: they fail per row with different shapes and would change the contracts.
2. Resource definitions stay in YAML (environments, variables, orchestration). Only schemas move.
3. `pandera` is a required dependency. `Field` arguments map onto dynamicio validations; anything else goes in
   `Field(metadata={"dynamicio": {"validations": ..., "metrics": ...}})`.
4. A per-instance `schema=` argument and `SCHEMA_FROM_ARGUMENT` placeholder let one IO class serve many datasets, as `SCHEMA_FROM_FILE` did.
5. `dynamicio migrate` converts YAML schemas to models, verifies parity and writes an LLM prompt for the call sites. Dict schemas are reported, not converted.
6. Behaviour is pinned first by `tests/test_contracts/`, parametrised over all three declaration styles.
7. Fixed on the way: when one validation failed on several columns only the last column's message survived; they are now joined with `; `.

## Consequences

- Gotchas, documented in the README: Pandera defaults to `nullable=False` (dynamicio allowed nulls) and `in_range` includes its bounds (`is_between` excludes them). The generator always emits both explicitly.
- Dataset names come from `Config.name` (upper-cased), so migrated models keep the YAML `name`, even when two files share one.
- Dynamic variables (`[[ NAME ]]`) in YAML schemas become ordinary Python names; the migrator leaves `# TODO` markers.
- Validators and metrics are pandas-only. The declaration layer ports to `pandera.polars` with the same `Field` arguments, but polars support needs a second execution backend behind a small interface (validate a frame, compute a metric). **Not implemented**; this ADR only records the seam.
- Breaking only in how schemas are declared when moving to models; existing YAML and dict schemas keep working in v10.
