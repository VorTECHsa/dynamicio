# Changelog

## Unreleased (v10.0.0)

- test: `tests/test_contracts/` pins the validation, metric, lifecycle-order and dtype behaviour that the schema work must preserve, for every schema declaration style
- feat: schemas can be declared as Pandera `DataFrameModel` classes (`class MyIO(UnifiedIO): schema = MyModel`). Dynamicio's validators and metric logging are unchanged, so error payloads and `METRIC` log lines are identical to the YAML and dict styles. `pandera` is now a required dependency. Anything Pandera cannot express natively goes in `Field(metadata={"dynamicio": {"validations": {...}, "metrics": [...]}})`. Note Pandera's default is `nullable=False`; dynamicio's YAML allowed nulls unless `has_no_null_values` was declared
- fix: when the same validation fails on several columns, `SchemaValidationError.message` now holds every column's message (joined with `; ` under the one validation-name key). Previously only the last column's message survived- feat: `schema=` constructor argument and `SCHEMA_FROM_ARGUMENT` placeholder, so one IO class can take a different Pandera model per instance (the Pandera counterpart of `SCHEMA_FROM_FILE`). The argument takes precedence over the class attribute
- feat: `dynamicio migrate <schemas dir> -o schemas.py` converts YAML schemas to Pandera models (dataset names, validations and metrics preserved, verified by a parity check) and writes `MIGRATION_PROMPT.md` listing the call sites to update by hand or with an LLM. The existing `-b/-s` schema generator is unchanged
- docs: README Step 4.1 (Pandera models) and migration guide, ADR 0004; the `demo/` project now declares its schemas as Pandera models and keeps its resource definitions, models and IO classes together under `demo/src/resources`, documented in the README as the recommended pipeline project structure

## v9.1.0

- chore(deps): dependency audit, ranges widened to what the code needs and CI proves. Removed packages the code never imports (`fsspec`, `libcst`, `logzero`, `no_implicit_optional`); dropped exact pins (`confluent-kafka>=2.4.0,<3`, `psycopg2-binary>=2.9.3,<3`, `python-json-logger>=2.0.1`, `simplejson>=3.17.2`); lifted the `numpy<2` ceiling (numpy 2.x passes the suite) and the `python<3.13` ceiling (3.13 and 3.14 pass; added to CI and classifiers). Raised floors so every allowed combination works with numpy 2 as well as numpy 1: `pandas>=2.2.2` (JSON writes with `index=False` for `records`/`values`, relied on since 8.1.1, need pandas 2.1, and pandas before 2.2.2 breaks with numpy 2), `pyarrow>=16.0.0`, `fastparquet>=2024.5.0`, `awswrangler>=3.9.0` (older ones cannot import against numpy 2 or pyarrow 16), `PyYAML>=6.0` (5.4.1 does not build with current toolchains). New CI job `dependency-ranges` tests the minimums (Python 3.10), the minimums with numpy 2 (Python 3.11) and the newest releases (Python 3.14); publishing waits for it
- fix: `python-json-logger` 3.1+ import path (`pythonjsonlogger.json`) with fallback to the old module, avoiding a `DeprecationWarning` on import

## v9.0.0

**Breaking:** Athena resources must rename `s3_staging_dir` to `s3_output` (see the first Athena entry below).

- docs: add Backstage `catalog-info.yaml` to enable TechDocs
- docs: README documents Athena I/O (resource options, `ctas_approach`, dtypes, `s3_staging_dir` migration); ADR 0003
- chore: migrate project and dependency management from Poetry to uv (PEP 621 `pyproject.toml`, `uv.lock`, `hatchling` + `hatch-vcs` build backend). The version is still derived from the git tag and the published wheel is unchanged apart from standard metadata. Contributors now use `make dev-env-setup` / `uv sync`; see `CONTRIBUTING.md` and `AGENTS.md`
- feat(RND-13624)!: `WithAthena` reads through AWS Data Wrangler (`wr.athena.read_sql_query`) instead of PyAthena, and the `pyathena` dependency is dropped. **Breaking (resource definitions):** the `s3_staging_dir` key of an `athena:` section is removed. Loading such a definition now fails with an error saying to rename it to `s3_output` (same value); nothing else changes in the YAML. `region_name` and `query` stay optional, and `database` (default `default`, Athena's own default) and `workgroup` are new optional keys. Reads use the shared per-process boto3 session unless `region_name` is set. Extra `options` are filtered to what `read_sql_query` accepts (e.g. `ctas_approach`, `unload_approach`, `athena_cache_settings`); `chunksize` is rejected with an error, because a chunked read returns an iterator rather than a DataFrame
- `ctas_approach` defaults to `False` (as PyAthena, which read the query's result file): column names keep the case used in the query (CTAS lower-cases them) and no temporary Glue table is created. Pass `ctas_approach: true` in `options` to opt in for large results
- note: results now come back with pandas nullable dtypes (`Int64`, `string`, `boolean`) and `date` as Python dates, as returned by awswrangler; declare the dtypes in the resource schema to cast them on read, as for any other source

## v8.1.1

- fix: S3 JSON reads and writes use pandas' defaults again: `lines=False` (a JSON array for `orient="records"`, as written by 7.x and earlier) unless the caller passes `lines`. 8.1.0 defaulted to JSON Lines for `orient="records"`, which could not read existing files and wrote files that 7.x readers cannot parse. Set `lines: true` in the resource `options` to opt in to JSON Lines
- fix: S3 JSON writes default `index` per orient as pandas does (`True` for `index`/`columns`/`split`/`table`, `False` for `records`/`values`) instead of always `False`, which made `orient="index"` and `orient="columns"` writes fail or drop the index

## v8.1.0-rc.11

- fix: an `s3_file` parquet source whose `file_path` ends in `/` (a directory of parquet objects, e.g. a downstream pipeline's outcome dataset) is read as a dataset again: all objects under it are concatenated in listing order, with the same missing-column tolerance as `s3_path_prefix`. Since rc.7 it failed with `NoSuchKey` because the single-file reader issued a `GetObject` on the `.../` key

## v8.1.0-rc.10

- fix: S3 CSV writes accept a caller-supplied `index` option (`TypeError: to_csv() got multiple values for keyword argument 'index'` since rc.1, hit by a downstream pipeline's `IO(..., index=False)`), and default to `index=True` like `DataFrame.to_csv` and the s3fs writer on master, instead of the hard-coded `index=False` of the wrangler writer
- fix: the awswrangler parquet fallback tolerates a caller-supplied `dataset` option instead of raising a duplicate-keyword `TypeError`

## v8.1.0-rc.9

- perf: parquet `no_disk_space` prefix reads use the same in-memory thread pool as the default path (~4x faster on a 3.7M-row prefix, 199s to 46s, lower peak memory); rows, order and dtypes unchanged
- fix: parquet prefix reads return rows in S3 listing order and null-filled object columns hold `None`, as `master`'s `no_disk_space` dataset read did (see rc.8 notes)

## v8.1.0-rc.8

- perf: parquet `WithS3PathPrefix` reads fetch and parse each object in memory on the download threads (no temp dir, parsing overlaps the network); ~15% faster on ~1000 files over WAN, same rows. Files are now concatenated in listing order instead of arbitrary `os.listdir` order
- fix: `WithS3PathPrefix` parquet reads no longer fail with `ArrowInvalid: No match for FieldRef.Name(<col>)` when some objects under the prefix predate a schema column. Such objects get nulls for that column (as master's `no_disk_space` dataset read did when the first listed file had the column), a warning lists the columns and how many objects lacked them, and a column missing from every object still raises `ValueError`
- fix: parquet `no_disk_space` prefix reads return rows in S3 listing order again, as `master` did (the branch had reversed it); null-filled object columns hold `None` like `master`'s dataset read, not `NaN`
- fix: `WithPostgres` logged `Started downloading table` twice per read (the query itself ran once); now logged once

## v8.1.0-rc.7

(rc.5 and rc.6 were tagged but never published: black/pylint/flake8 style checks failed. Contents are identical apart from formatting.)

- perf: reuse one boto3 session/client per process (fork-safe) instead of building a new one per awswrangler call
- perf: single-file S3 parquet reads/writes use one `GetObject`/`PutObject` over the shared client and parse in memory with the same pandas/pyarrow options as local I/O; awswrangler is only used when a wrangler-only option is passed (fixes the ~2.6x read slowdown seen in rc.1-rc.4)
- feat!: remove `awscli` and `s3fs` dependencies; `WithS3PathPrefix` now syncs with a boto3 thread pool (one request per file, concurrency 32) instead of the in-process `aws s3 sync`. No `aws` console script is installed into consumer venvs any more
- fix: S3 HDF writer honours `pickle_protocol`
- fix: S3 JSON reader supports `single_record`

## v8.1.0-rc.4

- fix: stop forcing `convert_dates=False` on local JSON reads, letting pandas' own date auto-detection run unless the caller opts out explicitly (fixes datetime dtype regression for consumers relying on auto-parsed dates, e.g. reference-data JSON)

## v8.1.0-rc.3

- fix: honor caller-supplied `orient`/`lines` for S3 and local JSON reads/writes instead of hardcoding `orient="records"` (fixes regression for consumers using `orient="index"`, e.g. reference-data JSON)
- fix: re-apply popped `orient`/`lines` options before `df.to_json` in the local JSON writer (previously silently discarded)

## v8.1.0-rc.2

- fix: stop forcing records-only orient on local JSON reads

## v8.1.0-rc.1

- feat(RND-13624): Migrate `WithS3File` to AWS Data Wrangler
- fix: address Copilot review findings on WithS3File wrangler migration
- fix: silence pre-existing pylint invalid-name warnings to unblock style-checks
- fix: satisfy black formatting and 90% coverage gate in CI
- fix: regenerate poetry.lock to match pyproject.toml
- docs: add awswrangler vs AWS CLI performance report, PR template, and skills
