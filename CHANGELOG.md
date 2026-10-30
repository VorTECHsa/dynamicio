# Changelog

## v9.2.0

- feat: a single-file S3 parquet read with `filters` reads only the row groups whose footer statistics can match, over ranged `GetObject` requests through `pyarrow.fs.S3FileSystem` (one for each process and bucket). The rows, index and dtypes are the same as before. Reads without `filters` or with `filters=[]`, fastparquet reads, reads with a non-pyarrow `filesystem`, prefix reads and reads with wrangler-only kwargs do not change. On the ranged read, a missing key or a permission problem raises pyarrow `FileNotFoundError` or `OSError` in place of botocore `ClientError`, and a custom S3 endpoint of boto3 (`AWS_ENDPOINT_URL`) does not apply. See ADR 0004
- feat: new postgres write option `create_table` (default `False`). When `True`, the writer creates a missing table from the schema columns, with no primary key and with unbounded string columns, in the same transaction as the write. The postgres writer now lists its options (`truncate_and_append`, `create_table`), so it drops other options with a warning, as the other writers do. A `create_table` value that is not a bool (for example the YAML string `"false"`) raises `TypeError`
- fix: the cache of pyarrow S3 file systems is cleared at exit, before the pyarrow S3 finalizer runs. With pyarrow 24 and later, a process that read with `filters` on a pod with web identity credentials did not exit (https://github.com/apache/arrow/issues/50188)
- chore: dev dependency `moto[server]>=5` for the tests of the ranged read

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
