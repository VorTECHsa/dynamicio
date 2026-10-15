# Changelog

## v8.1.0-rc.11

- fix: an `s3_file` parquet source whose `file_path` ends in `/` (a directory of parquet objects, e.g. vessel-state's `VISIT_STS_OUTCOME`) is read as a dataset again: all objects under it are concatenated in listing order, with the same missing-column tolerance as `s3_path_prefix`. Since rc.7 it failed with `NoSuchKey` because the single-file reader issued a `GetObject` on the `.../` key

## v8.1.0-rc.10

- fix: S3 CSV writes accept a caller-supplied `index` option (`TypeError: to_csv() got multiple values for keyword argument 'index'` since rc.1, hit by vessel-state's `IO(..., index=False)`), and default to `index=True` like `DataFrame.to_csv` and the s3fs writer on master, instead of the hard-coded `index=False` of the wrangler writer
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

- fix: stop forcing `convert_dates=False` on local JSON reads, letting pandas' own date auto-detection run unless the caller opts out explicitly (fixes datetime dtype regression for consumers relying on auto-parsed dates, e.g. Bon Voyage reference-data JSON)

## v8.1.0-rc.3

- fix: honor caller-supplied `orient`/`lines` for S3 and local JSON reads/writes instead of hardcoding `orient="records"` (fixes regression for consumers using `orient="index"`, e.g. Bon Voyage reference-data JSON)
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
