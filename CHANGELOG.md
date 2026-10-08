# Changelog

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
