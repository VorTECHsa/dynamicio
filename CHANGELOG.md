# Changelog

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
