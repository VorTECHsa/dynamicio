# 0001. Direct boto3 for S3 I/O; drop awscli and s3fs

- Status: Accepted (v8.1.0-rc.7, PR #88)
- Date: 2026-10-08

## Context

- v4.4.1 read single S3 parquet files with boto3 `download_fileobj` (HEAD + GET, temp file), wrote with `df.to_parquet("s3://")` via s3fs, and synced prefixes with an in-process `awscli` `s3 sync`.
- `awscli` installed an `aws` console script into consumer venvs, shadowing the system CLI, and pinned heavy transitive dependencies.
- rc.1-rc.4 moved `WithS3File` to awswrangler. That removed the awscli dependency for files but was ~2.6x slower on reads: awswrangler builds a session, TLS connection and client per call, issues an extra `ListObjectsV2` for a `str` path, and does HEAD plus ranged GET.
- Constraint from the main consumer (vessel-state): adoption must be a library bump only, with no code or config changes and no slowdown, under a fixed pod CPU budget.

## Decision

1. One shared boto3 session and client per process (fork-safe, pool of 64).
2. Single-file parquet: `get_object` / `put_object` in memory, parsed and written with the same pandas/pyarrow options as local I/O. Objects over 16 MiB use multipart.
3. awswrangler is kept only as a fallback when a wrangler-only kwarg is passed, and for CSV/JSON.
4. `WithS3PathPrefix` syncs with a `ThreadPoolExecutor` (default concurrency 32), one request per file, no `HeadObject`.
5. Remove `awscli` and `s3fs` dependencies.

## Consequences

- Reads ~2.4x faster, writes ~1.3x faster than v4.4.1 (see `docs/perf-s3-io.md`); CPU per call is lower, so no extra pod CPU is needed.
- Prefix sync is on par with the aws-cli sync at equal worker count; the 32-worker default adds a modest gain on downloads and none on uplink-bound uploads.
- Breaking for anyone relying on the `aws` script or s3fs being installed transitively (`feat!`).
- Wrangler-only kwargs still work but take the slower path.
- Larger levers (fewer/larger files, compression, skip-unchanged sync, prefetch) change behaviour and are out of scope.
