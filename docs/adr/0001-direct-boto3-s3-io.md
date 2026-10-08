# 0001. Direct boto3 for S3 I/O; drop awscli and s3fs

- Status: Accepted (branch `rnd-13624`, PR #88)
- Date: 2026-10-08

## Context

How `master` does S3 I/O today:

- Single-file reads: boto3 `download_fileobj` to a temp file (`HeadObject` + `GetObject`), then pandas.
- Single-file parquet writes: `df.to_parquet("s3://...")` through `s3fs` (pinned `==0.4.2`, which also holds back `fsspec`).
- `WithS3PathPrefix` read/write: in-process `awscli` `s3 sync` (10 workers).
- `awscli` as a library dependency installs an `aws` console script into every environment that installs dynamicio, shadowing the system AWS CLI (e.g. `aws sso login` runs the wrong binary).

The branch first moved `WithS3File` to awswrangler (rc.1-rc.4). That removed `awscli` for files but made single-file reads ~2.6x slower than `master`: awswrangler builds a session, TLS connection and client per call, issues an extra `ListObjectsV2` for a `str` path, and does `HeadObject` plus a ranged `GetObject`.

Constraint: promoting the branch must not change dynamicio behaviour for existing consumers (same YAML, kwargs and dtypes) and must not be slower. Consumers call S3 I/O many times per run from forked process pools with a fixed CPU budget, so per-call overhead and CPU per call matter.

## Decision

1. One shared boto3 session and client per process (fork-safe, pool of 64).
2. Single-file parquet: `get_object` / `put_object` in memory, parsed and written with the same pandas/pyarrow options as local I/O. Objects over 16 MiB use multipart.
3. awswrangler stays only as a fallback when a wrangler-only kwarg is passed, and for CSV/JSON.
4. `WithS3PathPrefix` syncs with a `ThreadPoolExecutor` (default concurrency 32), one request per file, no `HeadObject`.
5. Remove the `awscli` and `s3fs` dependencies.

## Consequences

- Versus `master`: single-file reads ~2.4x faster, writes ~1.3x faster, lower CPU per call (see [perf-s3-io.md](../perf-s3-io.md)).
- Prefix sync is on par with `master`'s aws-cli sync at equal worker count; the 32-worker default adds a modest download gain and nothing on uplink-bound uploads.
- Breaking (`feat!`) for anything relying on the `aws` script or s3fs arriving transitively through dynamicio.
- Wrangler-only kwargs still work but take the slower path.
- `aws s3 sync` skip-unchanged behaviour is not replicated.
- Larger levers (fewer/larger files, compression, skip-unchanged sync, prefetch) change behaviour and are out of scope.
