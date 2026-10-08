# S3 I/O performance: v4.4.1 vs v8.1.0-rc.7

Decision record: [ADR 0001](adr/0001-direct-boto3-s3-io.md). This replaces the earlier awswrangler-vs-awscli report (rc.1-rc.4 numbers are obsolete).

## What changed

| Path | v4.4.1 (vessel-state today) | rc.7 |
|---|---|---|
| Single-file parquet read | boto3 `download_fileobj` (HEAD + GET) to temp file, then pandas | one `GetObject`, parsed in memory |
| Single-file parquet write | `df.to_parquet("s3://")` via s3fs | pyarrow to `BytesIO`, one `PutObject` |
| Prefix read / write | in-process `aws s3 sync` (10 workers) | boto3 thread pool, 32 workers, no HEAD |
| awswrangler | not used | fallback only (wrangler-only kwargs), plus CSV/JSON |
| `awscli`, `s3fs` | dependencies | removed |

Why faster: one S3 request per file instead of two, one shared session/TLS connection per process instead of per call, no temp file on reads.

## Results

Laptop to S3 (dev bucket) over WAN, 12 CPUs, medians. Uploads are uplink-bound (~3.7 MB/s). Pod numbers may differ; validate in the dev pod.

### Single-file I/O, 10 processes (vessel-state `run_parallel` shape)

| Scenario | v4.4.1 | rc.7 | Speedup |
|---|---|---|---|
| Read, 300 files | 13.3 s | 5.5 s | 2.4x |
| Write, 200 files | 36.7 s | 28.5 s | 1.3x |

### Fixed worker budget (apples to apples, 200 files; wall s / CPU-s)

| Workers | Read v4.4.1 | Read rc.7 | Write v4.4.1 | Write rc.7 |
|---|---|---|---|---|
| 2 | 37.8 / 7.1 | 8.8 / 2.2 | 119.7 / 10.1 | 47.6 / 5.5 |
| 4 | 18.8 / 4.5 | 5.4 / 1.7 | 68.0 / 8.3 | 30.6 / 4.5 |
| 8 | 10.4 / 4.3 | 3.7 / 1.6 | not run | not run |
| 10 | 8.5 / 3.9 | 3.7 / 1.5 | not run | not run |

rc.7 is faster at every worker count and uses less CPU per call: no extra pod CPU is needed. It is network-bound, not CPU-bound. (Write runs at 8 and 10 workers were aborted by an expired SSO session.)

### Prefix sync at equal worker count (10), wall s

| Files | Direction | aws-cli sync (v4.4.1) | rc.7 at 10 | rc.7 at 32 (default) |
|---|---|---|---|---|
| 200 | down | 3.4 | 3.2 | 2.9 |
| 500 | down | 7.6 | 7.1 | 6.6 |
| 1000 | down | 14.4 | 14.4 | 13.3 |
| 200 | up | 26.3 | 25.5 | 25.2 |
| 500 | up | 65.3 | 67.9 | 75.1 |

Prefix sync is on par with the aws-cli sync it replaces; earlier larger gains came mostly from more workers. Uploads are uplink-bound and noisy (500-file runs at 32 workers ranged 64-86 s), so no upload gain is claimed.

## Further options (not implemented; behaviour-changing)

Fewer/larger files, compression choice, skipping unchanged files in sync, prefetching next inputs. Small non-breaking candidates: botocore `standard` retry mode, avoiding the bytes-to-`BytesIO` copy.

## Reproducing

The harness (bench, profile, deck builder) is kept locally under `docs/.temp/perf/` and is not versioned; it needs a dev bucket and credentials.
