# 0007. S3 I/O performance evidence behind the boto3 decision

- Status: Accepted (recorded retrospectively on 2026-11-11; measurements taken in October 2026 on releases v8.1.0-rc.7 to rc.9, PR #88)
- Supports: [ADR 0001](0001-direct-boto3-s3-io.md), [ADR 0002](0002-tolerate-missing-columns-in-parquet-prefix-reads.md)

## Context

ADR 0001 states that moving S3 I/O to direct boto3 must not be slower than `master`, but kept the numbers in a standalone benchmark page. The
page was a snapshot of one laptop-to-S3 session, not a living document, and it needed the PR history to be understood. Decisions that rest on a
measurement should keep the few figures that justified them; the full tables and scripts stay in PR #88 and its history.

## Decision

Keep the headline evidence here and do not maintain a benchmark page. Anyone revisiting S3 performance should re-measure in their own environment
and add a new ADR if the conclusion changes. The harness was never versioned (it needs a dev bucket and credentials).

## Evidence

Baseline: `master`'s S3 I/O (release v4.4.1). Laptop to S3 over WAN, 12 CPUs, medians; uploads are uplink-bound.

- Single-file reads (300 files, 10 processes): 13.3 s to 5.5 s (2.4x). Writes (200 files): 36.7 s to 28.5 s (1.3x). At every fixed worker count (2 to
  10) the new path was faster and used less CPU per call, so no extra pod CPU is needed; it is network-bound.
- Why: one S3 request per file instead of two, one shared session and TLS connection per process, no temp file on reads.
- Prefix sync at equal worker count (10) is on par with the aws-cli sync it replaced (for example 1000 files down: 14.4 s both). The 32-worker default
  adds a modest download gain (13.3 s) and nothing on uploads (noisy, 64 to 86 s for 500 files); no upload gain is claimed.
- Parquet prefixes parsed in memory on the download threads: about 15% faster on ~1000 files.
- Real-data prefix of 3.7 million rows with `no_disk_space: true`: 654 s and 2.9 GB peak memory on `master`, 46 s and 1.5 GB after routing it through the
  in-memory pool. The default prefix read of the same data failed on `master` (`ArrowInvalid: No match for FieldRef.Name(...)`, see ADR 0002) and took 45 s.
- A single 1.8 GB object (32.8 million rows, one column) reads in the same time as `master` (about 65 s) with lower peak memory (7.5 GB against 8.5 GB).
- The ADR 0002 fallback (re-read an object with the columns it has) costs one extra footer read per affected object; pyarrow rejects an unknown column
  before decoding data (about 0.1 ms on a 92 MB object).

## Consequences

- No benchmark page to keep in step with the code; the numbers above are dated evidence, not a guarantee.
- Not implemented and behaviour-changing, so out of scope: fewer or larger files, compression choice, skipping unchanged files in sync, prefetching the
  next inputs. Small non-breaking candidates: botocore `standard` retry mode, avoiding a bytes-to-`BytesIO` copy.
