# 0002. Tolerate missing columns in parquet prefix reads

- Status: Accepted (branch `rnd-13624`, PR #88)
- Date: 2026-10-14

## Context

A downstream pipeline reads `s3_path_prefix` parquet prefixes with `no_disk_space: true`. Some objects there predate columns added to the schema years ago (`model_action`). On the branch the read failed with `pyarrow.lib.ArrowInvalid: No match for FieldRef.Name(model_action) in id: string`.

How `master` behaves:

- `no_disk_space: true`, parquet: `pd.read_parquet("s3://bucket/prefix/", columns=[...])` reads the prefix as one pyarrow dataset. The dataset schema comes from the first listed object, so an old object lacking a column is nulled **only if the first listed object has it**. If the first one is old, the same `ArrowInvalid` is raised. Tolerance was an accident of listing order.
- Default path: objects are synced to a temp dir and read one by one with `columns=[...]`, so any object lacking a column fails.

The branch reads every object separately (in memory), which made the order-dependent behaviour into always-fail. For that pipeline that is a regression.

## Decision

For parquet `WithS3PathPrefix` reads (both paths):

1. Read each object with the schema columns. If pyarrow raises `ArrowInvalid`, read the object's schema, and if requested columns are absent, re-read it with the columns it has. Any other error is re-raised.
2. Concatenate; absent columns become nulls (`None` in object columns, as a pyarrow dataset read yields) and the schema column order is restored. Rows come back in S3 listing order, also on the `no_disk_space` path, matching `master`.
3. Log one warning with the per-column count of objects that lacked the column.
4. Raise `ValueError` if a requested column is missing from every object.

Not changed: single-file reads, `local_batch`, CSV (`usecols` stays strict, as on `master`), JSON and HDF (already keep only the schema columns that exist, silently).

## Consequences

- Deterministic regardless of listing order; strictly more tolerant than `master`, including on the default path where `master` failed.
- It can hide a data-quality problem (an object that lost a column). Mitigated by the warning with counts and by failing when no object has the column; consumers that must be strict should validate with schema validations on the column.
- Non-nullable dtypes (`int64`) cannot hold the nulls and still fail at the schema cast, which is correct.
- Cost: no overhead when every object has all columns (the check runs only on failure). When it triggers, the failed attempt is rejected at column projection before any decoding (about 0.1 ms on a 90 MB object), plus a footer read; the re-read costs the same as a normal read.
- Parity gap: `local_batch` (local environment) is strict, so a local sample containing a stale object would fail where S3 would not. Sample data should mirror current schemas.
- Verified against dev S3 with `master` (pandas 2.3.3, pyarrow 25.0.1) on the same objects: wherever `master` succeeds, rc.8 returns an identical frame (values, dtypes, row order, `None` fills); where `master` raised (old object listed first, or the default path), rc.8 returns the data.
