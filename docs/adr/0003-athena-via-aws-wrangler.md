# 0003. Read Athena through AWS Wrangler; drop PyAthena and `s3_staging_dir`

- Status: Accepted (branch `rnd-13624-athena`, PR #89)
- Date: 2026-10-19

## Context

`WithAthena` used PyAthena. S3 I/O already runs on boto3/awswrangler, so PyAthena was a second AWS client stack to install, authenticate and keep compatible.

## Decision

1. `WithAthena` reads with `awswrangler.athena.read_sql_query`, using the process-wide shared boto3 session (a dedicated session only when `region_name` is set).
2. `ctas_approach` defaults to `False`. wrangler's own default (CTAS) lower-cases column names and creates a temporary Glue table; PyAthena kept the case of the `SELECT`, so `False` keeps results and schema keys unchanged. Callers can opt in per read.
3. The resource key `s3_staging_dir` is removed (not aliased) in favour of `s3_output`, and `database` and `workgroup` are new optional keys. A definition that still uses `s3_staging_dir` fails to load with a message naming the rename. No consumer used `type: athena`, so the cost of breaking is small, and an explicit error is clearer than a silent alias that would live forever.
4. Writes stay unsupported (`NotImplementedError`).

## Consequences

- Breaking change: needs a major version (9.0).
- wrangler returns nullable dtypes (`Int64`, `string`, `boolean`) and Python `date` objects; with a declared schema dynamicio casts to the declared dtypes, which masks the difference.
- Verified against a real Athena workspace: same rows, order and values as PyAthena once a schema is applied.
