# Architecture Decision Records

Short records of significant design decisions: context, decision, consequences.
Number sequentially, never rewrite an accepted ADR (supersede it with a new one).

| # | Title | Status |
|---|-------|--------|
| [0001](0001-direct-boto3-s3-io.md) | Direct boto3 for S3 I/O; drop awscli and s3fs | Accepted |
| [0002](0002-tolerate-missing-columns-in-parquet-prefix-reads.md) | Tolerate missing columns in parquet prefix reads | Accepted |
| [0003](0003-athena-via-aws-wrangler.md) | Read Athena through AWS Wrangler; drop PyAthena and `s3_staging_dir` | Accepted |
