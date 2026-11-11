# 0008. Resource location, ranged reads for `columns`, and parquet footer `file_metadata`

- Status: Accepted
- Date: 2026-11-11
- Amends: [ADR 0004](0004-ranged-parquet-reads-with-filters.md) (the condition for a ranged read)

## Context

A consumer pipeline merges about 200 parquet part files per table inside a fixed memory budget. It streams them as arrow tables, reads
their footers without the data, reads one column of a large file, and writes a build time in the footer. dynamicio returns a whole pandas
DataFrame for each read and write, so the consumer kept this I/O in its own pyarrow code. That code reached into `config.s3.bucket` and
`config.local.file_path`, kept a second pyarrow S3 file system cache with its own exit workaround, and copied the dtype cast of a dynamicio
write by hand.

Three gaps, none of which needs a streaming or arrow-table API, let that code resolve its files through dynamicio and drop the copies:

1. there is no public way to get the file system and path of a resource;
2. a single-file read downloads the whole object unless `filters` is set, even when the caller asks for one column;
3. a parquet write cannot add key-value pairs to the footer.

## Decision

1. `ResourceLocation.of(source_config, write=False, **template)` returns `(pyarrow file system, path)` for a `local` or `s3_file` resource.
   For S3 it uses dynamicio's per-process, per-bucket file system (the one ADR 0004 clears at exit); `write=True` selects a second entry
   with `default_metadata={"ACL": "bucket-owner-full-control"}`. A local write location creates the parent directory.
2. A single-file S3 parquet read takes the ranged path of ADR 0004 when the caller passes a non-empty `filters` **or** `columns`. The
   `columns` that `WithLocal._read_parquet_file` fills from the schema are added after this check and do not trigger it, so reads that pass
   neither option are byte-for-byte as before.
3. `file_metadata` (a mapping) is a parquet write option. The table is built as `DataFrame.to_parquet` builds it, the pairs are merged into
   the schema metadata, and `pyarrow.parquet.write_table` writes it. Without the option nothing changes. It is rejected with fastparquet and
   with awswrangler-only options, instead of being dropped with a warning.

## Consequences

- The three additions are opt-in; the rows, index and dtypes of every existing read and write are unchanged. Tests compare the ranged `columns`
  read with the whole-object read, over a local file system and a moto server, and write through a CLOUD resource into an in-memory store.
- A read that passes `columns` now sends HEAD and footer requests first, and its errors come from pyarrow (`FileNotFoundError`, `OSError`) as
  with `filters`. A caller that catches botocore's `ClientError` around a `columns` read must catch these too. A read of most of the columns of
  a small object is slower than one `GetObject`; callers that do not pass `columns` do not pay this.
- The consumer keeps the pyarrow footer reads, table streaming and the merge logic. dynamicio only supplies the location. A streaming or
  arrow-table read stays out of scope until the polars engine of ADR 0005 shapes it.
- `ResourceLocation` has no pandas in its contract, so it stays valid when a polars backend is added.
- `file_metadata` with a pyarrow write accepts the `write_table` options plus `index` and `schema`; `partition_cols` and `storage_options` are not
  supported with it.
