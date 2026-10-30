# 0004. Read filtered single-file parquet over S3 byte ranges

- Status: Accepted
- Date: 2026-10-30
- Amends: decision 2 of [ADR 0001](0001-direct-boto3-s3-io.md), for reads with `filters` only

## Context

[ADR 0001](0001-direct-boto3-s3-io.md) decision 2 reads a single parquet file with one `get_object` into memory. pandas then applies `filters` to the full object. A caller that needs a small part of a big file still downloads and decodes all of it.

A caller can need a small range of rows from a large file that is sorted on the filter column. To read less data, the caller must open the file with `pyarrow.fs.S3FileSystem` in its own code. This puts IO code outside dynamicio.

## Decision

1. When the caller gives non-empty `filters` for a single-file parquet read (and the engine is not fastparquet), open the object with `pyarrow.fs.S3FileSystem` and give the path and the file system to `pd.read_parquet`.
2. pyarrow reads the footer and skips the row groups whose statistics cannot match `filters`. Then it reads only the column chunks of the remaining row groups, over ranged `GetObject` requests.
3. Keep one `S3FileSystem` for each process and bucket, in the region of the bucket (`resolve_s3_region`). It uses the default AWS credential chain, as boto3 does.
4. A caller `filesystem` is used for the ranged read only if it is a `pyarrow.fs.FileSystem`. With another file system (for example fsspec), the object is downloaded, as before.
5. All other reads do not change: reads without `filters` or with `filters=[]`, fastparquet reads, prefix and directory reads, and reads with wrangler-only kwargs.

## Consequences

- The rows, index and dtypes are the same as the `get_object` read with the same `filters`. A test compares the two reads.
- On a test file of 72 row groups, a filtered read of one key range read 936 MB of the file in 29 ranged reads. Peak memory and time were the same as a hand-written pyarrow read of the same range. With a moto server, a read of 1 of 3 row groups sent 1.9 MB of a 5.5 MB object.
- Each filtered read sends HEAD requests and a footer read before the data reads. For a read of the full file, this is more requests than one `get_object`. Callers that do not give `filters` do not pay this cost.
- A forked child makes its own file system, because the cache key has the process id. A local test read in 8 forked children after a read in the parent.
- The pyarrow S3 client is C++. The in-process moto mock does not intercept it, so the tests use a moto server.
- A file with no useful row-group statistics (for example, not sorted on the filter column) gets no gain. pyarrow then reads all row groups over ranged requests.
- Errors on the ranged read come from pyarrow: a missing key gives `FileNotFoundError` and a permission problem gives `OSError`. The download path gives botocore `ClientError`. A caller that catches `ClientError` must also catch these errors when it gives `filters`.
- The pyarrow client does not share the boto3 configuration. Shared: the default credential chain (environment, profile, SSO, instance or pod role). Not shared: a custom endpoint (`AWS_ENDPOINT_URL` or `endpoint_url` in the AWS config, for example LocalStack or MinIO), the boto3 retry and pool settings, and `botocore.config.Config`. pyarrow 25 ignores `AWS_ENDPOINT_URL`, and `resolve_s3_region` calls AWS. A caller on a non-AWS endpoint must give its own `filesystem=pyarrow.fs.S3FileSystem(endpoint_override=...)`.
- The region lookup is a network call. It runs outside the lock of the boto3 caches, so a slow lookup does not block other threads. A failed lookup raises and is not cached, so the next filtered read tries again.
- The file system cache has one entry for each bucket of the current process. It has no size bound. A process that reads many buckets keeps one C++ S3 client for each bucket. Entries of other processes are dropped on the first read after a fork.
