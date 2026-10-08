"""This module provides mixins that are providing S3 I/O support."""

import dataclasses
import fnmatch
import io
import os
import shutil
import tempfile
import threading
import urllib.parse
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import IO, Any, Callable, Dict, Generator, List, Optional, Union
from urllib.parse import urlparse

import awswrangler as wr
import boto3
import boto3.s3.transfer
import pandas as pd
import pyarrow.parquet as pq
import s3transfer.futures
import tables
from botocore.config import Config
from magic_logger import logger
from pandas import DataFrame, Series

# Application Imports
from dynamicio.config.pydantic import DataframeSchema, S3DataEnvironment, S3PathPrefixEnvironment
from dynamicio.mixins import utils, with_local
from dynamicio.mixins.utils import get_file_type_value

_SESSION_LOCK = threading.Lock()
_SESSION: Dict[int, boto3.Session] = {}
_CLIENT: Dict[int, object] = {}


def _shared_boto3_session() -> boto3.Session:
    """Return a per-process boto3 Session.

    Called without a session, awswrangler builds a brand-new `boto3.Session` (and re-resolves credentials) on
    every call, which dominates the cost of reading or writing many small files. The session is keyed by PID so a
    forked worker never shares one with its parent.
    """
    pid = os.getpid()
    with _SESSION_LOCK:
        if pid not in _SESSION:
            _SESSION.clear()
            _SESSION[pid] = boto3.Session()
        return _SESSION[pid]


class InMemStore(pd.io.pytables.HDFStore):
    """A subclass of pandas HDFStore that does not manage the pytables File object."""

    _in_mem_table = None

    def __init__(self, path: str, table: tables.File, mode: str = "r"):
        """Create a new HDFStore object."""
        self._in_mem_table = table
        super().__init__(path=path, mode=mode)

    def open(self, *_args, **_kwargs):
        """Open the in-memory table."""
        pd.io.pytables._tables()  # pylint: disable=protected-access
        self._handle = self._in_mem_table

    def close(self, *_args, **_kwargs):
        """Close the in-memory table."""

    @property
    def is_open(self):
        """Check if the in-memory table is open."""
        return self._handle is not None


class HdfIO:
    """Provides in-memory stream support for reading and writing HDF5 tables.

    Uses PyTables to create in-memory file handles, enabling read/write
    operations on HDF content without persisting to disk.
    """

    @contextmanager
    def create_file(self, label: str, mode: str, data: Optional[bytes] = None) -> Generator[tables.File, None, None]:
        """Create an in-memory HDF5 file using PyTables with optional preloaded data.

        Args:
            label (str): A label used for naming the temporary in-memory file.
            mode (str): File access mode ('r' for read, 'w' for write).
            data (Optional[bytes]): Raw file data to preload when opening for reading.

        Yields:
            tables.File: A PyTables file object representing the HDF5 structure.
        """
        extra_kw = {"driver_core_backing_store": 0}
        if data:
            extra_kw["driver_core_image"] = data

        file_name = f"{label}_{uuid.uuid4()}.h5"
        file_handle = tables.File(file_name, mode, title=label, root_uep="/", filters=None, driver="H5FD_CORE", **extra_kw)

        try:
            yield file_handle
        finally:
            file_handle.close()

    def load(self, fobj: IO[bytes], label: str = "unknown_file.h5", options: Optional[Dict] = None) -> Union[DataFrame, Series]:
        """Load a DataFrame or Series from an in-memory HDF5 file-like object.

        Args:
            fobj (IO[bytes]): A file-like object containing the HDF5 data.
            label (str): A logical name for the file (used in metadata).
            options (Optional[dict]): Optional keyword arguments to pass to `pd.read_hdf`.

        Returns:
            Union[DataFrame, Series]: The object read from the HDF file.
        """
        options = options or {}

        with self.create_file(label, mode="r", data=fobj.read()) as file_handle:
            return pd.read_hdf(InMemStore(label, file_handle), **options)

    def save(self, df: DataFrame, fobj: IO[bytes], label: str = "unknown_file.h5", options: Optional[Dict] = None) -> None:
        """Save a DataFrame to a file-like object as an HDF5 structure.

        Args:
            df (DataFrame): The DataFrame to store.
            fobj (IO[bytes]): The target file-like object.
            label (str): A logical name used for the in-memory file.
            options (Optional[dict]): Optional keyword arguments to pass to `HDFStore.put`.
                                      You can also include a `key` (defaults to 'df').

        Notes:
            Data is first written to an in-memory PyTables structure, then streamed into the provided file-like object.
        """
        options = options or {}
        key = options.pop("key", "df")

        with self.create_file(label, mode="w") as file_handle:
            store = InMemStore(path=label, table=file_handle, mode="w")
            store.put(key=key, value=df, **options)
            fobj.write(file_handle.get_file_image())


def _shared_s3_client():
    """Return a per-process S3 client built from the shared session (thread-safe, keeps connections alive)."""
    session = _shared_boto3_session()
    pid = os.getpid()
    with _SESSION_LOCK:
        if pid not in _CLIENT:
            _CLIENT.clear()
            _CLIENT[pid] = session.client("s3", config=Config(max_pool_connections=64))  # pool must cover the sync thread pools
        return _CLIENT[pid]


def _split_s3_url(url: str):
    parsed = urlparse(url)
    assert parsed.scheme == "s3", f"{url!r} should be an s3 url"
    return parsed.netloc, parsed.path.lstrip("/")


_SYNC_MAX_CONCURRENCY = 32  # measured best/flat from 32 up for 200-5000 files; the old 10 was up to 2x slower on small prefixes


def _download_file(client, bucket: str, key: str, local_path: str):
    """Download one object: a single GetObject for small ones, the multipart transfer manager for large ones."""
    response = client.get_object(Bucket=bucket, Key=key)
    if response["ContentLength"] > _DIRECT_TRANSFER_MAX_BYTES:
        response["Body"].close()
        client.download_file(bucket, key, local_path)
        return
    with open(local_path, "wb") as fobj:
        shutil.copyfileobj(response["Body"], fobj)


def s3_sync_down(source_url: str, dest_dir: str, include_pattern: Optional[str] = None, max_concurrency: int = _SYNC_MAX_CONCURRENCY):
    """Download every object under an S3 prefix into `dest_dir`, preserving the relative key layout.

    One `GetObject` per file (no `HeadObject`), issued from a thread pool while the listing is still paginating.

    Args:
        source_url: `s3://bucket/prefix` to download from.
        dest_dir: Local directory to download into.
        include_pattern: Optional `fnmatch` pattern matched against each key relative to the prefix.
        max_concurrency: Number of concurrent download threads.

    Raises:
        Whatever boto3 raises if listing or any download fails.
    """
    bucket, key_prefix = _split_s3_url(source_url)
    key_prefix = f"{key_prefix.rstrip('/')}/" if key_prefix else ""
    client = _shared_s3_client()

    with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
        futures = []
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key_prefix):
            for obj in page.get("Contents", []):
                relative_key = obj["Key"][len(key_prefix) :]
                if not relative_key or relative_key.endswith("/"):
                    continue
                if include_pattern and not fnmatch.fnmatch(relative_key, include_pattern):
                    continue
                local_path = os.path.join(dest_dir, *relative_key.split("/"))
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                futures.append(pool.submit(_download_file, client, bucket, obj["Key"], local_path))
        for future in futures:
            future.result()


def s3_read_down(source_url: str, reader: Callable[[io.BytesIO], Any], include_pattern: Optional[str] = None, max_concurrency: int = _SYNC_MAX_CONCURRENCY) -> List[Any]:
    """Fetch every object under an S3 prefix into memory and parse it with `reader`, overlapping network and parsing.

    Unlike `s3_sync_down` there is no temp-dir round trip: each worker thread does a `GetObject` and parses the bytes
    straight away, so parsing of early files overlaps with the download of later ones.

    Args:
        source_url: `s3://bucket/prefix` to read from.
        reader: Called with an in-memory file object for each object.
        include_pattern: Optional `fnmatch` pattern matched against each key relative to the prefix.
        max_concurrency: Number of concurrent worker threads.

    Returns:
        The `reader` results, in listing (lexicographic key) order.
    """
    bucket, key_prefix = _split_s3_url(source_url)
    key_prefix = f"{key_prefix.rstrip('/')}/" if key_prefix else ""
    client = _shared_s3_client()

    def fetch_and_parse(key: str):
        return reader(_download_to_memory(bucket, key))

    with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
        futures = []
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=key_prefix):
            for obj in page.get("Contents", []):
                relative_key = obj["Key"][len(key_prefix) :]
                if not relative_key or relative_key.endswith("/"):
                    continue
                if include_pattern and not fnmatch.fnmatch(relative_key, include_pattern):
                    continue
                futures.append(pool.submit(fetch_and_parse, obj["Key"]))
        return [future.result() for future in futures]


def _upload_file(client, local_path: str, bucket: str, key: str, acl: str):
    """Upload one file: a single PutObject for small ones, the multipart transfer manager for large ones."""
    if os.path.getsize(local_path) > _DIRECT_TRANSFER_MAX_BYTES:
        client.upload_file(local_path, bucket, key, ExtraArgs={"ACL": acl})
        return
    with open(local_path, "rb") as fobj:
        client.put_object(Bucket=bucket, Key=key, Body=fobj, ACL=acl)


def s3_sync_up(source_dir: str, dest_url: str, acl: str = "bucket-owner-full-control", max_concurrency: int = _SYNC_MAX_CONCURRENCY):
    """Upload every file under `source_dir` to an S3 prefix, preserving the relative layout.

    Args:
        source_dir: Local directory to upload.
        dest_url: `s3://bucket/prefix` to upload into.
        acl: Canned ACL applied to every uploaded object.
        max_concurrency: Number of concurrent upload threads.
    """
    bucket, key_prefix = _split_s3_url(dest_url)
    key_prefix = f"{key_prefix.rstrip('/')}/" if key_prefix else ""
    client = _shared_s3_client()

    with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
        futures = []
        for root, _, files in os.walk(source_dir):
            for name in files:
                local_path = os.path.join(root, name)
                relative_key = os.path.relpath(local_path, source_dir).replace(os.sep, "/")
                futures.append(pool.submit(_upload_file, client, local_path, bucket, f"{key_prefix}{relative_key}", acl))
        for future in futures:
            future.result()


_DIRECT_TRANSFER_MAX_BYTES = 16 * 1024**2  # larger objects use the multipart transfer manager

# Options only awswrangler understands; if any is given we defer to awswrangler to keep its semantics.
_WRANGLER_ONLY_READ_PARQUET = utils.args_of(wr.s3.read_parquet) - utils.args_of(pd.read_parquet) - utils.args_of(pq.read_table) - {"path"}
_WRANGLER_ONLY_WRITE_PARQUET = utils.args_of(wr.s3.to_parquet) - utils.args_of(pd.DataFrame.to_parquet) - utils.args_of(pq.write_table) - {"df", "path", "dataset", "use_threads"}


def _download_to_memory(bucket: str, key: str) -> io.BytesIO:
    """Download an S3 object into memory over the shared client (one round trip for small objects)."""
    client = _shared_s3_client()
    response = client.get_object(Bucket=bucket, Key=key)
    if response["ContentLength"] <= _DIRECT_TRANSFER_MAX_BYTES:
        return io.BytesIO(response["Body"].read())
    response["Body"].close()
    fobj = io.BytesIO()
    client.download_fileobj(bucket, key, fobj)
    fobj.seek(0)
    return fobj


def _upload_from_memory(fobj: io.BytesIO, bucket: str, key: str, acl: str = "bucket-owner-full-control") -> None:
    """Upload an in-memory buffer over the shared client (single PUT for small objects)."""
    client = _shared_s3_client()
    size = fobj.getbuffer().nbytes
    fobj.seek(0)
    if size <= _DIRECT_TRANSFER_MAX_BYTES:
        client.put_object(Bucket=bucket, Key=key, Body=fobj.getvalue(), ACL=acl)
    else:
        client.upload_fileobj(fobj, bucket, key, ExtraArgs={"ACL": acl})


@dataclasses.dataclass
class S3TransferHandle:
    """A dataclass used to track an ongoing data download from the s3."""

    s3_object: dict  # an entry of the `Contents` list returned by `list_objects_v2`
    fobj: IO[bytes]  # file-like object the data is being downloaded to
    done_future: s3transfer.futures.BaseTransferFuture


class WithS3PathPrefix(with_local.WithLocal):
    """Handles I/O operations for AWS S3 path prefixes (reads, and partitioned parquet writes), using boto3 transfers.

    This mixin assumes that the directories it reads from will only contain a single file-type.
    """

    sources_config: S3PathPrefixEnvironment  # type: ignore
    schema: DataframeSchema

    @property
    def boto3_client(self):
        """Per-process shared S3 client."""
        return _shared_s3_client()

    def _write_to_s3_path_prefix(self, df: pd.DataFrame):
        """Write a DataFrame to an S3 path prefix.

        The configuration object is expected to have the following keys:
            - `bucket`
            - `path_prefix`
            - `file_type`

        Args:
            df (pd.DataFrame): the DataFrame to be written to S3

        Raises:
            ValueError: In case `path_prefix` is missing from config
            ValueError: In case the `partition_cols` arg is missing while trying to write a parquet file
        """
        s3_config = self.sources_config.s3
        file_type = get_file_type_value(s3_config.file_type)
        if file_type != "parquet":
            raise ValueError(f"File type not supported: {file_type}, only parquet files can be written to an S3 key")
        if "partition_cols" not in self.options:
            raise ValueError("`partition_cols` is required as an option to write partitioned parquet files to S3")

        bucket = s3_config.bucket
        path_prefix = s3_config.path_prefix
        full_path_prefix = utils.resolve_template(f"s3://{bucket}/{path_prefix}", self.options)

        with tempfile.TemporaryDirectory() as temp_dir:
            self._write_parquet_file(df, temp_dir, **self.options)
            s3_sync_up(temp_dir, full_path_prefix)

    def _read_from_s3_path_prefix(self) -> pd.DataFrame:  # pylint: disable=too-many-locals,too-many-branches
        """Read files from an S3 bucket based on a path_prefix/dynamic_file_path and return a concatenated DataFrame.

        This function supports two types of file paths from the S3 configuration:
        1. `path_prefix`: Used to specify a static path prefix in the S3 bucket for downloading files.
        2. `dynamic_file_path`: Used for more dynamic path specifications, allowing pattern matching for selective file
        downloads.

        The `dynamic_file_path` supports pattern matching and variables (e.g., `part_{runner_id}.parquet`). This
        approach  enables the downloading of files that specifically match the given pattern, optimizing I/O for
        scenarios involving large datasets or multiple runners.

        The method dynamically invokes appropriate file reading functions based on the `file_type` specified in the
        configuration, supporting formats such as 'parquet', 'csv', 'hdf', and 'json'.

        The function also includes an option to minimize disk space usage (`no_disk_space`). This is particularly
        useful when needing to read a subset of columns from large files, thereby reducing the overall disk footprint.

        Parameters:
        - None

        Returns:
        - DataFrame: A pandas DataFrame concatenated from the read files.

        Raises:
        - ValueError: If the `file_type` specified in the configuration is not supported.

        Configuration Keys:
        - `bucket` (str): Name of the S3 bucket.
        - `path_prefix` (str, optional): Static path prefix in the S3 bucket for file downloads.
        - `dynamic_file_path` (str, optional): Dynamic file path with pattern matching for selective downloading of files.
        - `file_type` (str): Type of the file to read ('parquet', 'csv', 'hdf', 'json').

        Notes:
        - Only one of `path_prefix` and `dynamic_file_path` can be provided.
        - The function intelligently handles the download of files by synchronizing only those that match the specified
        pattern in `dynamic_file_path`.
        - e.g. a `runner_id` or any other variable used in `dynamic_file_path` for pattern matching should be specified
        in the `options` of the configuration.
        """
        s3_config = self.sources_config.s3
        file_type = get_file_type_value(s3_config.file_type)
        if file_type not in {"parquet", "csv", "hdf", "json"}:
            raise ValueError(f"File type not supported: {file_type}")

        bucket = s3_config.bucket
        dynamic_file_path = s3_config.dynamic_file_path
        path_prefix = s3_config.path_prefix

        if dynamic_file_path:
            full_path = utils.resolve_template(f"s3://{bucket}/{dynamic_file_path}", self.options)
        else:
            full_path = utils.resolve_template(f"s3://{bucket}/{path_prefix}", self.options)

        # The `no_disk_space` option should be used only when reading a subset of columns from S3
        if self.options.pop("no_disk_space", False) and path_prefix:
            if file_type == "parquet":
                parquet_dfs = [self._read_parquet_file(fobj, self.schema, **self.options) for fobj in self._iter_s3_files(full_path, file_ext=".parquet", max_memory_use=1024**3)]
                return pd.concat(parquet_dfs, ignore_index=True)
            if file_type == "hdf":
                dfs: List[DataFrame] = []
                for fobj in self._iter_s3_files(full_path, file_ext=".h5", max_memory_use=1024**3):  # 1 gib
                    dfs.append(HdfIO().load(fobj))
                df = pd.concat(dfs, ignore_index=True)
                columns = [column for column in df.columns.to_list() if column in self.schema.columns.keys()]
                return df[columns]

        if file_type == "parquet":
            # Parquet is parsed straight from memory by the download threads: no temp dir, and parsing overlaps the network.
            def reader(fobj: io.BytesIO) -> pd.DataFrame:
                return self._read_parquet_file(fobj, self.schema, **self.options)  # type: ignore[arg-type]

            if dynamic_file_path:
                prefix, suffix = full_path.rsplit("/**/", 1)
                results = s3_read_down(prefix, reader, include_pattern=f"**/{suffix}")
            else:
                results = s3_read_down(full_path, reader)
            return pd.concat([df for df in results if len(df) > 0], ignore_index=True)

        with tempfile.TemporaryDirectory() as temp_dir:
            if dynamic_file_path:
                prefix, suffix = full_path.rsplit("/**/", 1)
                s3_sync_down(prefix, temp_dir, include_pattern=f"**/{suffix}")
            else:
                s3_sync_down(full_path, temp_dir)

            dfs: List[DataFrame] = []
            for file in os.listdir(temp_dir):
                df = getattr(self, f"_read_{file_type}_file")(os.path.join(temp_dir, file), self.schema, **self.options)  # type: ignore
                if len(df) > 0:
                    dfs.append(df)

            return pd.concat(dfs, ignore_index=True)

    def _iter_s3_files(self, s3_prefix: str, file_ext: Optional[str] = None, max_memory_use: int = -1) -> Generator[IO[bytes], None, None]:  # pylint: disable=too-many-locals
        """Download sways of S3 objects.

        Args:
            s3_prefix: s3 url to fetch objects with
            file_ext: extension of s3 objects to allow through
            max_memory_use: The approximate number of bytes to allocate on each yield of Generator
        """
        parsed_url = urllib.parse.urlparse(s3_prefix)
        assert parsed_url.scheme == "s3", f"{s3_prefix!r} should be an s3 url"
        bucket_name = parsed_url.netloc
        file_prefix = f"{parsed_url.path.strip('/')}/"
        s3_objects_to_fetch = []
        # Collect objects to be loaded
        for page in self.boto3_client.get_paginator("list_objects_v2").paginate(Bucket=bucket_name, Prefix=file_prefix):
            for s3_object in page.get("Contents", []):
                if (not file_ext) or s3_object["Key"].endswith(file_ext):
                    s3_objects_to_fetch.append(s3_object)

        if max_memory_use < 0:
            # Unlimited memory use - fetch ALL
            max_memory_use = sum(s3_obj["Size"] for s3_obj in s3_objects_to_fetch) * 2
        transfer_config = boto3.s3.transfer.TransferConfig(max_concurrency=20)
        while s3_objects_to_fetch:
            mem_use_left = max_memory_use
            handles = []
            with boto3.s3.transfer.create_transfer_manager(self.boto3_client, transfer_config) as transfer_manager:
                while mem_use_left > 0 and s3_objects_to_fetch:
                    s3_object = s3_objects_to_fetch.pop()
                    fobj = io.BytesIO()
                    future = transfer_manager.download(bucket_name, s3_object["Key"], fobj)
                    handles.append(S3TransferHandle(s3_object, fobj, future))
                    mem_use_left -= s3_object["Size"]
                # Leaving the `transfer_manager` context implicitly waits for all downloads to complete
            # Rewind and yield all fobjs
            for handle in handles:
                handle.fobj.seek(0)
                yield handle.fobj


class WithS3File:
    """Handles I/O operations for AWS S3 using in-memory streaming for CSV, JSON, Parquet, and HDF files.

    For CSV, JSON, and Parquet, AWS Data Wrangler is used for efficient direct reads from S3.
    For HDF files, content is streamed into memory using boto3 and then loaded via PyTables.
    """

    sources_config: S3DataEnvironment
    schema: DataframeSchema

    def _read_from_s3_file(self) -> pd.DataFrame:
        """Read a file from an S3 bucket as a `DataFrame`.

        The configuration object is expected to have the following keys:
            - `bucket`
            - `file_path`
            - `file_type`

        To actually read the file, a method is dynamically invoked by name, using "_read_{file_type}_file".

        Returns:
            DataFrame
        """
        s3_config = self.sources_config.s3
        file_type = get_file_type_value(s3_config.file_type)
        options = getattr(self, "options", {})
        s3_path = f"s3://{s3_config.bucket}/{utils.resolve_template(s3_config.file_path, options)}"

        logger.info(f"[s3] Started downloading: {s3_path}")

        return getattr(self, f"_read_s3_{file_type}_file")(s3_path, self.schema, **options)

    @staticmethod
    def _read_s3_parquet_file(s3_path: str, schema: DataframeSchema, **kwargs) -> pd.DataFrame:
        """Read a single parquet file.

        By default the object is fetched in a single round trip over a shared boto3 client and parsed in memory with
        the same pandas/pyarrow options as local reads. If options only awswrangler understands are given (e.g.
        `s3_additional_kwargs`, `pyarrow_additional_kwargs`), awswrangler does the read instead.
        """
        if _WRANGLER_ONLY_READ_PARQUET & kwargs.keys():
            return WithS3File._read_s3_parquet_file_with_wrangler(s3_path, schema, **kwargs)
        bucket, key = _split_s3_url(s3_path)
        return with_local.WithLocal._read_parquet_file(_download_to_memory(bucket, key), schema, **kwargs)  # type: ignore[arg-type] # pylint: disable=protected-access

    @staticmethod
    @utils.allow_options(wr.s3.read_parquet)
    def _read_s3_parquet_file_with_wrangler(s3_path: str, schema: DataframeSchema, **kwargs) -> pd.DataFrame:
        kwargs.pop("columns", None)
        kwargs.setdefault("boto3_session", _shared_boto3_session())
        # A one-element list is read as-is; a bare string is treated as a prefix and costs an extra ListObjectsV2.
        return wr.s3.read_parquet(path=[s3_path], columns=(list(schema.columns.keys())), **kwargs)

    @staticmethod
    @utils.allow_options(utils.args_of(wr.s3.read_csv, pd.read_csv))
    def _read_s3_csv_file(s3_path: str, schema: DataframeSchema, **kwargs) -> pd.DataFrame:
        kwargs.pop("usecols", None)
        return wr.s3.read_csv(path=s3_path, usecols=(list(schema.columns.keys())), **kwargs)

    @staticmethod
    @utils.allow_options([*utils.args_of(wr.s3.read_json, pd.read_json), "single_record"])
    def _read_s3_json_file(s3_path: str, schema: DataframeSchema, **kwargs) -> pd.DataFrame:
        is_single_record = kwargs.pop("single_record", False)
        orient = kwargs.pop("orient", "records")
        lines = kwargs.pop("lines", None)
        if lines is None:
            lines = orient == "records"

        if kwargs.get("convert_dates") is True:
            logger.warning("[s3-json-read] Ignoring 'convert_dates=True'. Handle datetime parsing post-read.")
        kwargs.pop("convert_dates", None)

        raw_df = wr.s3.read_json(path=s3_path, orient=orient, lines=lines, **kwargs)

        if is_single_record:
            # Re-wrap as a single dict row, mirroring the local JSON reader
            raw_df = pd.DataFrame([{raw_df.columns[0]: dict(zip(raw_df.index, raw_df.iloc[:, 0]))}])

        return raw_df[[col for col in raw_df.columns if col in schema.columns]]

    @staticmethod
    @utils.allow_options(pd.read_hdf)
    def _read_s3_hdf_file(s3_path: str, schema: DataframeSchema, **kwargs) -> pd.DataFrame:
        parsed = urlparse(s3_path)
        bucket = parsed.netloc
        file_path = parsed.path.lstrip("/")

        fobj = io.BytesIO()  # Stream file directly into memory (no disk), unlike tempfile or open(...) which write to disk
        boto3.client("s3").download_fileobj(bucket, file_path, fobj)
        fobj.seek(0)

        df = HdfIO().load(fobj, options=kwargs)

        return df[list(schema.columns.keys())] if schema and schema.columns else df

    def _write_to_s3_file(self, df: pd.DataFrame):
        """Write a DataFrame to S3 based on the config's file type and path.

        The appropriate writer function is dynamically resolved via the file type:
        `_write_parquet_file`, `_write_csv_file`, `_write_json_file`, or `_write_hdf_file`.
        """
        s3_config = self.sources_config.s3
        file_type = get_file_type_value(s3_config.file_type)
        options = getattr(self, "options", {})
        s3_path = f"s3://{s3_config.bucket}/{utils.resolve_template(s3_config.file_path, options)}"

        logger.info(f"[s3] Started uploading: {s3_path}")
        getattr(self, f"_write_s3_{file_type}_file")(df, s3_path, **options)
        logger.info(f"[s3] Finished uploading: {s3_path}")

    @staticmethod
    def _write_s3_parquet_file(df: pd.DataFrame, s3_path: str, **kwargs):
        """Write a single parquet file.

        By default the file is serialised in memory with the same pandas/pyarrow options as local writes and uploaded
        over a shared boto3 client. If options only awswrangler understands are given, awswrangler does the write.
        """
        if kwargs.pop("dataset", False):
            raise ValueError(
                "[s3-parquet] dataset=True is not supported in the WithS3File mixin. Use a file path, not a directory. "
                "Use WithS3PathPrefix if you need partitioned writes or directory-style datasets."
            )

        if s3_path.endswith("/"):
            raise ValueError("[s3-parquet] Parquet output path must be a file, not a directory (e.g., 's3://bucket/data.parquet').")

        if _WRANGLER_ONLY_WRITE_PARQUET & kwargs.keys():
            WithS3File._write_s3_parquet_file_with_wrangler(df, s3_path, **kwargs)
            return
        kwargs.pop("use_threads", None)
        fobj = io.BytesIO()
        with_local.WithLocal._write_parquet_file(df, fobj, **kwargs)  # type: ignore[arg-type] # pylint: disable=protected-access
        bucket, key = _split_s3_url(s3_path)
        _upload_from_memory(fobj, bucket, key)

    @staticmethod
    @utils.allow_options(wr.s3.to_parquet)
    def _write_s3_parquet_file_with_wrangler(df: pd.DataFrame, s3_path: str, **kwargs):
        kwargs.setdefault("s3_additional_kwargs", {}).setdefault("ACL", "bucket-owner-full-control")
        kwargs.setdefault("boto3_session", _shared_boto3_session())
        wr.s3.to_parquet(df=df, path=s3_path, dataset=False, **kwargs)

    @staticmethod
    @utils.allow_options(utils.args_of(wr.s3.to_csv, pd.DataFrame.to_csv))
    def _write_s3_csv_file(df: pd.DataFrame, s3_path: str, **kwargs):
        if kwargs.pop("dataset", False):
            raise ValueError(
                "[s3-csv] dataset=True is not supported in the WithS3File mixin. Use a file path, not a directory. "
                "Use WithS3PathPrefix if you need partitioned writes or directory-style datasets."
            )

        if s3_path.endswith("/"):
            raise ValueError("[s3-csv] CSV output path must be a file, not a directory (e.g., 's3://bucket/data.csv').")

        kwargs.setdefault("s3_additional_kwargs", {}).setdefault("ACL", "bucket-owner-full-control")
        wr.s3.to_csv(df=df, path=s3_path, index=False, **kwargs)

    @staticmethod
    @utils.allow_options(utils.args_of(wr.s3.to_json, pd.DataFrame.to_json))
    def _write_s3_json_file(df: pd.DataFrame, s3_path: str, **kwargs):
        if kwargs.pop("dataset", False):
            raise ValueError(
                "[s3-json] dataset=True is not supported in the WithS3File mixin. Use a file path, not a directory. "
                "Use WithS3PathPrefix if you need partitioned writes or directory-style datasets."
            )

        if s3_path.endswith("/"):
            raise ValueError("[s3-json] JSON output path must be a file, not a directory (e.g., 's3://bucket/data.json').")

        user_orient = kwargs.pop("orient", "records")
        user_lines = kwargs.pop("lines", None)
        if user_lines is None:
            user_lines = user_orient == "records"
        user_index = kwargs.pop("index", False)

        kwargs.setdefault("s3_additional_kwargs", {}).setdefault("ACL", "bucket-owner-full-control")
        wr.s3.to_json(df=df, path=s3_path, orient=user_orient, lines=user_lines, index=user_index, **kwargs)

    @staticmethod
    @utils.allow_options([*utils.args_of(pd.HDFStore.put), "pickle_protocol"])
    def _write_s3_hdf_file(df: pd.DataFrame, s3_path: str, **kwargs):
        """Write a DataFrame to S3 as an HDF5 file, using in-memory streaming."""
        parsed = urlparse(s3_path)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")

        # Separate protocol and HDF put options
        pickle_protocol = kwargs.pop("pickle_protocol", None)

        fobj = io.BytesIO()
        with utils.pickle_protocol(protocol=pickle_protocol):
            HdfIO().save(df, fobj, options=kwargs)

        fobj.seek(0)
        boto3.client("s3").upload_fileobj(fobj, bucket, key, ExtraArgs={"ACL": "bucket-owner-full-control"})
