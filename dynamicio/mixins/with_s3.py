"""This module provides mixins that are providing S3 I/O support."""

import dataclasses
import io
import os
import tempfile
import urllib.parse
import uuid
from contextlib import contextmanager
from typing import IO, Dict, Generator, List, Optional, Union
from urllib.parse import urlparse

import awswrangler as wr
import boto3
import pandas as pd
import s3transfer.futures
import tables
from awscli.clidriver import create_clidriver
from magic_logger import logger
from pandas import DataFrame, Series

# Application Imports
from dynamicio.config.pydantic import DataframeSchema, S3DataEnvironment, S3PathPrefixEnvironment
from dynamicio.mixins import utils, with_local
from dynamicio.mixins.utils import get_file_type_value


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


def awscli_runner(*cmd: str):
    """Runs the awscli command provided.

    Args:
        *cmd: A list of args used in the command.

    Raises:
        A runtime error exception is raised if download fails.

    Example:

        >>> awscli_runner("s3", "sync", "s3://mock-bucket/mock-key", ".")
    """
    # Run
    exit_code = create_clidriver().main(cmd)

    if exit_code > 0:
        raise RuntimeError(f"AWS CLI exited with code {exit_code}")


@dataclasses.dataclass
class S3TransferHandle:
    """A dataclass used to track an ongoing data download from the s3."""

    s3_object: object  # boto3.resource('s3').ObjectSummary
    fobj: IO[bytes]  # file-like object the data is being downloaded to
    done_future: s3transfer.futures.BaseTransferFuture


class WithS3PathPrefix(with_local.WithLocal):
    """Handles I/O operations for AWS S3; implements read operations only.

    This mixin assumes that the directories it reads from will only contain a single file-type.
    """

    sources_config: S3PathPrefixEnvironment  # type: ignore
    schema: DataframeSchema

    boto3_resource = boto3.resource("s3")
    boto3_client = boto3.client("s3")

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
            awscli_runner(
                "s3",
                "sync",
                temp_dir,
                full_path_prefix,
                "--acl",
                "bucket-owner-full-control",
                "--only-show-errors",
                "--exact-timestamps",
            )

    def _read_from_s3_path_prefix(self) -> pd.DataFrame:
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
                return self._read_parquet_file(full_path, self.schema, **self.options)
            if file_type == "hdf":
                dfs: List[DataFrame] = []
                for fobj in self._iter_s3_files(full_path, file_ext=".h5", max_memory_use=1024**3):  # 1 gib
                    dfs.append(HdfIO().load(fobj))
                df = pd.concat(dfs, ignore_index=True)
                columns = [column for column in df.columns.to_list() if column in self.schema.columns.keys()]
                return df[columns]

        with tempfile.TemporaryDirectory() as temp_dir:
            # aws-cli is shown to be up to 6 times faster when downloading the complete dataset from S3 than using the boto3
            # client or pandas directly. This is because aws-cli uses the parallel downloader, which is much faster than the
            # boto3 client.
            if dynamic_file_path:
                prefix, suffix = full_path.rsplit("/**/", 1)
                awscli_runner(
                    "s3",
                    "sync",
                    prefix,
                    temp_dir,
                    "--exclude",
                    "*",
                    "--include",
                    f"**/{suffix}",
                    "--acl",
                    "bucket-owner-full-control",
                    "--only-show-errors",
                    "--exact-timestamps",
                )
            else:
                awscli_runner(
                    "s3",
                    "sync",
                    full_path,
                    temp_dir,
                    "--acl",
                    "bucket-owner-full-control",
                    "--only-show-errors",
                    "--exact-timestamps",
                )

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
        for s3_object in self.boto3_resource.Bucket(bucket_name).objects.filter(Prefix=file_prefix):
            good_object = (not file_ext) or (s3_object.key.endswith(file_ext))
            if good_object:
                s3_objects_to_fetch.append(s3_object)

        if max_memory_use < 0:
            # Unlimited memory use - fetch ALL
            max_memory_use = sum(s3_obj.size for s3_obj in s3_objects_to_fetch) * 2
        transfer_config = boto3.s3.transfer.TransferConfig(max_concurrency=20)
        while s3_objects_to_fetch:
            mem_use_left = max_memory_use
            handles = []
            with boto3.s3.transfer.create_transfer_manager(self.boto3_client, transfer_config) as transfer_manager:
                while mem_use_left > 0 and s3_objects_to_fetch:
                    s3_object = s3_objects_to_fetch.pop()
                    fobj = io.BytesIO()
                    future = transfer_manager.download(bucket_name, s3_object.key, fobj)
                    handles.append(S3TransferHandle(s3_object, fobj, future))
                    mem_use_left -= s3_object.size
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
    @utils.allow_options(wr.s3.read_parquet)
    def _read_s3_parquet_file(s3_path: str, schema: DataframeSchema, **kwargs) -> pd.DataFrame:
        return wr.s3.read_parquet(path=s3_path, columns=(list(schema.columns.keys())), **kwargs)

    @staticmethod
    @utils.allow_options(utils.args_of(wr.s3.read_csv, pd.read_csv))
    def _read_s3_csv_file(s3_path: str, schema: DataframeSchema, **kwargs) -> pd.DataFrame:
        return wr.s3.read_csv(path=s3_path, usecols=(list(schema.columns.keys())), **kwargs)

    @staticmethod
    @utils.allow_options(utils.args_of(wr.s3.read_json, pd.read_json))
    def _read_s3_json_file(s3_path: str, schema: DataframeSchema, **kwargs) -> pd.DataFrame:
        orient = kwargs.pop("orient", None)
        lines = kwargs.pop("lines", None)

        if orient is not None and orient != "records":
            raise ValueError("[s3-json] Unsupported orient='{orient}'. Only 'records' orientation is supported.")

        if lines is not None and lines is not True:
            logger.warning("[s3-json-read] Overriding lines=%s with lines=True for aws-wrangler consistency.", lines)

        if kwargs.get("convert_dates") is True:
            logger.warning("[s3-json-read] Ignoring 'convert_dates=True'. Handle datetime parsing post-read.")
        kwargs.pop("convert_dates", None)

        raw_df = wr.s3.read_json(path=s3_path, orient="records", lines=True, **kwargs)

        # Normalize dict in single-column (e.g. {"data": {...}})
        if len(raw_df.columns) == 1 and raw_df.iloc[:, 0].apply(lambda x: isinstance(x, dict)).all():
            nested_col = raw_df.columns[0]
            normalized = pd.json_normalize(raw_df[nested_col])
            normalized.index = [nested_col]  # Mimic structured key as index
            raw_df = normalized.T

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
    @utils.allow_options(wr.s3.to_parquet)
    def _write_s3_parquet_file(df: pd.DataFrame, s3_path: str, **kwargs):
        wr.s3.to_parquet(df=df, path=s3_path, dataset=True, **kwargs)

    @staticmethod
    @utils.allow_options(utils.args_of(wr.s3.to_csv, pd.DataFrame.to_csv))
    def _write_s3_csv_file(df: pd.DataFrame, s3_path: str, **kwargs):
        wr.s3.to_csv(df=df, path=s3_path, index=False, **kwargs)

    @staticmethod
    @utils.allow_options(utils.args_of(wr.s3.to_json, pd.DataFrame.to_json))
    def _write_s3_json_file(df: pd.DataFrame, s3_path: str, **kwargs):
        user_orient = kwargs.pop("orient", None)
        user_lines = kwargs.pop("lines", None)

        if user_orient is not None and user_orient != "records":
            raise ValueError(f"[s3-json] Unsupported orient='{user_orient}'. Only 'records' orientation is supported.")

        if user_lines is not None and user_lines is not True:
            logger.warning(f"[s3-json] Overriding lines={user_lines} with lines=True for JSON serialization.")

        wr.s3.to_json(df=df, path=s3_path, orient="records", lines=True, index=False, **kwargs)

    @staticmethod
    @utils.allow_options(pd.HDFStore.put)
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
