# pylint: disable=no-member, protected-access, too-few-public-methods

"""This module provides mixins that are providing S3 I/O support."""

import dataclasses
import io
import os
import tempfile
import urllib.parse
import uuid
from contextlib import contextmanager
from typing import Generator, IO, Optional

import boto3  # type: ignore
import pandas as pd  # type: ignore
import s3transfer.futures  # type: ignore
import tables  # type: ignore
from awscli.clidriver import create_clidriver  # type: ignore
from magic_logger import logger

from dynamicio.config.pydantic import DataframeSchema, S3DataEnvironment, S3PathPrefixEnvironment
from dynamicio.mixins import (
    utils,
    with_local,
)


class InMemStore(pd.io.pytables.HDFStore):
    """A subclass of pandas HDFStore that does not manage the pytables File object"""

    _in_mem_table = None

    def __init__(self, path: str, table: tables.File, mode: str = "r"):
        self._in_mem_table = table
        super().__init__(path=path, mode=mode)

    def open(self, *_args, **_kwargs):
        pd.io.pytables._tables()
        self._handle = self._in_mem_table

    def close(self, *_args, **_kwargs):
        pass

    @property
    def is_open(self):
        return self._handle is not None


class HdfIO:
    """Class providing stream support for HDF tables"""

    @contextmanager
    def create_file(self, label: str, mode: str, data: bytes = None) -> Generator[tables.File, None, None]:
        """Create an in-memory pytables table"""
        extra_kw = {}
        if data:
            extra_kw["driver_core_image"] = data
        file_handle = tables.File(f"{label}_{uuid.uuid4()}.h5", mode, title=label, root_uep="/", filters=None, driver="H5FD_CORE", driver_core_backing_store=0, **extra_kw)
        try:
            yield file_handle
        finally:
            file_handle.close()

    def load(self, fobj: IO[bytes], label: str = "unknown_file.h5") -> pd.DataFrame:
        """Load the dataframe from an file-like object"""
        with self.create_file(label, mode="r", data=fobj.read()) as file_handle:
            return pd.read_hdf(InMemStore(label, file_handle))

    def save(self, df: pd.DataFrame, fobj: IO[bytes], label: str = "unknown_file.h5", options: Optional[dict] = None):
        """Load the dataframe to a file-like object"""
        if not options:
            options = {}
        with self.create_file(label, mode="w", data=fobj.read()) as file_handle:
            store = InMemStore(path=label, table=file_handle, mode="w")
            store.put(key="df", value=df, **options)
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
    """A dataclass used to track an ongoing data download from the s3"""

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

        file_type = s3_config.file_type
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
        """Read all files under a path prefix from an S3 bucket as a `DataFrame`.

        The configuration object is expected to have the following keys:
            - `bucket`
            - `path_prefix`
            - `file_type`

        To actually read the file, a method is dynamically invoked by name, using
        "_read_{file_type}_path_prefix".

        Returns:
            DataFrame
        """
        s3_config = self.sources_config.s3
        file_type = s3_config.file_type
        if file_type not in {"parquet", "csv", "hdf", "json"}:
            raise ValueError(f"File type not supported: {file_type}")

        bucket = s3_config.bucket
        path_prefix = s3_config.path_prefix
        full_path_prefix = utils.resolve_template(f"s3://{bucket}/{path_prefix}", self.options)

        # The `no_disk_space` option should be used only when reading a subset of columns from S3
        if self.options.pop("no_disk_space", False):
            if file_type == "parquet":
                return self._read_parquet_file(full_path_prefix, self.schema, **self.options)
            if file_type == "hdf":
                dfs = []
                for fobj in self._iter_s3_files(
                    full_path_prefix,
                    file_ext=".h5",
                    max_memory_use=1024**3,  # 1 gib
                ):
                    dfs.append(HdfIO().load(fobj))
                df = pd.concat(dfs, ignore_index=True)
                columns = [column for column in df.columns.to_list() if column in self.schema.columns.keys()]
                return df[columns]

        with tempfile.TemporaryDirectory() as temp_dir:
            # aws-cli is shown to be up to 6 times faster when downloading the complete dataset from S3 than using the boto3
            # client or pandas directly. This is because aws-cli uses the parallel downloader, which is much faster than the
            # boto3 client.
            awscli_runner(
                "s3",
                "sync",
                full_path_prefix,
                temp_dir,
                "--acl",
                "bucket-owner-full-control",
                "--only-show-errors",
                "--exact-timestamps",
            )

            dfs = []
            for file in os.listdir(temp_dir):
                df = getattr(self, f"_read_{file_type}_file")(os.path.join(temp_dir, file), self.schema, **self.options)  # type: ignore
                if len(df) > 0:
                    dfs.append(df)

            return pd.concat(dfs, ignore_index=True)

    def _iter_s3_files(self, s3_prefix: str, file_ext: Optional[str] = None, max_memory_use: int = -1) -> Generator[IO[bytes], None, None]:  # pylint: disable=too-many-locals
        """Download sways of S3 objects.

        Parameters:
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


class WithS3File(with_local.WithLocal):
    """Handles I/O operations for AWS S3.

    All files are persisted to disk first using boto3 as this has proven to be faster than reading them into memory.
    Note that reading things into memory is available for csv, json and parquet types only. Unfortunately, until support
    for generic buffer is added to read_hdf, we need to download and persists the file to disk first anyway.

    Options:
        no_disk_space: If `True`, then s3fs + fsspec will be used to read data directly into memory.
    """

    sources_config: S3DataEnvironment  # type: ignore
    schema: DataframeSchema

    boto3_client = boto3.client("s3")

    @contextmanager
    def _s3_named_file_reader(self, s3_bucket: str, s3_key: str) -> Generator:
        """Contextmanager to abstract reading different file types in S3.

        This implementation saves the downloaded data to a temporary file.

        Args:
            s3_bucket: The S3 bucket from where to read the file.
            s3_key: The file-path to the target file to be read.

        Returns:
            The local file path from where the file can be read, once it has been downloaded there by the boto3.client.

        """
        with tempfile.NamedTemporaryFile("wb") as target_file:
            # Download the file from S3
            self.boto3_client.download_fileobj(s3_bucket, s3_key, target_file)
            # Yield local file path to body of `with` statement
            target_file.flush()
            yield target_file

    @contextmanager
    def _s3_reader(self, s3_bucket: str, s3_key: str) -> Generator[io.BytesIO, None, None]:
        """Contextmanager to abstract reading different file types in S3.

         This implementation only retains data in-memory, avoiding creating any temp files.

        Args:
            s3_bucket: The S3 bucket from where to read the file.
            s3_key: The file-path to the target file to be read.

        Returns:
            The local file path from where the file can be read, once it has been downloaded there by the boto3.client.

        """
        fobj = io.BytesIO()
        # Download the file from S3
        self.boto3_client.download_fileobj(s3_bucket, s3_key, fobj)
        # Yield the buffer
        fobj.seek(0)
        yield fobj

    @contextmanager
    def _s3_writer(self, s3_bucket: str, s3_key: str) -> Generator[IO[bytes], None, None]:
        """Contextmanager to abstract loading different file types to S3.

        Args:
            s3_bucket: The S3 bucket to upload the file to.
            s3_key: The file-path where the target file should be uploaded to.

        Returns:
            The local file path where to actually write the file, to be read and uploaded by boto3.client.
        """
        fobj = io.BytesIO()
        yield fobj
        fobj.seek(0)
        self.boto3_client.upload_fileobj(fobj, s3_bucket, s3_key, ExtraArgs={"ACL": "bucket-owner-full-control"})

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
        file_type = s3_config.file_type
        file_path = utils.resolve_template(s3_config.file_path, self.options)
        bucket = s3_config.bucket

        logger.info(f"[s3] Started downloading: s3://{s3_config.bucket}/{file_path}")
        if self.options.pop("no_disk_space", None):
            no_disk_space_rv = None
            if file_type in ["csv", "json", "parquet"]:
                no_disk_space_rv = getattr(self, f"_read_{file_type}_file")(f"s3://{s3_config.bucket}/{file_path}", self.schema, **self.options)  # type: ignore
            elif file_type == "hdf":
                with self._s3_reader(s3_bucket=bucket, s3_key=file_path) as fobj:  # type: ignore
                    no_disk_space_rv = HdfIO().load(fobj)  # type: ignore
            else:
                raise NotImplementedError(f"Unsupported file type {file_type!r}.")
            if no_disk_space_rv is not None:
                return no_disk_space_rv
        with self._s3_named_file_reader(s3_bucket=bucket, s3_key=file_path) as target_file:  # type: ignore
            return getattr(self, f"_read_{file_type}_file")(target_file.name, self.schema, **self.options)  # type: ignore

    def _write_to_s3_file(self, df: pd.DataFrame):
        """Write a dataframe to s3 based on the {file_type} of the config_io configuration.

        The configuration object is expected to have two keys:

            - `file_path`
            - `file_type`

        To actually write the file, a method is dynamically invoked by name, using "_write_{file_type}_file".

        Args:
            df: The dataframe to be written out
        """
        s3_config = self.sources_config.s3
        bucket = s3_config.bucket
        file_path = utils.resolve_template(s3_config.file_path, self.options)
        file_type = s3_config.file_type

        logger.info(f"[s3] Started uploading: s3://{bucket}/{file_path}")
        if file_type in ["csv", "json", "parquet"]:
            getattr(self, f"_write_{file_type}_file")(df, f"s3://{bucket}/{file_path}", **self.options)  # type: ignore
        elif file_type == "hdf":
            hdf_options = dict(self.options)
            pickle_protocol = hdf_options.pop("pickle_protocol", None)
            with self._s3_writer(s3_bucket=s3_config.bucket, s3_key=file_path) as target_file, utils.pickle_protocol(protocol=pickle_protocol):
                HdfIO().save(df, target_file, hdf_options)  # type: ignore
        else:
            raise ValueError(f"File type: {file_type} not supported!")
        logger.info(f"[s3] Finished uploading: s3://{bucket}/{file_path}")
