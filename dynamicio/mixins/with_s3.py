# pylint: disable=no-member, protected-access, too-few-public-methods

"""This module provides mixins that are providing S3 I/O support."""

import os
import tempfile
from contextlib import contextmanager
from typing import Generator

import boto3  # type: ignore
import pandas as pd  # type: ignore
from awscli.clidriver import create_clidriver  # type: ignore
from magic_logger import logger

from dynamicio.config.pydantic import DataframeSchema, S3DataEnvironment, S3PathPrefixEnvironment
from dynamicio.mixins import (
    utils,
    with_local,
)


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


class WithS3PathPrefix(with_local.WithLocal):
    """Handles I/O operations for AWS S3; implements read operations only.

    This mixin assumes that the directories it reads from will only contain a single file-type.
    """

    sources_config: S3PathPrefixEnvironment
    schema: DataframeSchema

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
        if self.options.pop("no_disk_space", False) and file_type == "parquet":
            return self._read_parquet_file(full_path_prefix, self.schema, **self.options)

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


class WithS3File(with_local.WithLocal):
    """Handles I/O operations for AWS S3.

    All files are persisted to disk first using boto3 as this has proven to be faster than reading them into memory.
    Note that reading things into memory is available for csv, json and parquet types only. Unfortunately, until support
    for generic buffer is added to read_hdf, we need to download and persists the file to disk first anyway.

    Options:
        no_disk_space: If `True`, then s3fs + fsspec will be used to read data directly into memory.
    """

    sources_config: S3DataEnvironment
    schema: DataframeSchema

    boto3_client = boto3.client("s3")

    @contextmanager
    def _s3_reader(self, s3_bucket: str, s3_key: str) -> Generator:
        """Contextmanager to abstract reading different file types in S3.

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
    def _s3_writer(self, s3_bucket: str, s3_key: str) -> Generator:
        """Contextmanager to abstract loading different file types to S3.

        Args:
            s3_bucket: The S3 bucket to upload the file to.
            s3_key: The file-path where the target file should be uploaded to.

        Returns:
            The local file path where to actually write the file, to be read and uploaded by boto3.client.
        """
        with tempfile.NamedTemporaryFile("wb") as target_file:
            # Yield local file path to body of `with` statement
            yield target_file
            target_file.flush()

            # Upload the file to S3
            self.boto3_client.upload_file(target_file.name, s3_bucket, s3_key, ExtraArgs={"ACL": "bucket-owner-full-control"})

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

        logger.info(f"[s3] Started downloading: s3://{bucket}/{file_path}")
        if file_type in ["csv", "json", "parquet"] and self.options.pop("no_disk_space", None):
            return getattr(self, f"_read_{file_type}_file")(f"s3://{bucket}/{file_path}", self.schema, **self.options)  # type: ignore
        with self._s3_reader(s3_bucket=bucket, s3_key=file_path) as target_file:  # type: ignore
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
            with self._s3_writer(s3_bucket=bucket, s3_key=file_path) as target_file:  # type: ignore
                self._write_hdf_file(df, target_file.name, **self.options)  # type: ignore
        else:
            raise ValueError(f"File type: {file_type} not supported!")
        logger.info(f"[s3] Finished uploading: s3://{bucket}/{file_path}")
