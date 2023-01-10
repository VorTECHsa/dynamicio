# pylint: disable=no-member, protected-access, too-few-public-methods

"""This module provides mixins that are providing Local FS I/O support."""

import glob
import os
from threading import Lock
from typing import Any, MutableMapping

import pandas as pd  # type: ignore
from fastparquet import ParquetFile, write  # type: ignore
from pyarrow.parquet import read_table, write_table  # type: ignore # pylint: disable=no-name-in-module

from dynamicio.config.pydantic import DataframeSchema, LocalBatchDataEnvironment, LocalDataEnvironment
from dynamicio.mixins import utils

hdf_lock = Lock()


class WithLocal:
    """Handles local I/O operations."""

    schema: DataframeSchema
    sources_config: LocalDataEnvironment
    options: MutableMapping[str, Any]

    def _read_from_local(self) -> pd.DataFrame:
        """Read a local file as a `DataFrame`.

        The configuration object is expected to have two keys:
            - `file_path`
            - `file_type`

        To actually read the file, a method is dynamically invoked by name, using
        "_read_{file_type}_file".

        Returns:
            DataFrame
        """
        local_config = self.sources_config.local
        file_path = utils.resolve_template(local_config.file_path, self.options)
        file_type = local_config.file_type

        return getattr(self, f"_read_{file_type}_file")(file_path, self.schema, **self.options)

    def _write_to_local(self, df: pd.DataFrame):
        """Write a dataframe locally based on the {file_type} of the config_io configuration.

        The configuration object is expected to have two keys:

            - `file_path`
            - `file_type`

        To actually write the file, a method is dynamically invoked by name, using
        "_write_{file_type}_file".

        Args:
            df: The dataframe to be written out.
        """
        local_config = self.sources_config.local
        file_path = utils.resolve_template(local_config.file_path, self.options)
        file_type = local_config.file_type

        getattr(self, f"_write_{file_type}_file")(df, file_path, **self.options)

    @staticmethod
    @utils.allow_options(pd.read_hdf)
    def _read_hdf_file(file_path: str, schema: DataframeSchema, **options: Any) -> pd.DataFrame:
        """Read a HDF file as a DataFrame using `pd.read_hdf`.

        All `options` are passed directly to `pd.read_hdf`.

        Caveats: As HDFs are not thread-safe, we use a Lock on this operation. This, practically means
            that when used with asyncio through `async_read()` HDF files will be read sequentially.
            For more information see: https://pandas.pydata.org/pandas-docs/dev/user_guide/io.html#caveats

        Args:
            file_path: The path to the hdf file to be read.
            options: The pandas `read_hdf` options.

        Returns:
            DataFrame: The dataframe read from the hdf file.
        """
        with hdf_lock:
            df = pd.read_hdf(file_path, **options)

        columns = [column for column in df.columns.to_list() if column in schema.column_names]
        df = df[columns]
        return df

    @staticmethod
    @utils.allow_options(pd.read_csv)
    def _read_csv_file(file_path: str, schema: DataframeSchema, **options: Any) -> pd.DataFrame:
        """Read a CSV file as a DataFrame using `pd.read_csv`.

        All `options` are passed directly to `pd.read_csv`.

        Args:
            file_path: The path to the csv file to be read.
            options: The pandas `read_csv` options.

        Returns:
            DataFrame: The dataframe read from the csv file.
        """
        options["usecols"] = list(schema.column_names)
        return pd.read_csv(file_path, **options)

    @staticmethod
    @utils.allow_options(pd.read_json)
    def _read_json_file(file_path: str, schema: DataframeSchema, **options: Any) -> pd.DataFrame:
        """Read a json file as a DataFrame using `pd.read_hdf`.

        All `options` are passed directly to `pd.read_hdf`.

        Args:
            file_path:
            options:

        Returns:
            DataFrame
        """
        df = pd.read_json(file_path, **options)
        columns = [column for column in df.columns.to_list() if column in schema.column_names]
        df = df[columns]
        return df

    @staticmethod
    def _read_parquet_file(file_path: str, schema: DataframeSchema, **options: Any) -> pd.DataFrame:
        """Read a Parquet file as a DataFrame using `pd.read_parquet`.

        All `options` are passed directly to `pd.read_parquet`.

        Args:
            file_path: The path to the parquet file to be read.
            options: The pandas `read_parquet` options.

        Returns:
            DataFrame: The dataframe read from the parquet file.
        """
        options["columns"] = list(schema.column_names)

        if options.get("engine") == "fastparquet":
            return WithLocal.__read_with_fastparquet(file_path, **options)
        return WithLocal.__read_with_pyarrow(file_path, **options)

    @classmethod
    @utils.allow_options([*utils.args_of(pd.read_parquet), *utils.args_of(read_table)])
    def __read_with_pyarrow(cls, file_path: str, **options: Any) -> pd.DataFrame:
        return pd.read_parquet(file_path, **options)

    @classmethod
    @utils.allow_options([*utils.args_of(pd.read_parquet), *utils.args_of(ParquetFile)])
    def __read_with_fastparquet(cls, file_path: str, **options: Any) -> pd.DataFrame:
        return pd.read_parquet(file_path, **options)

    @staticmethod
    @utils.allow_options([*utils.args_of(pd.DataFrame.to_hdf), *["protocol"]])
    def _write_hdf_file(df: pd.DataFrame, file_path: str, **options: Any):
        """Write a dataframe to hdf using `df.to_hdf`.

        All `options` are passed directly to `df.to_hdf`.

        Caveats: As HDFs are not thread-safe, we use a Lock on this operation. This, practically means
            that when used with asyncio through `async_read()` HDF files will be written sequentially.
            For more information see: https://pandas.pydata.org/pandas-docs/dev/user_guide/io.html#caveats

        Args:
            df: A dataframe write out.
            file_path: The location where the file needs to be written.
            options: The pandas `to_hdf` options.

                - The pandas `to_hdf` options, &;
                - protocol: The pickle protocol to use for writing the hdf file out; a value <=5.
        """
        with utils.pickle_protocol(protocol=options.pop("protocol", None)), hdf_lock:
            df.to_hdf(file_path, key="df", mode="w", **options)

    @staticmethod
    @utils.allow_options(pd.DataFrame.to_csv)
    def _write_csv_file(df: pd.DataFrame, file_path: str, **options: Any):
        """Write a dataframe as a CSV file using `df.to_csv`.

        All `options` are passed directly to `df.to_csv`.

        Args:
            df: A dataframe write out.
            file_path: The location where the file needs to be written.
            options: Options relative to writing a csv file.
        """
        df.to_csv(file_path, **options)

    @staticmethod
    @utils.allow_options(pd.DataFrame.to_json)
    def _write_json_file(df: pd.DataFrame, file_path: str, **options: Any):
        """Write a dataframe as a json file using `df.to_json`.

        All `options` are passed directly to `df.to_json`.

        Args:
            df: A dataframe write out.
            file_path: The location where the file needs to be written.
            options: Options relative to writing a json file.
        """
        df.to_json(file_path, **options)

    @staticmethod
    def _write_parquet_file(df: pd.DataFrame, file_path: str, **options: Any):
        """Write a dataframe as a parquet file using `df.to_parquet`.

        All `options` are passed directly to `df.to_parquet`.

        Args:
            df: A dataframe write out.
            file_path: The location where the file needs to be written.
            options: Options relative to writing a parquet file.
        """
        if options.get("engine") == "fastparquet":
            return WithLocal.__write_with_fastparquet(df, file_path, **options)
        return WithLocal.__write_with_pyarrow(df, file_path, **options)

    @classmethod
    @utils.allow_options([*utils.args_of(pd.DataFrame.to_parquet), *utils.args_of(write_table)])
    def __write_with_pyarrow(cls, df: pd.DataFrame, filepath: str, **options: Any) -> pd.DataFrame:
        return df.to_parquet(filepath, **options)

    @classmethod
    @utils.allow_options([*utils.args_of(pd.DataFrame.to_parquet), *utils.args_of(write)])
    def __write_with_fastparquet(cls, df: pd.DataFrame, filepath: str, **options: Any) -> pd.DataFrame:
        return df.to_parquet(filepath, **options)


class WithLocalBatch(WithLocal):
    """Responsible for batch reading local files."""

    sources_config: LocalBatchDataEnvironment  # type: ignore

    def _read_from_local_batch(self) -> pd.DataFrame:
        """Reads a set of files for a specified file type, concatenates them and returns a dataframe.

        Returns:
            A concatenated dataframe composed of all files read through local_batch.
        """
        local_batch_config = self.sources_config.local

        file_type = local_batch_config.file_type
        filtering_file_type = file_type.value
        if filtering_file_type == "hdf":
            filtering_file_type = "h5"

        files = glob.glob(os.path.join(local_batch_config.path_prefix, f"*.{filtering_file_type}"))

        dfs_to_concatenate = []
        for file in files:
            file_to_load = os.path.join(local_batch_config.path_prefix, file)
            dfs_to_concatenate.append(getattr(self, f"_read_{file_type}_file")(file_to_load, self.schema, **self.options))  # type: ignore

        return pd.concat(dfs_to_concatenate).reset_index(drop=True)
