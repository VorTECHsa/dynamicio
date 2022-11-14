"""Implements all dynamic(i/o) mixins."""
# pylint: disable=no-member, protected-access, too-few-public-methods
__all__ = [
    "WithLocal",
    "WithS3File",
    "WithS3PathPrefix",
    "WithLocalBatch",
    "WithPostgres",
    "WithKafka",
    "args_of",
    "get_string_template_field_names",
    "resolve_template",
]

import csv
import glob
import inspect
import os
import string
import tempfile
from contextlib import contextmanager
from functools import wraps
from threading import Lock
from types import FunctionType
from typing import Any, Callable, Collection, Dict, Generator, Iterable, Mapping, MutableMapping, Optional, Union

import boto3  # type: ignore
import pandas as pd  # type: ignore
import simplejson
from awscli.clidriver import create_clidriver  # type: ignore
from fastparquet import ParquetFile, write  # type: ignore
from kafka import KafkaProducer  # type: ignore
from magic_logger import logger
from pyarrow.parquet import read_table, write_table  # type: ignore # pylint: disable=no-name-in-module
from sqlalchemy import BigInteger, Boolean, Column, Date, DateTime, Float, Integer, String, create_engine  # type: ignore
from sqlalchemy.ext.declarative import declarative_base  # type: ignore
from sqlalchemy.orm import Query  # type: ignore
from sqlalchemy.orm.decl_api import DeclarativeMeta  # type: ignore
from sqlalchemy.orm.session import Session as SqlAlchemySession  # type: ignore
from sqlalchemy.orm.session import sessionmaker  # type: ignore

Session = sessionmaker(autoflush=True)

Base = declarative_base()
_type_lookup = {
    "bool": Boolean,
    "boolean": Boolean,
    "object": String(64),
    "int64": Integer,
    "float64": Float,
    "int": Integer,
    "date": Date,
    "datetime64[ns]": DateTime,
    "bigint": BigInteger,
}

hdf_lock = Lock()


@contextmanager
def pickle_protocol(protocol: Optional[int]):
    """Downgrade to the provided pickle protocol within the context manager.

    Args:
        protocol: The number of the protocol HIGHEST_PROTOCOL to downgrade to. Defaults to 4, which covers python 3.4 and higher.
    """
    import pickle  # pylint: disable=import-outside-toplevel

    previous = pickle.HIGHEST_PROTOCOL
    try:
        pickle.HIGHEST_PROTOCOL = 4
        if protocol:
            pickle.HIGHEST_PROTOCOL = protocol
        yield
    finally:
        pickle.HIGHEST_PROTOCOL = previous


@contextmanager
def session_for(connection_string: str) -> Generator[SqlAlchemySession, None, None]:
    """Connect to a database using `connection_string` and returns an active session to that connection.

    Args:
        connection_string:

    Yields:
        Active session
    """
    engine = create_engine(connection_string)
    session = Session(bind=engine)

    try:
        yield session
    finally:
        session.close()  # pylint: disable=no-member


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


def allow_options(options: Union[Iterable, FunctionType]):
    """Validate **options for a decorated reader function.

    Args:
        options: A set of valid options for a reader (e.g. `pandas.read_parquet` or `pandas.read_csv`)

    Returns:
        read_with_valid_options: The input function called with modified options.
    """

    def _filter_out_irrelevant_options(kwargs: Mapping, valid_options: Iterable):
        filtered_options = {}
        invalid_options = {}
        for key_arg in kwargs.keys():
            if key_arg in valid_options:
                filtered_options[key_arg] = kwargs[key_arg]
            else:
                invalid_options[key_arg] = kwargs[key_arg]
        if len(invalid_options) > 0:
            logger.warning(
                f"Options {invalid_options} were not used because they were not supported by the read or write method configured for this source. "
                "Check if you expected any of those to have been used by the operation!"
            )
        return filtered_options

    def read_with_valid_options(func):
        @wraps(func)
        def _(*args, **kwargs):
            if callable(options):
                return func(*args, **_filter_out_irrelevant_options(kwargs, args_of(options)))
            return func(*args, **_filter_out_irrelevant_options(kwargs, options))

        return _

    return read_with_valid_options


def args_of(func):
    """Retrieve allowed options for a given function.

    Args:
        func: A function like, e.g., pd.read_csv

    Returns:
        A set of allowed options
    """
    return set(inspect.signature(func).parameters.keys())


def get_string_template_field_names(s: str) -> Collection[str]:  # pylint: disable=C0103
    """Given a string `s`, it parses the string to identify any template fields and returns the names of those fields.

     If `s` is not a string template, the returned `Collection` is empty.

    Args:
        s:

    Returns:
        Collection[str]

    Example:

        >>> get_string_template_field_names("abc{def}{efg}")
        ["def", "efg"]
        >>> get_string_template_field_names("{0}-{1}")
        ["0", "1"]
        >>> get_string_template_field_names("hello world")
        []
    """
    # string.Formatter.parse returns a 4-tuple of:
    # `literal_text`, `field_name`, `form_at_spec`, `conversion`
    # More info here https://docs.python.org/3.8/library/string.html#string.Formatter.parse
    field_names = [group[1] for group in string.Formatter().parse(s) if group[1] is not None]

    return field_names


def resolve_template(path: str, options: MutableMapping[str, Any]) -> str:  # pylint: disable=C0103
    """Given a string `path`, it attempts to replace all templates fields with values provided in `options`.

    If `path` is not a string template, `path` is returned.

    Args:
        path: A string which is either a template, e.g. /path/to/file/{replace_me}.h5 or just a path /path/to/file/dont_replace_me.h5
        options: A dynamic name for the "replace_me" field in the templated string. e.g. {"replace_me": "name_of_file"}

    Returns:
        str: Returns a static path replaced with the value in the options mapping.

    Raises:
        ValueError: if any template fields in s are not named using valid Python identifiers
        ValueError: if a given template field cannot be resolved in `options`
    """
    fields = get_string_template_field_names(path)

    if len(fields) == 0:
        return path

    if not all(field.isidentifier() for field in fields):
        raise ValueError(f"Expected valid Python identifiers, found {fields}")

    if not all(field in options for field in fields):
        raise ValueError(f"Expected values for all fields in {fields}, found {list(options.keys())}")

    path = path.format(**{field: options[field] for field in fields})
    for field in fields:
        options.pop(field)

    return path


class WithLocal:
    """Handles local I/O operations."""

    sources_config: Mapping
    schema: Mapping
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
        local_config = self.sources_config["local"]
        file_path = resolve_template(local_config["file_path"], self.options)
        file_type = local_config["file_type"]

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
        local_config = self.sources_config["local"]
        file_path = resolve_template(local_config["file_path"], self.options)
        file_type = local_config["file_type"]

        getattr(self, f"_write_{file_type}_file")(df, file_path, **self.options)

    @staticmethod
    @allow_options(pd.read_hdf)
    def _read_hdf_file(file_path: str, schema: Mapping[str, str], **options: Any) -> pd.DataFrame:
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

        columns = [column for column in df.columns.to_list() if column in schema.keys()]
        df = df[columns]
        return df

    @staticmethod
    @allow_options(pd.read_csv)
    def _read_csv_file(file_path: str, schema: Mapping[str, str], **options: Any) -> pd.DataFrame:
        """Read a CSV file as a DataFrame using `pd.read_csv`.

        All `options` are passed directly to `pd.read_csv`.

        Args:
            file_path: The path to the csv file to be read.
            options: The pandas `read_csv` options.

        Returns:
            DataFrame: The dataframe read from the csv file.
        """
        options["usecols"] = list(schema.keys())
        return pd.read_csv(file_path, **options)

    @staticmethod
    @allow_options(pd.read_json)
    def _read_json_file(file_path: str, schema: Mapping[str, str], **options: Any) -> pd.DataFrame:
        """Read a json file as a DataFrame using `pd.read_hdf`.

        All `options` are passed directly to `pd.read_hdf`.

        Args:
            file_path:
            options:

        Returns:
            DataFrame
        """
        df = pd.read_json(file_path, **options)
        columns = [column for column in df.columns.to_list() if column in schema.keys()]
        df = df[columns]
        return df

    @staticmethod
    def _read_parquet_file(file_path: str, schema: Mapping[str, str], **options: Any) -> pd.DataFrame:
        """Read a Parquet file as a DataFrame using `pd.read_parquet`.

        All `options` are passed directly to `pd.read_parquet`.

        Args:
            file_path: The path to the parquet file to be read.
            options: The pandas `read_parquet` options.

        Returns:
            DataFrame: The dataframe read from the parquet file.
        """
        options["columns"] = list(schema.keys())

        if options.get("engine") == "fastparquet":
            return WithLocal.__read_with_fastparquet(file_path, **options)
        return WithLocal.__read_with_pyarrow(file_path, **options)

    @classmethod
    @allow_options([*args_of(pd.read_parquet), *args_of(read_table)])
    def __read_with_pyarrow(cls, file_path: str, **options: Any) -> pd.DataFrame:
        return pd.read_parquet(file_path, **options)

    @classmethod
    @allow_options([*args_of(pd.read_parquet), *args_of(ParquetFile)])
    def __read_with_fastparquet(cls, file_path: str, **options: Any) -> pd.DataFrame:
        return pd.read_parquet(file_path, **options)

    @staticmethod
    @allow_options([*args_of(pd.DataFrame.to_hdf), *["protocol"]])
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
        with pickle_protocol(protocol=options.pop("protocol", None)), hdf_lock:
            df.to_hdf(file_path, key="df", mode="w", **options)

    @staticmethod
    @allow_options(pd.DataFrame.to_csv)
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
    @allow_options(pd.DataFrame.to_json)
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
    @allow_options([*args_of(pd.DataFrame.to_parquet), *args_of(write_table)])
    def __write_with_pyarrow(cls, df: pd.DataFrame, filepath: str, **options: Any) -> pd.DataFrame:
        return df.to_parquet(filepath, **options)

    @classmethod
    @allow_options([*args_of(pd.DataFrame.to_parquet), *args_of(write)])
    def __write_with_fastparquet(cls, df: pd.DataFrame, filepath: str, **options: Any) -> pd.DataFrame:
        return df.to_parquet(filepath, **options)


class WithLocalBatch(WithLocal):
    """Responsible for batch reading local files."""

    def _read_from_local_batch(self) -> pd.DataFrame:
        """Reads a set of files for a specified file type, concatenates them and returns a dataframe.

        Returns:
            A concatenated dataframe composed of all files read through local_batch.
        """
        local_batch_config = self.sources_config["local"]

        file_type = local_batch_config["file_type"]
        filtering_file_type = file_type
        if filtering_file_type == "hdf":
            filtering_file_type = "h5"

        files = glob.glob(f"{local_batch_config['path_prefix']}/*.{filtering_file_type}")

        dfs_to_concatenate = []
        for file in files:
            file_to_load = os.path.join(local_batch_config["path_prefix"], file)
            dfs_to_concatenate.append(getattr(self, f"_read_{file_type}_file")(file_to_load, self.schema, **self.options))  # type: ignore

        return pd.concat(dfs_to_concatenate).reset_index(drop=True)


class WithS3PathPrefix(WithLocal):
    """Handles I/O operations for AWS S3; implements read operations only.

    This mixin assumes that the directories it reads from will only contain a single file-type.
    """

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
        s3_config = self.sources_config["s3"]
        if "path_prefix" not in s3_config:
            raise ValueError("`path_prefix` is required to write multiple files to an S3 key")

        file_type = s3_config["file_type"]
        if file_type != "parquet":
            raise ValueError(f"File type not supported: {file_type}, only parquet files can be written to an S3 key")
        if "partition_cols" not in self.options:
            raise ValueError("`partition_cols` is required as an option to write partitioned parquet files to S3")

        bucket = s3_config["bucket"]
        path_prefix = s3_config["path_prefix"]
        full_path_prefix = resolve_template(f"s3://{bucket}/{path_prefix}", self.options)

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
        s3_config = self.sources_config["s3"]
        if "path_prefix" not in s3_config:
            raise ValueError("`path_prefix` is required to read multiple files from an S3 source")

        file_type = s3_config["file_type"]
        if file_type not in {"parquet", "csv", "hdf", "json"}:
            raise ValueError(f"File type not supported: {file_type}")

        bucket = s3_config["bucket"]
        path_prefix = s3_config["path_prefix"]
        full_path_prefix = resolve_template(f"s3://{bucket}/{path_prefix}", self.options)

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


class WithS3File(WithLocal):
    """Handles I/O operations for AWS S3.

    All files are persisted to disk first using boto3 as this has proven to be faster than reading them into memory.
    Note that reading things into memory is available for csv, json and parquet types only. Unfortunately, until support
    for generic buffer is added to read_hdf, we need to download and persists the file to disk first anyway.

    Options:
        no_disk_space: If `True`, then s3fs + fsspec will be used to read data directly into memory.
    """

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
        s3_config = self.sources_config["s3"]
        if "file_path" not in s3_config:
            raise ValueError("`file_path` is required for reading a file from an S3 source")

        file_type = s3_config["file_type"]
        file_path = resolve_template(s3_config["file_path"], self.options)
        bucket = s3_config["bucket"]

        logger.info(f"[s3] Started downloading: s3://{s3_config['bucket']}/{file_path}")
        if file_type in ["csv", "json", "parquet"] and self.options.pop("no_disk_space", None):
            return getattr(self, f"_read_{file_type}_file")(f"s3://{s3_config['bucket']}/{file_path}", self.schema, **self.options)  # type: ignore
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
        s3_config = self.sources_config["s3"]
        file_path = resolve_template(s3_config["file_path"], self.options)
        file_type = s3_config["file_type"]

        logger.info(f"[s3] Started uploading: s3://{s3_config['bucket']}/{file_path}")
        if file_type in ["csv", "json", "parquet"]:
            getattr(self, f"_write_{file_type}_file")(df, f"s3://{s3_config['bucket']}/{file_path}", **self.options)  # type: ignore
        elif file_type == "hdf":
            with self._s3_writer(s3_bucket=s3_config["bucket"], s3_key=file_path) as target_file:  # type: ignore
                self._write_hdf_file(df, target_file.name, **self.options)  # type: ignore
        else:
            raise ValueError(f"File type: {file_type} not supported!")
        logger.info(f"[s3] Finished uploading: s3://{s3_config['bucket']}/{file_path}")


class WithPostgres:
    """Handles I/O operations for Postgres.

    Args:
       - options:
           - `truncate_and_append: bool`: If set to `True`, truncates the table and then appends the new rows. Otherwise, it drops the table and recreates it with the new rows.
    """

    sources_config: Mapping
    schema: Mapping
    options: MutableMapping[str, Any]

    def _read_from_postgres(self) -> pd.DataFrame:
        """Read data from postgres as a `DataFrame`.

        The configuration object is expected to have the following keys:
            - `db_user`
            - `db_password`
            - `db_host`
            - `db_port`
            - `db_name`

        Returns:
            DataFrame
        """
        postgres_config = self.sources_config["postgres"]
        db_user = postgres_config["db_user"]
        db_password = postgres_config["db_password"]
        db_host = postgres_config["db_host"]
        db_port = postgres_config["db_port"]
        db_name = postgres_config["db_name"]

        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

        sql_query = self.options.get("sql_query")

        query = None

        if "schema" not in self.sources_config:
            schema_dict = self.schema
        else:
            schema_dict = self.sources_config["schema"]
        schema_name = self.sources_config["name"]

        model = self._generate_model_from_schema(schema_dict, schema_name)

        query = Query(self._get_table_columns(model))
        if sql_query:
            query = sql_query

        with session_for(connection_string) as session:
            return self._read_database(session, query, **self.options)

    @staticmethod
    def _generate_model_from_schema(schema_dict: Mapping, schema_name: str) -> DeclarativeMeta:
        json_cls_schema: Dict[str, Any] = {"tablename": schema_name, "columns": []}

        for col, dtype in schema_dict.items():
            new_col = {"name": col}

            if dtype in _type_lookup:
                new_col.update({"name": col, "type": _type_lookup[dtype]})
                json_cls_schema["columns"].append(new_col)

        class_name = "".join(word.capitalize() or "_" for word in schema_name.split("_")) + "Model"

        class_dict = {"clsname": class_name, "__tablename__": schema_name, "__table_args__": {"extend_existing": True}}
        class_dict.update({column["name"]: Column(column["type"], primary_key=True) if idx == 0 else Column(column["type"]) for idx, column in enumerate(json_cls_schema["columns"])})

        generated_model = type(class_name, (Base,), class_dict)
        return generated_model

    @staticmethod
    def _get_table_columns(model):
        tables_colums = []
        if model:
            for col in list(model.__table__.columns):
                tables_colums.append(getattr(model, col.name))
        return tables_colums

    @staticmethod
    @allow_options(pd.read_sql)
    def _read_database(session: SqlAlchemySession, query: Union[str, Query], **options: Any) -> pd.DataFrame:
        """Run `query` against active `session` and returns the result as a `DataFrame`.

        Args:
            session: Active session
            query: If a `Query` object is given, it should be unbound. If a `str` is given, the
                value is used as-is.

        Returns:
            DataFrame
        """
        if options.get("model"):
            options.pop("model")

        if isinstance(query, Query):
            query = query.with_session(session).statement
        return pd.read_sql(sql=query, con=session.get_bind(), **options)

    def _write_to_postgres(self, df: pd.DataFrame):
        """Write a dataframe to postgres based on the {file_type} of the config_io configuration.

        Args:
            df: The dataframe to be written
        """
        postgres_config = self.sources_config["postgres"]
        db_user = postgres_config["db_user"]
        db_password = postgres_config["db_password"]
        db_host = postgres_config["db_host"]
        db_port = postgres_config["db_port"]
        db_name = postgres_config["db_name"]

        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"

        schema_dict = self.sources_config["schema"]
        schema_name = self.sources_config["name"]
        model = self._generate_model_from_schema(schema_dict, schema_name)

        is_truncate_and_append = self.options.get("truncate_and_append", False)

        with session_for(connection_string) as session:
            self._write_to_database(session, model.__tablename__, df, is_truncate_and_append)  # type: ignore

    @staticmethod
    def _write_to_database(session: SqlAlchemySession, table_name: str, df: pd.DataFrame, is_truncate_and_append: bool):
        """Write a dataframe to any database provided a session with a data model and a table name.

        Args:
            session: Generated from a data model and a table name
            table_name: The name of the table to read from a DB
            df: The dataframe to be written out
            is_truncate_and_append: Supply to truncate the table and append new rows to it; otherwise, delete and replace
        """
        if is_truncate_and_append:
            session.execute(f"TRUNCATE TABLE {table_name};")

            # Below is a speedup hack in place of `df.to_csv` with the multipart option. As of today, even with
            # `method="multi"`, uploading to Postgres is painfully slow. Hence, we're resorting to dumping the file as
            # csv and using Postgres's CSV import function.
            # https://stackoverflow.com/questions/2987433/how-to-import-csv-file-data-into-a-postgresql-table
            with tempfile.NamedTemporaryFile(mode="r+") as temp_file:
                df.to_csv(temp_file, index=False, header=False, sep="\t", doublequote=False, escapechar="\\", quoting=csv.QUOTE_NONE)
                temp_file.flush()
                temp_file.seek(0)

                cur = session.connection().connection.cursor()
                cur.copy_from(temp_file, table_name, columns=df.columns, null="")
        else:
            df.to_sql(name=table_name, con=session.get_bind(), if_exists="replace", index=False)

        session.commit()


class WithKafka:
    """Handles I/O operations for Kafka.

    Args:
        - options:
            - Standard: Keyword-arguments passed to the KafkaProducer constructor (see `KafkaProducer.DEFAULT_CONFIG.keys()`).
             - Additional Options:

                - `key_generator: Callable[[Any, Mapping], T]`: defines the keying policy to be used for sending keyed-messages to Kafka. It is a `Callable` that takes a
                `tuple(idx, row)` and returns a string that will serve as the message's key, invoked prior to serialising the key. It defaults to the dataframe's index
                (which may not be composed of unique values or string type keys). It goes hand in hand with the default `key-serialiser`, which assumes that the keys
                are strings and encode's them as such.

                - `key_serializer: Callable[T, bytes]`: Custom key serialiser; if not provided, a default key-serializer will be used, applied on a string-key (unless key is None).

                N.B. Providing a custom key-generator that generates a non-string key is best provided alongside a custom key-serializer best suited to handle the custom key-type.

                - `document_transformer: Callable[[Mapping[Any, Any]`: Manipulates the messages/rows sent to Kafka as values. It is  a `Callable` taking a `Mapping` as its only
                argument and return a `Mapping`, then this callable will be invoked prior to serializing each document. This can be used, for example, to add metadata to each
                document that will be written to the target  Kafka topic.

                - `value_serializer: Callable[Mapping, bytes]`: Custom value serialiser; if not provided, a default value-serializer will be used applied on a Mapping..

    Example:
        >>> # Given
        >>> keyed_test_df = pd.DataFrame.from_records(
        >>>     [
        >>>         ["key-01", "cm_1", "id_1", 1000, "ABC"],
        >>>         ["key-02", "cm_2", "id_2", 1000, "ABC"],
        >>>         ["key-03", "cm_3", "id_3", 1000, "ABC"],
        >>>     ],
        >>>     columns=["key", "id", "foo", "bar", "baz"],
        >>> ).set_index("key")
        >>>
        >>> kafka_cloud_config = IOConfig(
        >>>     path_to_source_yaml=(os.path.join(constants.TEST_RESOURCES, "processed.yaml")),
        >>>     env_identifier="CLOUD",
        >>>     dynamic_vars=constants,
        >>> ).get(source_key="WRITE_TO_KAFKA_JSON")
        >>>
        >>> write_kafka_io = WriteKafkaIO(kafka_cloud_config, key_generator=lambda key, _: key, document_transformer=lambda doc: doc["new_field"]="new_value")
        >>>
        >>> # When
        >>> with patch.object(mixins, "KafkaProducer") as mock__kafka_producer:
        >>>     mock__kafka_producer.DEFAULT_CONFIG = KafkaProducer.DEFAULT_CONFIG
        >>>     mock_producer = MockKafkaProducer()
        >>>     mock__kafka_producer.return_value = mock_producer
        >>>     write_kafka_io.write(keyed_test_df)
        >>>
        >>> # Then
        >>> assert mock_producer.my_stream == [
        >>>     {"key": "key-01", "value": {"bar": 1000, "baz": "ABC", "foo": "id_1", "id": "cm_1", "new_field": "new_value"}},
        >>>     {"key": "key-02", "value": {"bar": 1000, "baz": "ABC", "foo": "id_2", "id": "cm_2", "new_field": "new_value"}},
        >>>     {"key": "key-03", "value": {"bar": 1000, "baz": "ABC", "foo": "id_3", "id": "cm_3", "new_field": "new_value"}},
        >>> ]
    """

    sources_config: Mapping
    schema: Mapping
    options: MutableMapping[str, Any]
    __kafka_config: Optional[Mapping] = None
    __producer: Optional[KafkaProducer] = None
    __key_generator: Optional[Callable[[Any, Mapping[Any, Any]], Optional[str]]] = None
    __document_transformer: Optional[Callable[[Mapping[Any, Any]], Mapping[Any, Any]]] = None

    def _write_to_kafka(self, df: pd.DataFrame) -> None:
        """Given a dataframe where each row is a message to be sent to a Kafka Topic, iterate through all rows and send them to a Kafka topic.

         The topic is defined in `self.sources_config["kafka"]` and using a kafka producer, which is flushed at the
         end of this process.

        Args:
            df: A dataframe where each row is a message to be sent to a Kafka Topic.
        """
        if self.__key_generator is None:
            self.__key_generator = lambda idx, __: idx  # default key generator uses the dataframe's index
            if self.options.get("key_generator") is not None:
                self.__key_generator = self.options.pop("key_generator")

        if self.__document_transformer is None:
            self.__document_transformer = lambda value: value
            if self.options.get("document_transformer") is not None:
                self.__document_transformer = self.options.pop("document_transformer")

        if self.__producer is None:
            self.__producer = self._get_producer(self.sources_config["kafka"]["kafka_server"], **self.options)

        self._send_messages(df=df, topic=self.sources_config["kafka"]["kafka_topic"])

    @allow_options(KafkaProducer.DEFAULT_CONFIG.keys())
    def _get_producer(self, server: str, **options: MutableMapping[str, Any]) -> KafkaProducer:
        """Generate and return a Kafka Producer.

        Default options are used to generate the producer. Specifically:
            - `bootstrap_servers`: Passed on through the source_config
            - `value_serializer`: Uses a default_value_serializer defined in this mixin

        More options can be added to the producer by passing them as keyword arguments, through valid options.

        These can also override the default options.

        Args:
            server: The host name.
            **options: Keyword arguments to pass to the KafkaProducer.

        Returns:
            A Kafka producer instance.
        """
        self.__kafka_config = {
            **{
                "bootstrap_servers": server,
                "compression_type": "snappy",
                "key_serializer": self._default_key_serializer,
                "value_serializer": self._default_value_serializer,
            },
            **options,
        }
        return KafkaProducer(**self.__kafka_config)

    def _send_messages(self, df: pd.DataFrame, topic: str) -> None:
        logger.info(f"Sending {len(df)} messages to Kafka topic:{topic}.")

        messages = df.reset_index(drop=True).to_dict("records")
        for idx, message in zip(df.index.values, messages):
            self.__producer.send(topic, key=self.__key_generator(idx, message), value=self.__document_transformer(message))  # type: ignore

        self.__producer.flush()  # type: ignore

    @staticmethod
    def _default_key_serializer(key: Optional[str]) -> Optional[bytes]:
        if key:
            return key.encode("utf-8")
        return None

    @staticmethod
    def _default_value_serializer(value: Mapping) -> bytes:
        return simplejson.dumps(value, ignore_nan=True).encode("utf-8")

    def _read_from_kafka(self) -> Iterable[Mapping]:  # type: ignore
        """Read messages from a Kafka Topic and convert them to separate dataframes.

        Returns:
            Multiple dataframes, one per message read from the Kafka topic of interest.
        """
        # TODO: Implement kafka reader
