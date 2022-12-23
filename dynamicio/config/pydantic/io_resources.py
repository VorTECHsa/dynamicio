# pylint: disable=no-member, no-self-argument, unused-argument

"""This module contains pylint models for physical data sources (places the bytes are being read from)"""

import enum
import posixpath
import typing

import pydantic

import dynamicio.config.pydantic.table_schema as table_spec


@enum.unique
class DataBackendType(str, enum.Enum):
    """Input file types"""

    # pylint: disable=invalid-name
    local = "local"
    local_batch = "local_batch"
    s3 = "s3"  # is there a difference between 's3' and 's3_file' ?
    s3_file = "s3_file"
    s3_path_prefix = "s3_path_prefix"
    postgres = "postgres"
    athena = "athena"
    kafka = "kafka"


@enum.unique
class FileType(str, enum.Enum):
    """List of supported file formats."""

    # pylint: disable=invalid-name
    parquet = "parquet"
    csv = "csv"
    json = "json"
    hdf = "hdf"
    txt = "txt"


class IOEnvironment(pydantic.BaseModel):
    """A section specifiing an data source backed by a particular data backend"""

    _parent: typing.Optional["dynamicio.config.pydantic.config.IOBinding"] = None  # noqa: F821
    options: typing.Mapping = pydantic.Field(default_factory=dict)
    data_backend_type: DataBackendType = pydantic.Field(alias="type", const=None)

    class Config:
        """Additional pydantic configuration for the model."""

        underscore_attrs_are_private = True

    @property
    def dynamicio_schema(self) -> typing.Union[table_spec.DataframeSchema, table_spec.DataframeSchemaRef, None]:
        """Returns tabular data structure definition for the data source (if available)"""
        return self._parent.dynamicio_schema

    def set_parent(self, parent: "dynamicio.config.pydantic.config.IOBinding"):  # noqa: F821
        """Helper method to set parent config object."""
        assert self._parent is None
        self._parent = parent


class LocalDataSubSection(pydantic.BaseModel):
    """Config section for local data provider"""

    file_path: str
    file_type: FileType


class LocalDataEnvironment(IOEnvironment):
    """The data is provided by local storage"""

    local: LocalDataSubSection


class LocalBatchDataSubSection(pydantic.BaseModel):
    """Config section for local batch data (multiple input files)"""

    path_prefix: str
    file_type: FileType


class LocalBatchDataEnvironment(IOEnvironment):
    """Parent section for local batch (multiple files) config."""

    local: LocalBatchDataSubSection


class S3DataSubSection(pydantic.BaseModel):
    """Config section for S3 data source"""

    file_path: str
    file_type: FileType
    bucket: str


class S3DataEnvironment(IOEnvironment):
    """Parent section for s3 data source config"""

    s3: S3DataSubSection


class S3PathPrefixSubSection(pydantic.BaseModel):
    """Config section for s3 prefix data source (multiple s3 objects)"""

    path_prefix: str
    file_type: FileType
    bucket: str

    @pydantic.root_validator(pre=True)
    def support_legacy_config_path_prefix(cls, values):
        """
        This validator implements support for legacy config format where the
        bucket & path_prefix path could've been passed as a single param in 'bucket' field.

        E.g.
            bucket: "[[ MOCK_BUCKET ]]/data/input/{file_name_to_replace}.hdf"
        """
        bucket = values.get("bucket")
        path_prefix = values.get("path_prefix")
        if (bucket and isinstance(bucket, str) and posixpath.sep in bucket) and (not path_prefix):
            (new_bucket, new_path_prefix) = bucket.split(posixpath.sep, 1)
            values.update(
                {
                    "bucket": new_bucket,
                    "path_prefix": new_path_prefix,
                }
            )
        return values


class S3PathPrefixEnvironment(IOEnvironment):
    """Parent section for the multi-object s3 data source"""

    s3: S3PathPrefixSubSection


class KafkaDataSubSection(pydantic.BaseModel):
    """Kafka configuration section."""

    kafka_server: str
    kafka_topic: str


class KafkaDataEnvironment(IOEnvironment):
    """Parent section for kafka data source config"""

    kafka: KafkaDataSubSection


class PostgresDataSubSection(pydantic.BaseModel):
    """Postgres data source configuration."""

    db_host: str
    db_port: str
    db_name: str
    db_user: str
    db_password: str


class PostgresDataEnvironment(IOEnvironment):
    """Parent section for postgres data source."""

    postgres: PostgresDataSubSection