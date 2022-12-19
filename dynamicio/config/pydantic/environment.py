import enum
import posixpath
import typing

import pydantic

import dynamicio.config.pydantic.table_schema as table_spec


@enum.unique
class DataBackendType(str, enum.Enum):
    """Input file types"""

    local = "local"
    local_batch = "local_batch"
    s3 = "s3"  # XXX: is there a difference between 's3' and 's3_file' ?
    s3_file = "s3_file"
    s3_path_prefix = "s3_path_prefix"
    postgres = "postgres"
    athena = "athena"
    kafka = "kafka"


@enum.unique
class FileType(str, enum.Enum):
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
        underscore_attrs_are_private = True

    @property
    def dynamicio_schema(self) -> typing.Union[table_spec.DataframeSchema, table_spec.DataframeSchemaRef, None]:
        return self._parent.dynamicio_schema

    def set_parent(self, parent: "dynamicio.config.pydantic.config.IOBinding"):  # noqa: F821
        assert self._parent is None
        self._parent = parent


class LocalDataSubSection(pydantic.BaseModel):
    file_path: str
    file_type: FileType


class LocalDataEnvironment(IOEnvironment):
    """The data is provided by local storage"""

    local: LocalDataSubSection


class LocalBatchDataSubSection(pydantic.BaseModel):
    path_prefix: str
    file_type: FileType


class LocalBatchDataEnvironment(IOEnvironment):
    local: LocalBatchDataSubSection


class S3DataSubSection(pydantic.BaseModel):
    file_path: str
    file_type: FileType
    bucket: str


class S3DataEnvironment(IOEnvironment):
    s3: S3DataSubSection


class S3PathPrefixSubSection(pydantic.BaseModel):
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
    s3: S3PathPrefixSubSection


class KafkaDataSubSection(pydantic.BaseModel):
    kafka_server: str
    kafka_topic: str


class KafkaDataEnvironment(IOEnvironment):
    kafka: KafkaDataSubSection


class PostgresDataSubSection(pydantic.BaseModel):

    db_host: str
    db_port: str
    db_name: str
    db_user: str
    db_password: str


class PostgresDataEnvironment(IOEnvironment):
    postgres: PostgresDataSubSection
