import enum
import typing
import pathlib

import pydantic

import dynamicio.config.pydantic.table_schema as table_spec


@enum.unique
class DataBackendType(str, enum.Enum):
    """Input file types"""

    local = "local"
    s3 = "s3"
    s3_file = "s3_file"
    s3_path_prefix = "s3_path_prefix"
    postgres = "postgres"
    athena = "athena"
    kafka = "kafka"


@enum.unique
class FileType(str, enum.Enum):
    parquet = "parquet"
    csv = "csv"


class IOEnvironment(pydantic.BaseModel):
    """A section specifiing an data source backed by a particular data backend"""

    parent: typing.Optional["dynamicio.pydantic.config.IOBinding"] = None
    options: typing.Mapping = pydantic.Field(default_factory=dict)
    data_backend_type: DataBackendType = pydantic.Field(alias="type", const=None)

    @property
    def schema(self) -> typing.Union[table_spec.DataframeSchema, table_spec.DataframeSchemaRef, None]:
        return self.parent.dynamicio_schema


class LocalDataSubSection(pydantic.BaseModel):
    file_path: str
    file_type: FileType


class LocalDataEnvironment(IOEnvironment):
    """The data is provided by local storage"""

    local: LocalDataSubSection
