"""This module contains pylint models for physical data sources (places the bytes are being read from)."""

# pylint: disable=no-member, no-self-argument, unused-argument, broad-exception-raised

import enum
import posixpath
from typing import Mapping, Optional, Union

import pydantic
from pydantic import BaseModel

# Application Imports
import dynamicio.config.pydantic.table_schema as table_spec


@enum.unique
class DataBackendType(str, enum.Enum):
    """Input file types."""

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


class IOBinding(BaseModel):
    """A binding for a single i/o object."""

    name: str = pydantic.Field(alias="__binding_name__")
    environments: Mapping[
        str,
        Union["IOEnvironment", "LocalDataEnvironment", "LocalBatchDataEnvironment", "S3DataEnvironment", "S3PathPrefixEnvironment", "KafkaDataEnvironment", "PostgresDataEnvironment"],
    ]
    dynamicio_schema: Union[table_spec.DataframeSchema, None] = pydantic.Field(default=None, alias="schema")

    def get_binding_for_environment(self, environment: str) -> "IOEnvironment":
        """Fetch the IOEnvironment spec for the name provided."""
        return self.environments[environment]

    @pydantic.validator("environments", pre=True, always=True)
    def pick_correct_env_cls(cls, info):
        """This pre-validator picks an appropriate IOEnvironment subclass for the `data_backend_type`."""
        if not isinstance(info, Mapping):
            raise ValueError(f"Environments input should be a dict. Got {info!r} instead.")
        config_cls_overrides = {
            DataBackendType.local: LocalDataEnvironment,
            DataBackendType.local_batch: LocalBatchDataEnvironment,
            DataBackendType.s3: S3DataEnvironment,
            DataBackendType.s3_file: S3DataEnvironment,
            DataBackendType.s3_path_prefix: S3PathPrefixEnvironment,
            DataBackendType.kafka: KafkaDataEnvironment,
            DataBackendType.postgres: PostgresDataEnvironment,
            DataBackendType.athena: AthenaDataEnvironment,
        }
        out_dict = {}
        for env_name, env_data in info.items():
            base_obj: IOEnvironment = IOEnvironment(**env_data)
            override_cls = config_cls_overrides.get(base_obj.data_backend_type)
            if override_cls:
                use_obj = override_cls(**env_data)
            else:
                use_obj = base_obj
            out_dict[env_name] = use_obj
        return out_dict

    @pydantic.root_validator(pre=True)
    def _preprocess_raw_config(cls, values):
        if not isinstance(values, Mapping):
            raise ValueError(f"IOBinding must be a dict at the top level. (got {values!r} instead)")
        remapped_value = {"environments": {}}
        for key, value in values.items():
            if key in ("__binding_name__", "schema"):
                # Passthrough params
                remapped_value[key] = value
            else:
                # Assuming an environment config
                remapped_value["environments"][key] = value
        return remapped_value


class IOEnvironment(BaseModel):
    """A section specifying an data source backed by a particular data backend."""

    _parent: Optional[IOBinding] = None  # noqa: F821
    options: Mapping = pydantic.Field(default_factory=dict)
    data_backend_type: DataBackendType = pydantic.Field(alias="type", const=None)

    class Config:
        """Additional pydantic configuration for the model."""

        underscore_attrs_are_private = True

    @property
    def dynamicio_schema(self) -> Union[table_spec.DataframeSchema, None]:
        """Returns tabular data structure definition for the data source (if available)."""
        if not self._parent:
            raise Exception("Parent field is not set.")
        return self._parent.dynamicio_schema

    def set_parent(self, parent: IOBinding):  # noqa: F821
        """Helper method to set parent config object."""
        assert self._parent is None
        self._parent = parent


class LocalDataSubSection(BaseModel):
    """Config section for local data provider."""

    file_path: str
    file_type: FileType


class LocalDataEnvironment(IOEnvironment):
    """The data is provided by local storage."""

    local: LocalDataSubSection


class LocalBatchDataSubSection(BaseModel):
    """Config section for local batch data (multiple input files)."""

    path_prefix: Optional[str] = None
    dynamic_file_path: Optional[str] = None
    file_type: FileType

    @pydantic.root_validator(pre=True)
    def check_path_fields(cls, values):
        """Check that only one of path_prefix or dynamic_file_path is provided & they meet format requirements."""
        path_prefix = values.get("path_prefix")
        dynamic_file_path = values.get("dynamic_file_path")

        # Check for mutual exclusivity
        if path_prefix and dynamic_file_path:
            raise ValueError("Only one of path_prefix or dynamic_file_path should be provided")

        # Check for path_prefix format
        if path_prefix and not path_prefix.endswith("/"):
            raise ValueError("path_prefix must end with '/'")

        # Check for dynamic_file_path format
        if dynamic_file_path:
            allowed_extensions = [".parquet", ".h5", ".json", ".csv"]
            if not any(dynamic_file_path.endswith(ext) for ext in allowed_extensions):
                raise ValueError("`dynamic_file_path` must end with one of: .parquet, .h5, .json, .csv")

        return values


class LocalBatchDataEnvironment(IOEnvironment):
    """Parent section for local batch (multiple files) config."""

    local: LocalBatchDataSubSection


class S3DataSubSection(BaseModel):
    """Config section for S3 data source."""

    file_path: str
    file_type: FileType
    bucket: str


class S3DataEnvironment(IOEnvironment):
    """Parent section for s3 data source config."""

    s3: S3DataSubSection


class S3PathPrefixSubSection(BaseModel):
    """Config section for s3 prefix data source (multiple s3 objects)."""

    path_prefix: Optional[str] = None
    dynamic_file_path: Optional[str] = None
    file_type: FileType
    bucket: str

    @pydantic.root_validator(pre=True)
    def check_path_fields(cls, values):
        """Check that only one of path_prefix or dynamic_file_path is provided & they meet format requirements."""
        path_prefix = values.get("path_prefix")
        dynamic_file_path = values.get("dynamic_file_path")

        # Check for mutual exclusivity
        if path_prefix and dynamic_file_path:
            raise ValueError("Only one of path_prefix or dynamic_file_path should be provided")

        # Check for at least one being provided
        if not path_prefix and not dynamic_file_path:
            raise ValueError("Either path_prefix or dynamic_file_path must be provided")

        # Check for path_prefix format
        if path_prefix and not path_prefix.endswith("/"):
            raise ValueError("path_prefix must end with '/'")

        # Check for dynamic_file_path format
        if dynamic_file_path:
            allowed_extensions = [".parquet", ".h5", ".json", ".csv"]
            if not any(dynamic_file_path.endswith(ext) for ext in allowed_extensions):
                raise ValueError("`dynamic_file_path` must end with one of: .parquet, .h5, .json, .csv")

        return values

    @pydantic.root_validator(pre=True)
    def support_legacy_config_path_prefix(cls, values):
        """This validator implements support for legacy config format.

        I addresses a case where the bucket & path_prefix path could've been passed as a single param in 'bucket' field.
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
    """Parent section for the multi-object s3 data source."""

    s3: S3PathPrefixSubSection


class KafkaDataSubSection(BaseModel):
    """Kafka configuration section."""

    kafka_server: str
    kafka_topic: str


class KafkaDataEnvironment(IOEnvironment):
    """Parent section for kafka data source config."""

    kafka: KafkaDataSubSection


class PostgresDataSubSection(BaseModel):
    """Postgres data source configuration."""

    db_host: str
    db_port: str
    db_name: str
    db_user: str
    db_password: str


class PostgresDataEnvironment(IOEnvironment):
    """Parent section for postgres data source."""

    postgres: PostgresDataSubSection


class AthenaDataSubSection(BaseModel):
    """AWS Athena configuration section."""

    s3_staging_dir: str
    region_name: str

    # Optional fields, one must be provided via YAML or mixin options
    query: Optional[str] = None


class AthenaDataEnvironment(IOEnvironment):
    """Parent section for Athena source config."""

    athena: AthenaDataSubSection


IOBinding.update_forward_refs()
