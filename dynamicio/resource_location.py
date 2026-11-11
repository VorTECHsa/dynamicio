"""Resolves a resource definition to the pyarrow file system and path that hold its file."""

import os
from typing import Any, Tuple

import pyarrow.fs as pafs

from dynamicio.config.pydantic.io_resources import DataBackendType, IOEnvironment
from dynamicio.mixins import utils, with_s3


class ResourceLocation:
    """Where the file of a resource definition lives: a pyarrow file system and a path in it.

    Use it when a task has to open a file itself (for example to stream it as arrow tables or to read its parquet footer), but the
    environment, bucket and path must still come from the resource definition, as for a dynamicio read or write.
    """

    @staticmethod
    def of(source_config: IOEnvironment, write: bool = False, **template: Any) -> Tuple[pafs.FileSystem, str]:
        """Return the file system and path of a single-file resource.

        Args:
            source_config: The resource of an `IOConfig` for its environment, e.g. `config.get(source_key="MY_RESOURCE")`.
            write: Whether the file is going to be written. A local write gets the parent directory created, as pyarrow does not, and
                an S3 write gets the `bucket-owner-full-control` ACL, as every dynamicio S3 write does.
            **template: Values for the `{placeholders}` of the path.

        Returns:
            For S3, the per-process file system of the bucket that dynamicio uses for ranged reads (it is released at exit, which avoids the pyarrow
            S3 shutdown hang described in ADR 0004) and the path `bucket/key`. For a local file, a `LocalFileSystem` and the path.

        Raises:
            ValueError: If the resource is not a single local or S3 file, or `template` does not fill every placeholder of the path.
        """
        if source_config.data_backend_type == DataBackendType.s3_file:
            s3 = source_config.s3  # type: ignore[attr-defined]
            return with_s3._s3_filesystem(s3.bucket, write=write), f"{s3.bucket}/{utils.resolve_template(s3.file_path, dict(template))}"  # pylint: disable=protected-access
        if source_config.data_backend_type == DataBackendType.local:
            path = utils.resolve_template(source_config.local.file_path, dict(template))  # type: ignore[attr-defined]
            if write:
                os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
            return pafs.LocalFileSystem(), path
        raise ValueError(f"A resource location is defined for single `local` and `s3_file` resources, not {source_config.data_backend_type.value!r}")
