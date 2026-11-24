"""The footer of a parquet file: its arrow schema, its key-value metadata and its row count, without the rows (change 19 of ADR 0010).

The footer read is the same on the two engines. It reads a ``local`` file with pyarrow, and the end of an ``s3_file`` object with ranged ``GetObject``
calls: one call gets the last ``TAIL_BYTES`` bytes, and a second call gets the rest of a footer that is larger than that."""

from typing import TYPE_CHECKING

import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from dynamicio.engines.engine import Engine
from dynamicio.errors import EngineResourceError, S3KeyNotFoundError
from dynamicio.formats.parquet.footer import ParquetFooter
from dynamicio.mixins import utils, with_s3
from dynamicio.transport.s3.tail import S3Tail

if TYPE_CHECKING:  # pragma: no cover
    from dynamicio.core import DynamicDataIO


class ParquetFooters:
    """The footer read of ``DynamicDataIO.footer()``."""

    SOURCES = ("local", "s3_file")

    @classmethod
    def read(cls, io: "DynamicDataIO") -> ParquetFooter:
        """Read the footer of the parquet file of a ``local`` or an ``s3_file`` resource.

        Raises:
            EngineResourceError: The resource is not a ``local`` or ``s3_file`` parquet file.
            FileNotFoundError: A local file does not exist.
            S3KeyNotFoundError: The S3 key does not exist.
        """
        source = Engine.source_of(io)
        section = (io.sources_config.s3 if source.startswith("s3") else io.sources_config.local) if source in cls.SOURCES else None
        file_type = utils.get_file_type_value(section.file_type) if section is not None else None
        if section is None or file_type != "parquet":
            raise EngineResourceError(f"footer() reads the parquet file of a local or s3_file resource, and {io.name!r} is a {file_type} file of a {source} resource")
        path = utils.resolve_template(section.file_path, dict(io.options))
        if source == "local":
            return cls.of(pq.ParquetFile(path))
        return cls.of_s3(section.bucket, path)

    @classmethod
    def of_s3(cls, bucket: str, key: str) -> ParquetFooter:
        """The footer of an S3 object, from ranged reads of its end.

        Raises:
            S3KeyNotFoundError: The key does not exist.
        """
        s3_path = f"s3://{bucket}/{key}"
        try:
            tail = S3Tail(bucket, key)
        except ClientError as error:
            if S3KeyNotFoundError.is_missing_key(error):
                raise S3KeyNotFoundError(s3_path, error) from error
            raise
        with_s3.logger.info(f"[s3] Read the parquet footer of {s3_path}")
        return cls.of(pq.ParquetFile(tail))

    @staticmethod
    def of(file: pq.ParquetFile) -> ParquetFooter:
        """The footer of an open parquet file.

        The schema gets the key-value metadata of the file. The ``ARROW:schema`` key of a pyarrow file holds the metadata of the schema, but polars writes
        the ``metadata`` of ``write_parquet`` only as key-value metadata of the file, outside ``ARROW:schema``.
        """
        metadata = {key: value for key, value in (file.metadata.metadata or {}).items() if key != b"ARROW:schema"}
        schema = file.schema_arrow.with_metadata({**metadata, **(file.schema_arrow.metadata or {})}) if metadata else file.schema_arrow
        return ParquetFooter(schema=schema, rows=file.metadata.num_rows, row_groups=file.metadata.num_row_groups)
