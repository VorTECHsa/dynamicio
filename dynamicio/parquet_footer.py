"""The footer of a parquet file: its arrow schema, its key-value metadata and its row count, without the rows (change 19 of ADR 0010).

The footer read is the same on the two engines. It reads a ``local`` file with pyarrow, and the end of an ``s3_file`` object with ranged ``GetObject``
calls: one call gets the last ``TAIL_BYTES`` bytes, and a second call gets the rest of a footer that is larger than that.
"""

import io as std_io
from typing import TYPE_CHECKING, Dict, NamedTuple

import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from dynamicio.errors import EngineResourceError, S3KeyNotFoundError
from dynamicio.mixins import utils, with_s3

if TYPE_CHECKING:  # pragma: no cover
    from dynamicio.core import DynamicDataIO


class ParquetFooter(NamedTuple):
    """The footer of a parquet file.

    Attributes:
        schema: The arrow schema of the file, with the key-value metadata, for example the ``pandas`` key.
        rows: The number of rows of the file.
        row_groups: The number of row groups of the file.
    """

    schema: pa.Schema
    rows: int
    row_groups: int

    @property
    def metadata(self) -> Dict[bytes, bytes]:
        """The key-value metadata of the file. A file without metadata gives an empty dict."""
        return dict(self.schema.metadata or {})


class S3Tail(std_io.RawIOBase):
    """A read-only file of an S3 object that gets its bytes with ranged ``GetObject`` calls and keeps the end of the object in memory.

    pyarrow reads the footer of a parquet file from its end, so the first read gets the last ``TAIL_BYTES`` bytes and the size of the object. A read
    before the kept bytes gets the missing range and keeps it.
    """

    TAIL_BYTES = 64 * 1024

    def __init__(self, bucket: str, key: str):
        super().__init__()
        self.bucket, self.key = bucket, key
        try:
            body = self.get(f"bytes=-{self.TAIL_BYTES}")
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") != "InvalidRange":  # an empty object has no last bytes
                raise
            body = {"ContentLength": 0, "Body": std_io.BytesIO(b"")}
        self.size = int(body["ContentRange"].rsplit("/", 1)[1]) if "ContentRange" in body else body["ContentLength"]
        self.kept = body["Body"].read()
        self.start = self.size - len(self.kept)
        self.position = 0

    def get(self, byte_range: str) -> dict:
        """The ``GetObject`` response of a byte range of the object, with the retries of transient S3 errors."""
        return with_s3._retry_transient(  # pylint: disable=protected-access
            lambda: with_s3._shared_s3_client().get_object(Bucket=self.bucket, Key=self.key, Range=byte_range),  # pylint: disable=protected-access
            f"s3://{self.bucket}/{self.key}",
        )

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self.position

    def seek(self, offset: int, whence: int = std_io.SEEK_SET) -> int:
        base = {std_io.SEEK_SET: 0, std_io.SEEK_CUR: self.position, std_io.SEEK_END: self.size}[whence]
        self.position = max(0, base + offset)
        return self.position

    def read(self, size: int = -1) -> bytes:
        end = self.size if size is None or size < 0 else min(self.size, self.position + size)
        if self.position >= end:
            return b""
        if self.position < self.start:
            self.kept = self.get(f"bytes={self.position}-{self.start - 1}")["Body"].read() + self.kept
            self.start = self.position
        data = self.kept[self.position - self.start : end - self.start]
        self.position = end
        return data


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
        source = io.sources_config.data_backend_type.value
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
        """The footer of an open parquet file."""
        return ParquetFooter(schema=file.schema_arrow, rows=file.metadata.num_rows, row_groups=file.metadata.num_row_groups)
