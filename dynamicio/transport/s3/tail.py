"""A read-only file of an S3 object for pyarrow, which reads the footer of a parquet object with ranged ``GetObject`` calls."""

import contextlib
import io as std_io
from typing import TYPE_CHECKING, Tuple

from botocore.exceptions import ClientError

from dynamicio.mixins import with_s3

if TYPE_CHECKING:  # pragma: no cover
    pass


class S3Tail(std_io.RawIOBase):
    """A read-only file of an S3 object that gets its bytes with ranged ``GetObject`` calls and keeps the end of the object in memory.

    pyarrow reads the footer of a parquet file from its end, so the first read gets the last ``TAIL_BYTES`` bytes and the size of the object. A read
    before the kept bytes gets the missing range and keeps it.

    Each later ``GetObject`` sends the ``ETag`` of the first one in ``IfMatch``. An object that another writer replaces between two reads thus raises
    the ``PreconditionFailed`` ``ClientError`` of S3, and the footer never mixes the bytes of two versions of the object.
    """

    TAIL_BYTES = 64 * 1024

    def __init__(self, bucket: str, key: str):
        super().__init__()
        self.bucket, self.key = bucket, key
        self.etag = None
        try:
            response, self.kept = self.get(f"bytes=-{self.TAIL_BYTES}")
        except ClientError as error:
            if error.response.get("Error", {}).get("Code") != "InvalidRange":  # an empty object has no last bytes
                raise
            response, self.kept = {"ContentLength": 0}, b""
        self.etag = response.get("ETag")
        self.size = int(response["ContentRange"].rsplit("/", 1)[1]) if "ContentRange" in response else response["ContentLength"]
        self.start = self.size - len(self.kept)
        self.position = 0

    def get(self, byte_range: str) -> Tuple[dict, bytes]:
        """The ``GetObject`` response of a byte range of the object and the bytes of its body, with the retries of transient S3 errors.

        The body is read in the retried operation, so a connection that breaks while the body is read repeats the ranged request.
        """

        def attempt() -> Tuple[dict, bytes]:
            arguments = {"Bucket": self.bucket, "Key": self.key, "Range": byte_range, **({"IfMatch": self.etag} if self.etag else {})}
            response = with_s3._shared_s3_client().get_object(**arguments)  # pylint: disable=protected-access
            with contextlib.closing(response["Body"]) as body:
                return response, body.read()

        return with_s3._retry_transient(attempt, f"s3://{self.bucket}/{self.key}")  # pylint: disable=protected-access

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
            self.kept = self.get(f"bytes={self.position}-{self.start - 1}")[1] + self.kept
            self.start = self.position
        data = self.kept[self.position - self.start : end - self.start]
        self.position = end
        return data
