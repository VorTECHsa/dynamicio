"""A buffer that ``close()`` does not close, so a failed boto3 upload can read it again."""

import io as std_io


class KeptOpen(std_io.RawIOBase):
    """A buffer that ``close()`` does not close. The boto3 ``PutObject`` of a small object closes its stream when it ends, also when it fails,
    and the next attempt of the upload must read the buffer again."""

    def __init__(self, buffer: std_io.BytesIO):
        super().__init__()
        self.buffer = buffer

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        return self.buffer.read(size)

    def readinto(self, target) -> int:
        return self.buffer.readinto(target)

    def seek(self, offset: int, whence: int = std_io.SEEK_SET) -> int:
        return self.buffer.seek(offset, whence)

    def tell(self) -> int:
        return self.buffer.tell()

    def close(self) -> None:
        """Leave the buffer open."""
