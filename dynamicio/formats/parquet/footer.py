"""The footer of a parquet file: its arrow schema, its key-value metadata and its number of rows."""

from typing import TYPE_CHECKING, Dict, NamedTuple

import pyarrow as pa

if TYPE_CHECKING:  # pragma: no cover
    pass


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
