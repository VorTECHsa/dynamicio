"""The ``index_column`` option of a JSON resource, on the two engines."""

import bz2
import gzip
import io
import json
import logging
import lzma
import zipfile
from typing import Any, Callable, Dict, MutableMapping, Optional

logger = logging.getLogger(__name__)


class JsonIndex:
    """The `index_column` option of a JSON resource: the column that holds the row labels of a JSON object with orient `index` or `columns`.

    `DataFrame.to_json` writes the index as the keys of these orients, and `pd.read_json` reads the keys back as the index. With `index_column`,
    a read gives the keys as a column of that name, and a write takes the keys from that column. The file is that of the frame with
    `set_index(index_column)`. The polars engine requires the option for these orients, because a polars frame has no index.
    """

    OPTION = "index_column"
    ORIENTS = ("index", "columns")
    # The compression of each file suffix, as `compression="infer"` of `DataFrame.to_json` gives it
    SUFFIXES = {".gz": "gzip", ".bz2": "bz2", ".xz": "xz", ".zip": "zip", ".zst": "zstd", ".tar": "tar"}
    DECOMPRESSORS: Dict[str, Callable[[bytes], bytes]] = {"gzip": gzip.decompress, "bz2": bz2.decompress, "xz": lzma.decompress}

    @classmethod
    def decompressed(cls, data: bytes, path: str, compression: Any = "infer") -> bytes:
        """The JSON text of a file that `DataFrame.to_json` wrote with the option `compression`, or with the compression of the suffix of `path`.

        Args:
            data: The bytes of the file.
            path: The path or the S3 key of the file, for `compression="infer"`.
            compression: The `compression` option of `DataFrame.to_json`: None, "infer", a method name, or a dict with the key "method".

        Raises:
            ValueError: The compression is not gzip, bz2, xz or zip.
        """
        method = compression.get("method", "infer") if isinstance(compression, dict) else compression
        if method == "infer":
            method = next((name for suffix, name in cls.SUFFIXES.items() if path.lower().endswith(suffix)), None)
        if method is None:
            return data
        if method == "zip":
            with zipfile.ZipFile(io.BytesIO(data)) as archive:
                return archive.read(archive.namelist()[0])
        if method not in cls.DECOMPRESSORS:
            raise ValueError(f"a JSON file of orient index or columns reads with the compression gzip, bz2, xz or zip, and {path} has the compression {method!r}")
        return cls.DECOMPRESSORS[method](data)

    @classmethod
    def pop(cls, options: MutableMapping[str, Any], orient: Optional[str], lines: bool = False) -> Optional[str]:
        """Remove `index_column` from the options of a JSON reader or writer, and return it.

        Args:
            options: The options of the reader or the writer. The method removes `index_column` from them.
            orient: The orient of the file. None is the `columns` default of `pd.read_json` and `DataFrame.to_json`.
            lines: Whether the file holds JSON lines.

        Raises:
            ValueError: `index_column` is not a string, or the orient is not `index` or `columns`, or `lines` is true.
        """
        index_column = options.pop(cls.OPTION, None)
        if index_column is None:
            return None
        if not isinstance(index_column, str):
            raise ValueError(f"`{cls.OPTION}` must be the name of a column, got {index_column!r}")
        if (orient or "columns") not in cls.ORIENTS or lines:
            raise ValueError(f"`{cls.OPTION}` applies to a JSON object with orient index or columns only, not to orient {orient!r} with lines={lines!r}")
        return index_column

    @staticmethod
    def records(data: bytes, orient: str, index_column: str) -> bytes:
        """The JSON records of a JSON object with orient `index` or `columns`. The key of each row goes in the column `index_column`, first.

        Orient `columns` holds one object for each column. The rows are the keys of these objects, in the order of their first use, and a key that a
        column does not have gives a null, as on pandas.

        Raises:
            ValueError: The JSON is not an object of objects, or the object has a column of the name `index_column`, as `reset_index(names=index_column)`
                raises on pandas. The key of a row would otherwise replace the value of that column.
        """
        document = json.loads(data)
        if not isinstance(document, dict) or not all(isinstance(values, dict) for values in document.values()):
            raise ValueError(f"a JSON file of orient {orient!r} must hold an object of objects, and this one holds a {type(document).__name__} of other values")
        if orient == "columns":
            if index_column in document:
                raise ValueError(f"cannot insert {index_column}, already exists: the JSON object has a column of the name of `{JsonIndex.OPTION}`")
            keys = dict.fromkeys(key for values in document.values() for key in values)
            document = {key: {column: values.get(key) for column, values in document.items()} for key in keys}
        elif any(index_column in row for row in document.values()):
            raise ValueError(f"cannot insert {index_column}, already exists: the JSON object has a column of the name of `{JsonIndex.OPTION}`")
        return json.dumps([{index_column: key, **row} for key, row in document.items()]).encode()
