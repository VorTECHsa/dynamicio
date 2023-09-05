from functools import partial
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

import pandas as pd

from dynamicio.io.resource import BaseResource
from dynamicio.io.serde import CsvSerde, HdfSerde, JsonSerde, ParquetSerde, PickleSerde


class LocalFileResource(BaseResource):
    path: Path
    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}
    injectables: List[str] = ["path"]
    file_type: Optional[Literal["parquet", "hdf", "csv", "json", "pickle"]] = None

    def _read(self) -> pd.DataFrame:
        return self.get_serde()._read(self.path)

    def _write(self, df: pd.DataFrame) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        return self.get_serde()._write(self.path, df)

    def cache_key(self) -> Path:
        if self.test_path is not None:
            return self.test_path
        else:
            return self.path

    @property
    def serde_class(self):
        file_type = self.file_type or (self.path.suffix[1:] if self.path.suffix else None)

        if file_type == "parquet":
            serde_class = partial(ParquetSerde, read_kwargs=self.read_kwargs, write_kwargs=self.write_kwargs)
        elif file_type == "hdf" or file_type == "h5":
            serde_class = partial(HdfSerde, read_kwargs=self.read_kwargs, write_kwargs=self.write_kwargs)
        elif file_type == "csv":
            serde_class = partial(CsvSerde, read_kwargs=self.read_kwargs, write_kwargs=self.write_kwargs)
        elif file_type == "json":
            serde_class = partial(JsonSerde, read_kwargs=self.read_kwargs, write_kwargs=self.write_kwargs)
        elif file_type == "pickle":
            serde_class = partial(PickleSerde, read_kwargs=self.read_kwargs, write_kwargs=self.write_kwargs)
        elif file_type is None:
            raise ValueError(f"File type not specified for {self.path}")
        else:
            raise ValueError(f"Unknown file type {file_type}")

        return serde_class


if __name__ == "__main__":
    df = LocalFileResource(path="tests/fixtures/sample.parquet").read()
    print(df)
