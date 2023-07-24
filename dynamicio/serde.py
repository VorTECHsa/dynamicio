from __future__ import annotations

from threading import Lock
from typing import Any, Callable, Dict

import pandas as pd
from pydantic import BaseModel
from uhura.serde import Serde

from dynamicio import utils


class ValidatedSerde(Serde[pd.DataFrame]):
    file_extension = ""  # Does not matter since we are overwriting cache keys

    def __init__(self, validation_callback: Callable[[pd.DataFrame], pd.DataFrame], serde: Serde):
        self.validation_callback = validation_callback
        self._serde = serde

    def read_from_file(self, file: str) -> pd.DataFrame:
        return self.validation_callback(self._serde.read_from_file(file))

    def write_to_file(self, file: str, obj: pd.DataFrame):
        return self._serde.write_to_file(file, self.validation_callback(obj))


class ParquetSerde(BaseModel, Serde[pd.DataFrame]):
    file_extension = ".parquet"

    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}

    def read_from_file(self, file: str) -> pd.DataFrame:
        return pd.read_parquet(file, **self.read_kwargs)

    def write_to_file(self, file: str, obj: pd.DataFrame):
        return obj.to_parquet(file, **self.write_kwargs)


class JsonSerde(BaseModel, Serde[pd.DataFrame]):
    file_extension = ".json"

    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}

    def read_from_file(self, file: str) -> pd.DataFrame:
        df = pd.read_json(file, **self.read_kwargs)
        return df

    def write_to_file(self, file: str, obj: pd.DataFrame):
        return obj.to_json(file, **self.write_kwargs)


class CsvSerde(BaseModel, Serde[pd.DataFrame]):
    file_extension = ".csv"

    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}

    def read_from_file(self, file: str) -> pd.DataFrame:
        df = pd.read_csv(file, **self.read_kwargs)
        return df

    def write_to_file(self, file: str, obj: pd.DataFrame):
        return obj.to_csv(file, **(self.write_kwargs if self.write_kwargs else {"index": False}))


hdf_lock = Lock()


class HdfSerde(BaseModel, Serde[pd.DataFrame]):
    file_extension = ".h5"

    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}

    def read_from_file(self, file: str) -> pd.DataFrame:
        with hdf_lock:
            df = pd.read_hdf(file, **self.read_kwargs)
        return df

    def write_to_file(self, file: str, df: pd.DataFrame):
        with utils.pickle_protocol(protocol=4), hdf_lock:
            return df.to_hdf(file, key="df", mode="w", **self.write_kwargs)
