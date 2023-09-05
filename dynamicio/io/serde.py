"""These are the base serde classes, used for testing & when appropriate for actual IO."""
import pickle
from abc import ABC, abstractmethod
from threading import Lock
from typing import Callable, Optional, TypeVar

import pandas as pd
from uhura.serde import Serde

from dynamicio import utils

SerdeType = TypeVar("SerdeType")


class BaseSerde(ABC, Serde[pd.DataFrame]):
    def __init__(self, validations: Optional[Callable] = None, **kwargs):
        self.validations = validations or []

    def read_from_file(self, file) -> pd.DataFrame:
        df = self._read(file)
        return self.validate(df)

    @abstractmethod
    def _read(self, file) -> pd.DataFrame:
        raise NotImplementedError

    def write_to_file(self, path: str, obj: pd.DataFrame) -> None:
        return self._write(path, obj)

    @abstractmethod
    def _write(self, path: str, obj: pd.DataFrame) -> None:
        raise NotImplementedError

    def validate(self, df: pd.DataFrame):
        """Validation is done here to avoid double validations in the framework."""
        for validator in self.validations:
            validator(df)
        return df


class PickleSerde(BaseSerde):
    def _read(self, file) -> SerdeType:
        with open(file, "rb") as infile:
            return pickle.load(infile)

    def _write(self, file, obj: SerdeType) -> None:
        with open(file, "wb") as outfile:
            pickle.dump(obj, outfile)


class ParquetSerde(BaseSerde):
    file_extension = "_"

    def __init__(self, read_kwargs=None, write_kwargs=None, **kwargs):
        self._read_kwargs = read_kwargs or {}
        self._write_kwargs = write_kwargs or {}
        super().__init__(**kwargs)

    def _read(self, file: str) -> pd.DataFrame:
        return pd.read_parquet(file, **self._read_kwargs)

    def _write(self, file: str, obj: pd.DataFrame) -> None:
        obj.to_parquet(file, **self._write_kwargs)


hdf_lock = Lock()


class HdfSerde(BaseSerde):
    file_extension = "_"

    def __init__(self, read_kwargs=None, write_kwargs=None, **kwargs):
        self._read_kwargs = read_kwargs or {}
        self._write_kwargs = write_kwargs or {}
        super().__init__(**kwargs)

    def _read(self, file: str) -> pd.DataFrame:
        with hdf_lock:
            df = pd.read_hdf(file, **self._read_kwargs)
        return df

    def _write(self, file: str, obj: pd.DataFrame) -> None:
        with utils.pickle_protocol(protocol=4), hdf_lock:
            obj.to_hdf(file, key="df", mode="w", **self._write_kwargs)


class CsvSerde(BaseSerde):
    file_extension = "_"

    def __init__(self, read_kwargs=None, write_kwargs=None, **kwargs):
        self._read_kwargs = read_kwargs or {}
        self._write_kwargs = write_kwargs or {"index": False}
        super().__init__(**kwargs)

    def _read(self, file: str) -> pd.DataFrame:
        return pd.read_csv(file, **self._read_kwargs)

    def _write(self, file: str, obj: pd.DataFrame) -> None:
        obj.to_csv(file, **self._write_kwargs)


class JsonSerde(BaseSerde):
    file_extension = "_"

    def __init__(self, read_kwargs=None, write_kwargs=None, **kwargs):
        self._read_kwargs = read_kwargs or {}
        self._write_kwargs = write_kwargs or {}
        super().__init__(**kwargs)

    def _read(self, file: str) -> pd.DataFrame:
        return pd.read_json(file, **self._read_kwargs)

    def _write(self, file: str, obj: pd.DataFrame) -> None:
        obj.to_json(file, **self._write_kwargs)
