from __future__ import annotations

from threading import Lock
from typing import Any, Callable, Dict

import pandas as pd
from uhura.serde import Serde

from dynamicio import utils
from dynamicio.inject import check_injections


class ParquetSerde(Serde[pd.DataFrame]):
    file_extension = ".parquet"  # Used in cache key
    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}
    validation_callback: Callable[[pd.DataFrame], pd.DataFrame]

    def __init__(
        self,
        read_kwargs: dict[str, Any],
        write_kwargs: dict[str, Any],
        validation_callback: Callable[[pd.DataFrame], pd.DataFrame],
    ):
        self.validation_callback = validation_callback
        self.read_kwargs = read_kwargs
        self.write_kwargs = write_kwargs

    def read_from_file(self, file: str) -> pd.DataFrame:
        check_injections(file)
        df = pd.read_parquet(file, **self.read_kwargs)
        return self.validation_callback(df)

    def write_to_file(self, file: str, obj: pd.DataFrame):
        check_injections(file)
        obj = self.validation_callback(obj)
        return obj.to_parquet(file, **self.write_kwargs)


class JsonSerde(Serde[pd.DataFrame]):
    file_extension = ".json"  # Used in cache key
    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}
    validation_callback: Callable[[pd.DataFrame], pd.DataFrame]

    def __init__(
        self,
        read_kwargs: dict[str, Any],
        write_kwargs: dict[str, Any],
        validation_callback: Callable[[pd.DataFrame], pd.DataFrame],
    ):
        self.validation_callback = validation_callback
        self.read_kwargs = read_kwargs
        self.write_kwargs = write_kwargs

    def read_from_file(self, file: str) -> pd.DataFrame:
        check_injections(file)
        df = pd.read_json(file, **self.read_kwargs)
        return self.validation_callback(df)

    def write_to_file(self, file: str, obj: pd.DataFrame):
        check_injections(file)
        obj = self.validation_callback(obj)
        return obj.to_json(file, **self.write_kwargs)


class CsvSerde(Serde[pd.DataFrame]):
    file_extension = ".csv"  # Used in cache key
    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}
    validation_callback: Callable[[pd.DataFrame], pd.DataFrame]

    def __init__(
        self,
        read_kwargs: dict[str, Any],
        write_kwargs: dict[str, Any],
        validation_callback: Callable[[pd.DataFrame], pd.DataFrame],
    ):
        self.validation_callback = validation_callback
        self.read_kwargs = read_kwargs
        self.write_kwargs = write_kwargs

    def read_from_file(self, file: str) -> pd.DataFrame:
        check_injections(file)
        df = pd.read_csv(file, **self.read_kwargs)
        return self.validation_callback(df)

    def write_to_file(self, file: str, obj: pd.DataFrame):
        check_injections(file)
        obj = self.validation_callback(obj)
        return obj.to_csv(file, **self.write_kwargs)


hdf_lock = Lock()


class HdfSerde(Serde[pd.DataFrame]):
    file_extension = ".h5"  # Used in cache key
    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {}
    validation_callback: Callable[[pd.DataFrame], pd.DataFrame]
    pickle_protocol: int

    def __init__(
        self,
        read_kwargs: dict[str, Any],
        write_kwargs: dict[str, Any],
        validation_callback: Callable[[pd.DataFrame], pd.DataFrame],
        pickle_protocol: int = 4,
    ):
        self.validation_callback = validation_callback
        self.read_kwargs = read_kwargs
        self.write_kwargs = write_kwargs
        self.pickle_protocol = pickle_protocol

    def read_from_file(self, file: str) -> pd.DataFrame:
        check_injections(file)
        with hdf_lock:
            df = pd.read_hdf(file, **self.read_kwargs)
        return self.validation_callback(df)

    def write_to_file(self, file: str, df: pd.DataFrame):
        check_injections(file)
        df = self.validation_callback(df)
        with utils.pickle_protocol(protocol=self.pickle_protocol), hdf_lock:
            return df.to_hdf(file, key="df", mode="w", **self.write_kwargs)
