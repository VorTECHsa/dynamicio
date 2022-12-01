"""This module provides passthrough to the generic pandas handlers"""

import typing

import pandas as pd

from . import base


class CSV(base.Base):
    def read_local_file(self, file_path: typing.AnyStr, options: dict) -> pd.DataFrame:
        return pd.read_csv(file_path, **options)

    def write_local_file(self, df: pd.DataFrame, file_path: typing.AnyStr, options: dict):
        df.to_csv(file_path, **options)


class JSON(base.Base):
    def read_local_file(self, file_path: typing.AnyStr, options: dict) -> pd.DataFrame:
        return pd.read_json(file_path, **options)

    def write_local_file(self, df: pd.DataFrame, file_path: typing.AnyStr, options: dict):
        df.to_json(file_path, **options)


class Parquet(base.Base):
    def read_local_file(self, file_path: typing.AnyStr, options: dict) -> pd.DataFrame:
        return pd.read_parquet(file_path, **options)

    def write_local_file(self, df: pd.DataFrame, file_path: typing.AnyStr, options: dict):
        return df.to_parquet(file_path, **options)
