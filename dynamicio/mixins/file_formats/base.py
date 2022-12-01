import typing
import io
import shutil

import pandas as pd

import tempfile


class Base:
    """Base class for file format parsers."""

    def read_local_file(self, file_path: typing.AnyStr, options: dict) -> pd.DataFrame:
        """Read the data from a file on the local drive"""
        raise NotImplementedError(f"{self.__class__.__name__}.read_local_file({file_path!r})")

    def read_fobj(self, fobj: typing.BinaryIO, options: dict):
        """Read the data from a generic readable fileobject"""
        with tempfile.NamedTemporaryFile() as temp_file:
            shutil.copyfileobj(fobj, temp_file, options)
            temp_file.flush()
            temp_file.seek(0, 0)
            return self.read_local_file()

    def write_local_file(self, df: pd.DataFrame, file_path: typing.AnyStr, options: dict):
        """Write the data to the local file"""
        raise NotImplementedError(f"{self.__class__.__name__}.write_local_file({file_path!r})")

    def write_fobj(self, df: pd.DataFrame, fobj: typing.BinaryIO, options: dict):
        """Save the data to a writeable fileobject"""
        with tempfile.NamedTemporaryFile() as temp_file:
            self.write_local_file(df, temp_file.name, options)
            temp_file.flush()
            temp_file.seek(0, 0)
            shutil.copyfileobj(temp_file, fobj)
