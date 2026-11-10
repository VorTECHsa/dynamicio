"""Locates, reads, removes and promotes the generated and expected files of one resource."""

import os
import shutil
from typing import Any, Mapping, Optional

import pandas as pd

from dynamicio.config import IOConfig
from dynamicio.config.pydantic.io_resources import FileType


class OutputFiles:
    """The files of ``source_key`` in the environment ``config`` selects: what the task generated, and what it is expected to generate.

    Args:
        config: The ``IOConfig`` (in its LOCAL-style environment) the task wrote the output with.
        source_key: The resource name.
        params: Values for ``{placeholders}`` in the generated and expected paths, for outputs whose file name carries a runtime parameter such as a date.
    """

    def __init__(self, config: IOConfig, source_key: str, params: Optional[Mapping[str, Any]] = None):
        self.config = config
        self.source_key = source_key
        self.params = params

    def _format(self, path: str) -> str:
        return path.format(**self.params) if self.params else path

    @property
    def environment_name(self) -> str:
        """The environment the config selects, for messages."""
        return self.config.env_identifier

    @property
    def generated_path(self) -> str:
        """The local path the task writes the resource to."""
        local = getattr(self.config.get(self.source_key), "local", None)
        file_path = getattr(local, "file_path", None)
        if file_path is None:
            raise ValueError(f"{self.source_key} is not a single-file local resource in environment {self.environment_name!r}; only those can be checked")
        return self._format(file_path)

    @property
    def expected_path(self) -> str:
        """The path of the reference file declared under ``expected``."""
        expected = self.config.get(self.source_key).expected
        if expected is None:
            raise ValueError(f"{self.source_key} declares no `expected` path in environment {self.environment_name!r}")
        return self._format(expected.path)

    def _file_type(self, path: str, use_expected: bool) -> str:
        environment = self.config.get(self.source_key)
        declared = environment.expected.file_type if use_expected and environment.expected else None
        declared = declared or getattr(getattr(environment, "local", None), "file_type", None)
        if declared is not None:
            return FileType(declared).value
        extension = os.path.splitext(path)[1].lstrip(".").lower()
        return {"h5": "hdf"}.get(extension, extension)

    @staticmethod
    def read_file(path: str, file_type: str) -> pd.DataFrame:
        """Read a file with plain pandas, so no schema or cast hides a difference."""
        readers = {"parquet": pd.read_parquet, "csv": pd.read_csv, "json": pd.read_json, "hdf": pd.read_hdf}
        if file_type not in readers:
            raise ValueError(f"Unsupported file type {file_type!r} for {path}")
        return readers[file_type](path)

    def read_generated(self) -> pd.DataFrame:
        """The output the task generated."""
        return self.read_file(self.generated_path, self._file_type(self.generated_path, use_expected=False))

    def read_expected(self) -> pd.DataFrame:
        """The reference output."""
        return self.read_file(self.expected_path, self._file_type(self.expected_path, use_expected=True))

    def generated_exists(self) -> bool:
        """Whether the task wrote its output."""
        return os.path.exists(self.generated_path)

    def expected_exists(self) -> bool:
        """Whether the reference file exists."""
        return os.path.exists(self.expected_path)

    def remove_generated(self) -> None:
        """Delete the generated file if it is there."""
        if self.generated_exists():
            os.remove(self.generated_path)

    def promote_generated(self) -> None:
        """Make the generated file the new reference (creating the expected directory if needed)."""
        os.makedirs(os.path.dirname(os.path.abspath(self.expected_path)), exist_ok=True)
        shutil.copyfile(self.generated_path, self.expected_path)
