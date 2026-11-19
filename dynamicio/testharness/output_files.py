"""Locates, reads, removes and promotes the generated and expected files of one resource."""

import os
import shutil
import string
from typing import Any, List, Mapping, Optional

import pandas as pd

from dynamicio.config import IOConfig
from dynamicio.config.pydantic.io_resources import FileType


class OutputFiles:
    """The files of ``source_key`` in the environment ``config`` selects: what the task generated, and what it is expected to generate.

    Args:
        config: The ``IOConfig`` (in its LOCAL-style environment) the task wrote the output with.
        source_key: The resource name.
        params: Values for ``{placeholders}`` in the generated and expected paths, for outputs whose file name carries a runtime parameter such as a date.
        environment: The environment to look the resource up in; by default the first one the resource definitions list.
    """

    JSON_READ_OPTIONS = ("orient", "lines")

    def __init__(self, config: IOConfig, source_key: str, params: Optional[Mapping[str, Any]] = None, environment: Optional[str] = None):
        self.config = config
        self.source_key = source_key
        self.params = params
        self.selected_environment = environment

    @property
    def resource(self):
        """The definition of the resource in the environment under test."""
        binding = self.config.config.bindings[self.source_key]
        try:
            return binding.get_binding_for_environment(self.environment_name)
        except KeyError:
            raise ValueError(f"{self.source_key} has no environment {self.environment_name!r}; it defines {sorted(binding.environments)}") from None

    @staticmethod
    def declaring_expected(config: IOConfig, environment: Optional[str] = None) -> List[str]:
        """The resources of ``config`` that declare an ``expected`` path in the environment: the outputs a test can check."""
        keys = []
        for source_key in config.sources:
            try:
                declared = OutputFiles(config, source_key, environment=environment).resource.expected
            except ValueError:  # the resource has no such environment
                continue
            if declared is not None:
                keys.append(source_key)
        return keys

    @property
    def placeholders(self) -> List[str]:
        """The ``{placeholders}`` of the paths that ``params`` does not fill."""
        paths = [self.resource.expected.path] if self.resource.expected else []
        local = getattr(self.resource, "local", None)
        paths += [getattr(local, "file_path", None) or ""]
        names = {name for path in paths for _, name, _, _ in string.Formatter().parse(path) if name}
        return sorted(names - set(self.params or {}))

    def _format(self, path: str) -> str:
        return path.format(**self.params) if self.params else path

    @property
    def environment_name(self) -> str:
        """The environment looked up: the selected one, else the first one the resource definitions list."""
        return self.selected_environment or self.config.default_environment

    @property
    def generated_path(self) -> str:
        """The local path the task writes the resource to."""
        local = getattr(self.resource, "local", None)
        file_path = getattr(local, "file_path", None)
        if file_path is None:
            raise ValueError(f"{self.source_key} is not a single-file local resource in environment {self.environment_name!r}; only those can be checked")
        return self._format(file_path)

    @property
    def expected_path(self) -> str:
        """The path of the reference file declared under ``expected``."""
        expected = self.resource.expected
        if expected is None:
            raise ValueError(f"{self.source_key} declares no `expected` path in environment {self.environment_name!r}")
        return self._format(expected.path)

    def _file_type(self, path: str, use_expected: bool) -> str:
        environment = self.resource
        declared = environment.expected.file_type if use_expected and environment.expected else None
        declared = declared or getattr(getattr(environment, "local", None), "file_type", None)
        if declared is not None:
            return FileType(declared).value
        extension = os.path.splitext(path)[1].lstrip(".").lower()
        return {"h5": "hdf"}.get(extension, extension)

    @property
    def is_parquet(self) -> bool:
        """Whether the resource is written as parquet."""
        return self._file_type(self.generated_path, use_expected=False) == "parquet"

    @property
    def read_options(self) -> Mapping[str, Any]:
        """Reader options for both files: the JSON ``orient`` and ``lines`` the resource is written with, overridden by ``expected.options``."""
        environment = self.resource
        options = {name: environment.options[name] for name in self.JSON_READ_OPTIONS if name in environment.options}
        if environment.expected:
            options.update(environment.expected.options)
        return options

    @classmethod
    def read_file(cls, path: str, file_type: str, **options: Any) -> Any:
        """Read a file with the plain readers of the engine of the process, so no schema or cast hides a difference.

        ``options`` go to the reader (``orient`` and ``lines`` only to JSON). On the polars engine they are the arguments of the polars reader.

        Raises:
            ValueError: The engine has no reader of the file type, or of the JSON orient.
        """
        from dynamicio.engines import EngineSetting  # pylint: disable=import-outside-toplevel

        if EngineSetting.current().name == "polars":
            return cls.read_polars_file(path, file_type, **options)
        readers = {"parquet": pd.read_parquet, "csv": pd.read_csv, "json": pd.read_json, "hdf": pd.read_hdf}
        if file_type not in readers:
            raise ValueError(f"Unsupported file type {file_type!r} for {path}")
        if file_type != "json":
            for name in OutputFiles.JSON_READ_OPTIONS:
                options.pop(name, None)
        return readers[file_type](path, **options)

    @staticmethod
    def read_polars_file(path: str, file_type: str, **options: Any) -> Any:
        """Read a file with the plain polars readers. A JSON file has the ``records`` orient, with ``lines`` for one record on each line.

        Raises:
            ValueError: The file type has no polars reader (``hdf``), the JSON orient is not ``records``, or polars cannot read the file. The error of
                polars is the cause of the ``ValueError``.
        """
        import polars as pl  # pylint: disable=import-outside-toplevel

        orient, lines = options.pop("orient", "records"), options.pop("lines", False)
        if file_type == "json":
            if orient != "records":
                raise ValueError(f"polars reads the JSON records orient only, and {path} has the orient {orient!r}")
            reader = pl.read_ndjson if lines else pl.read_json
        else:
            reader = {"parquet": pl.read_parquet, "csv": pl.read_csv}.get(file_type)
            if reader is None:
                raise ValueError(f"Unsupported file type {file_type!r} for {path} on the polars engine")
        try:
            return reader(path, **options)
        except pl.exceptions.PolarsError as error:  # ExpectedOutputs.check() records a ValueError and checks the other outputs
            raise ValueError(f"polars cannot read {path}: {error}") from error

    def read_generated(self) -> Any:
        """The output the task generated."""
        return self.read_file(self.generated_path, self._file_type(self.generated_path, use_expected=False), **self.read_options)

    def read_expected(self) -> Any:
        """The reference output."""
        return self.read_file(self.expected_path, self._file_type(self.expected_path, use_expected=True), **self.read_options)

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
        """Make the generated file the new reference (creating the expected directory if needed).

        Raises:
            ValueError: If the reference is declared in another file type than the output, as copying would leave a reference the checks cannot read.
        """
        generated_type, expected_type = self._file_type(self.generated_path, use_expected=False), self._file_type(self.expected_path, use_expected=True)
        if generated_type != expected_type:
            raise ValueError(f"cannot copy a {generated_type} output over a {expected_type} reference; declare the reference as {generated_type} or regenerate it in {expected_type}")
        os.makedirs(os.path.dirname(os.path.abspath(self.expected_path)), exist_ok=True)
        shutil.copyfile(self.generated_path, self.expected_path)
