"""Redirects the local outputs of a task into a scratch directory, so tests that share resource definitions can not overwrite each other's files."""

import os
from typing import Dict, List

from dynamicio.config import IOConfig


class OutputSandbox:
    """Context manager that points every local output of ``configs`` below ``directory`` and restores the original paths on exit.

    A resource at ``/data/out/foo.parquet`` is written to ``<directory>/data/out/foo.parquet`` (its directory is created); ``{placeholders}`` are kept. The ``expected`` files are not
    moved, and ``ExpectedOutputs`` follows the redirected path, so a test reads like one without a sandbox. This makes tests that write the same outputs
    independent: they can run in parallel (``pytest-xdist``) and leave nothing behind in the repository.

    Pass the output configs of the task under test only: an input config would be redirected too, and the task would no longer find its inputs.

    Args:
        directory: Where the outputs go, typically pytest's ``tmp_path``.
        configs: The ``IOConfig`` objects (in their LOCAL-style environment) the task writes with.
    """

    def __init__(self, directory, *configs: IOConfig):
        self.directory = str(directory)
        self.configs = configs
        self._original: Dict[int, tuple] = {}

    def redirected(self, file_path: str) -> str:
        """The path ``file_path`` is redirected to."""
        return os.path.join(self.directory, os.path.splitdrive(file_path)[1].lstrip("/\\"))

    def _make_directory(self, file_path: str) -> None:
        """Create the directory of the redirected file, down to the first one whose name has a ``{placeholder}``, as the original directories exist already."""
        parts = []
        for part in os.path.dirname(self.redirected(file_path)).split(os.sep):
            if "{" in part:
                break
            parts.append(part)
        os.makedirs(os.sep.join(parts) or os.sep, exist_ok=True)

    def _local_sections(self) -> List:
        sections = []
        for config in self.configs:
            for source_key in config.sources:
                environment = config.config.bindings[source_key].environments.get(config.env_identifier)  # a resource may not exist in every environment
                section = getattr(environment, "local", None)
                if section is not None and all(section is not other for other in sections):
                    sections.append(section)
        return sections

    def __enter__(self) -> "OutputSandbox":
        for section in self._local_sections():
            self._original[id(section)] = (section, section.file_path)
            section.file_path = self.redirected(section.file_path)
            self._make_directory(self._original[id(section)][1])
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        for section, file_path in self._original.values():
            section.file_path = file_path
        self._original.clear()
