"""The pandas models of a module or of a package directory."""

import importlib
import inspect
import os
import sys
from typing import Any, Dict, List

import pandera.pandas as pa

from dynamicio.schema.models import ModelCompiler


class PandasModels:
    """Finds the pandas models of a module or of a package directory."""

    @staticmethod
    def module_names(path: str, root: str) -> List[str]:
        """The dotted names of the Python modules at ``path`` (a ``.py`` file or a directory), relative to ``root``.

        Raises:
            ValueError: ``path`` is not under ``root``, so its modules cannot be imported by name.
        """
        path, root = os.path.abspath(path), os.path.abspath(root)
        if os.path.commonpath([path, root]) != root:
            raise ValueError(f"{path} is not under {root}: run dynamicio migrate from the root of the repo")
        if path.endswith(".py"):
            files = [path]
        else:
            files = sorted(os.path.join(folder, name) for folder, _, names in os.walk(path) for name in names if name.endswith(".py"))
        names = []
        for file_path in files:
            parts = os.path.splitext(os.path.relpath(file_path, root))[0].split(os.sep)
            names.append(".".join(parts[:-1] if parts[-1] == "__init__" else parts))
        return names

    @staticmethod
    def of_module(module: Any) -> List[type]:
        """The pandas ``DataFrameModel`` classes that a module defines, in the order of the source."""
        models = [
            value
            for value in vars(module).values()
            if inspect.isclass(value) and issubclass(value, pa.DataFrameModel) and value.__module__ == module.__name__ and value is not pa.DataFrameModel
        ]
        return [model for model in models if ModelCompiler.backend_of(model) == "pandas"]

    @classmethod
    def find(cls, path: str, root: str) -> Dict[str, List[type]]:
        """The pandas models of each module at ``path``, by the dotted module name. A module without a model is left out."""
        if root not in sys.path:
            sys.path.insert(0, root)
        found = {}
        for name in cls.module_names(path, root):
            models = cls.of_module(importlib.import_module(name))
            if models:
                found[name] = models
        return found
