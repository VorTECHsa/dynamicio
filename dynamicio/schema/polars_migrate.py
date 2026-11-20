"""Migrate the pandas models of a repo to polars models, and list what else the polars engine needs (``dynamicio migrate --engine polars``).

The command writes one ``dynamicio.schema.polars.DataModel`` for each pandas ``DataModel``, checks that each one compiles to the same dtypes,
validations, metrics and names, and writes a report. The report lists the checks to port by hand and the resource definitions that the polars
engine refuses or reads differently. The command does not change the resource definitions or the transformation code of the repo.
"""

import importlib
import inspect
import os
import sys
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pandera.pandas as pa
import yaml

from dynamicio.config.pydantic.table_schema import DataframeSchema
from dynamicio.engines.polars_options import PolarsOptions
from dynamicio.engines.polars_postgres import PolarsPostgres
from dynamicio.errors import EngineResourceError
from dynamicio.mixins.utils import get_string_template_field_names
from dynamicio.schema.migrate import Migration, ModelRenderer, SchemaComparison
from dynamicio.schema.models import ModelCompiler, to_dataframe_schema
from dynamicio.schema.polars_dtypes import DtypeMap


class PolarsModelRenderer(ModelRenderer):
    """Renders the ``DataframeSchema`` of a pandas model as the source of a polars ``DataModel`` with the same columns, validations and metrics."""

    MODULE_NAMES = ModelRenderer.MODULE_NAMES | {"Annotated", "pl"}
    MIGRATED = "Migrated from the pandas model {name}."

    @staticmethod
    def polars_dtype_source(pandas_dtype: str) -> str:
        """The annotation of a column of a pandas dtype: a polars dtype, with the time unit (and the time zone of a ``Datetime``, None without one).

        Raises:
            ValueError: The pandas dtype has no polars pair, for example ``datetime64[s]`` or ``period[D]``.
        """
        dtype = DtypeMap.polars_of(pandas_dtype)
        name = type(dtype).__name__
        if name in ("Datetime", "Duration"):
            arguments = [dtype.time_unit] + ([dtype.time_zone] if name == "Datetime" else [])  # Pandera needs both arguments of a Datetime
            return f"Annotated[pl.{name}, {', '.join(repr(argument) for argument in arguments)}]"
        return f"pl.{name}"

    @staticmethod
    def pandas_dtype_argument(pandas_dtype: str) -> Optional[str]:
        """The ``pandas_dtype`` of the field, or None when the reverse map of the polars dtype already gives the pandas dtype."""
        if DtypeMap.reverse_name(DtypeMap.polars_of(pandas_dtype)) == SchemaComparison.canonical_dtype(pandas_dtype):
            return None
        return pandas_dtype

    @classmethod
    def polars_class_source(cls, schema: DataframeSchema, class_name: str, config: Mapping[str, Any], docstring: str) -> str:
        """The source of one polars ``DataModel`` class for ``schema``, with the ``Config`` options of the pandas model in ``config``."""
        keywords = f"dataset={schema.name!r}" + (f", table={schema.table_name!r}" if schema.table_name else "")
        lines = [f"class {class_name}(DataModel, {keywords}):", f'    """{docstring}"""', ""]
        for name, column in schema.columns.items():
            attribute = cls.attribute_name(name, schema.columns, cls.MODULE_NAMES)
            arguments = []
            pandas_dtype = cls.pandas_dtype_argument(column.data_type)
            if pandas_dtype is not None:
                arguments.append(f"pandas_dtype={pandas_dtype!r}")
            if attribute != name:
                arguments.append(f"alias={name!r}")
            field = cls.render_field(column)
            if arguments:
                field = field[:-1] + ", " + ", ".join(arguments) + ")"
            lines.append(f"    {attribute}: {cls.polars_dtype_source(column.data_type)} = {field}")
        if config:
            lines += ["", "    class Config:"] + [f"        {option} = {value!r}" for option, value in config.items()]
        return "\n".join(lines) + "\n"

    @staticmethod
    def polars_module(classes: Sequence[str]) -> str:
        """A module of polars models, with the imports that the class bodies use."""
        body = "\n\n".join(classes)
        header = []
        if "Annotated[" in body:
            header += ["from typing import Annotated", ""]
        header += ["import polars as pl", ""]
        helpers = ["Field"] + (["Metric"] if "Metric." in body else [])
        header += [f"from dynamicio.schema import {', '.join(helpers)}", "from dynamicio.schema.polars import DataModel", "", "", ""]
        return "\n".join(header) + body


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


class PortedModel:
    """One pandas model and the parts of it that the polars model cannot take over."""

    def __init__(self, model: type):
        self.model = model
        self.source = to_dataframe_schema(model)
        self.todos: List[str] = []
        # `Series[str]` compiles to `object` too: only the Pandera dtype tells a column of `object` values, which can hold lists, from a column of strings
        self.objects = [f"{model.__name__}.{name}" for name, column in model.to_schema().columns.items() if str(column.dtype) == DtypeMap.LIST]
        self.schema = self._without_checks()
        self.config = self._config()

    @property
    def label(self) -> str:
        """The dotted name of the pandas model."""
        return f"{self.model.__module__}.{self.model.__name__}"

    def _without_checks(self) -> DataframeSchema:
        """The schema without the Pandera checks of the pandas model (``str_matches``, a custom ``@pa.check``): they are pandas code to port by hand."""
        columns: Dict[str, Dict[str, Any]] = {}
        for name, column in self.source.columns.items():
            validations = {}
            for validation in column.validations:
                if "pandera_check" in validation.options:
                    self.todos.append(f"{self.model.__name__}.{name}: port the check {validation.name!r} to polars")
                else:
                    validations[validation.name] = {"apply": validation.apply, "options": dict(validation.options)}
            columns[name] = {"type": column.data_type, "validations": validations, "metrics": [getattr(metric, "value", metric) for metric in column.metrics]}
        return DataframeSchema(name=self.source.name, table_name=self.source.table_name, columns=columns)

    def _config(self) -> Dict[str, Any]:
        """The ``Config`` options of the pandas model that a polars model has too. The dataframe checks and the index are listed to port by hand."""
        schema = self.model.to_schema()
        config: Dict[str, Any] = {}
        for option in ("strict", "coerce", "unique", "ordered"):
            value = getattr(schema, option)
            if value not in (False, None):
                config[option] = list(value) if isinstance(value, (list, tuple)) else value
        if schema.checks:
            self.todos.append(f"{self.model.__name__}: port the dataframe checks {[check.name for check in schema.checks]} to polars")
        if schema.index is not None:
            self.todos.append(f"{self.model.__name__}: a polars frame has no index. Write the index as a column (reset_index()) and declare it in the model")
        return config

    def class_source(self) -> str:
        """The source of the polars model.

        Raises:
            ValueError: A dtype has no polars pair, or a validation has no ``Field`` argument.
        """
        try:
            return PolarsModelRenderer.polars_class_source(self.schema, self.model.__name__, self.config, PolarsModelRenderer.MIGRATED.format(name=self.label))
        except ValueError as error:
            raise ValueError(f"{self.label}: {error}") from None

    def differences(self, polars_model: type) -> List[str]:
        """How the polars model differs from the pandas model, without the checks to port by hand (empty when equivalent)."""
        compiled = to_dataframe_schema(polars_model)
        found = SchemaComparison.differences(compiled, self.schema)
        return found + ([f"table: {compiled.table_name!r} != {self.schema.table_name!r}"] if compiled.table_name != self.schema.table_name else [])


class ResourceReview:
    """Lists the resource definitions that the polars engine refuses, and the CSV definitions without an ``index`` option.

    It reads the YAML files and changes none of them. It cannot see the options that the code passes to an IO object.
    """

    FILE_SOURCES = {"local": "local", "local_batch": "local", "s3": "s3", "s3_file": "s3", "s3_path_prefix": "s3"}

    @staticmethod
    def definitions(root: str) -> Dict[str, Dict[str, Any]]:
        """The resource definitions under ``root``, by the path of their YAML file. A file that is not a mapping of resources is left out."""
        found = {}
        for folder, _, names in os.walk(root):
            for name in sorted(names):
                if not name.endswith((".yaml", ".yml")):
                    continue
                file_path = os.path.join(folder, name)
                try:
                    with open(file_path, encoding="utf-8") as handle:
                        content = yaml.safe_load(handle)
                except (yaml.YAMLError, UnicodeDecodeError):
                    continue
                if isinstance(content, dict):
                    resources = {key: value for key, value in content.items() if isinstance(value, dict) and any(isinstance(env, dict) and "type" in env for env in value.values())}
                    if resources:
                        found[file_path] = resources
        return dict(sorted(found.items()))

    @classmethod
    def problem_of(cls, environment: Mapping[str, Any]) -> Tuple[Optional[str], bool]:
        """The reason that the polars engine refuses one environment of a resource, or None, and whether it is a CSV without an ``index`` option."""
        source, options = environment.get("type"), dict(environment.get("options") or {})
        if source in cls.FILE_SOURCES:
            section = environment.get(cls.FILE_SOURCES[source]) or {}
            file_type = str(section.get("file_type", "")).lower()
            paths = [str(section.get(key)) for key in ("file_path", "path_prefix", "dynamic_file_path") if section.get(key)]
            fields = {field for path in paths for field in get_string_template_field_names(path)}
            try:
                PolarsOptions.check(file_type, "s3_file" if source == "s3" else source, {name: value for name, value in options.items() if name not in fields})
            except EngineResourceError as error:
                return str(error), False
            return None, file_type == "csv" and "index" not in options
        if source == "kafka" and "key_column" not in options:
            return "the polars engine needs the option key_column: a polars frame has no index to give the message keys.", False
        refused = sorted(PolarsPostgres.PANDAS_READ_OPTIONS & set(options)) if source == "postgres" else []
        return (f"the polars engine has no Postgres option {', '.join(refused)}.", False) if refused else (None, False)

    @classmethod
    def review(cls, root: str) -> Dict[str, List[str]]:
        """``{"refused": [...], "csv_index": [...]}``: one ``file: RESOURCE.ENVIRONMENT`` line for each finding."""
        refused, csv_index = [], []
        for file_path, resources in cls.definitions(root).items():
            relative = os.path.relpath(file_path, root)
            for resource, environments in resources.items():
                for environment_name, environment in environments.items():
                    if not isinstance(environment, dict) or "type" not in environment:
                        continue
                    problem, no_index = cls.problem_of(environment)
                    if problem:
                        refused.append(f"{relative}: {resource}.{environment_name}: {problem}")
                    if no_index:
                        csv_index.append(f"{relative}: {resource}.{environment_name}")
        return {"refused": refused, "csv_index": csv_index}


class PolarsMigrationReport:
    """The Markdown report of a polars migration, for the user or for an LLM."""

    @staticmethod
    def section(title: str, intro: str, lines: Sequence[str]) -> List[str]:
        """A report section: a heading, a sentence, and a list. A section without lines says so."""
        return [f"## {title}", "", intro, ""] + ([f"- {line}" for line in lines] if lines else ["None."]) + [""]

    @classmethod
    def render(cls, output: str, models: Sequence[str], todos: Sequence[str], resources: Mapping[str, Sequence[str]], objects: Sequence[str] = ()) -> str:
        """The report of the models at ``output``, the checks to port, the ``object`` columns, and the findings of the resource definitions."""
        lines = [
            "# Polars migration",
            "",
            f"`dynamicio migrate --engine polars` wrote {len(models)} polars models to `{output}`. Each model has the dtypes, validations, metrics and names of its pandas model.",
            "",
            "1. Replace the imports of the pandas models with the imports of the polars models.",
            '2. Call `dynamicio.configure(engine="polars")` at the start of each process, before the first read or write.',
            "3. Change the transformation code from the pandas API to the polars API.",
            "4. Run the tests with `pytest --dynamicio-engine=polars`.",
            "",
        ]
        lines += cls.section("Checks to port", "These Pandera checks are pandas code. Write them again for polars.", todos)
        lines += cls.section(
            "Columns of the object dtype",
            "The polars models give these columns the `String` dtype. If a column holds lists, declare `pl.List` of the item dtype, for example `pl.List(pl.String)`.",
            objects,
        )
        lines += cls.section(
            "Resource definitions that the polars engine refuses",
            "Change these definitions, or keep these resources on the pandas engine. The options that the code gives to an IO object are not in this list.",
            resources["refused"],
        )
        lines += cls.section(
            "CSV definitions without an index option",
            "The polars engine writes the leading index column with the row numbers 0 to n-1. Set `index: false` if the file has no index column.",
            resources["csv_index"],
        )
        return "\n".join(lines)


class PolarsMigration:
    """Writes a polars model for each pandas model under a path, checks parity, and writes the report next to the models."""

    REPORT = "POLARS_MIGRATION.md"

    def __init__(self, path: str, output: str, scan: str = "", root: str = ""):
        self.path = path
        self.output = output
        self.scan = scan or os.getcwd()
        self.root = os.path.abspath(root or os.getcwd())
        self.package = not output.endswith(".py")

    def _ported(self) -> Dict[str, List[PortedModel]]:
        """The ported models of each output module: the source module name in package mode, else one module."""
        found = PandasModels.find(self.path, self.root)
        if not found:
            raise ValueError(f"No pandas models found under {self.path}")
        modules: Dict[str, List[PortedModel]] = {}
        seen: Dict[str, str] = {}
        for module, models in found.items():
            target = module.rsplit(".", 1)[-1] if self.package else ""
            for model in models:
                key = f"{target}.{model.__name__}"
                if key in seen:
                    raise ValueError(f"The pandas models {seen[key]} and {module}.{model.__name__} both map to the polars model {model.__name__} in one module")
                seen[key] = f"{module}.{model.__name__}"
                modules.setdefault(target, []).append(PortedModel(model))
        return modules

    @staticmethod
    def _verify_parity(source: str, ported: Sequence[PortedModel]) -> None:
        """Compile a generated module and compare each polars model with its pandas model."""
        namespace = ModelRenderer.compile_module(source)
        for model in ported:
            problems = model.differences(namespace[model.model.__name__])
            if problems:
                raise ValueError(f"The polars model {model.model.__name__} is not equivalent to {model.label}:\n" + "\n".join(problems))

    INIT = '"""The polars models of the datasets, one module for each module of the pandas models."""\n'

    def run(self) -> Dict[str, Any]:
        """Run the migration; see :func:`migrate_to_polars`."""
        modules = self._ported()
        sources = {name: PolarsModelRenderer.polars_module([model.class_source() for model in ported]) for name, ported in modules.items()}
        for name, ported in modules.items():
            self._verify_parity(sources[name], ported)
        Migration.write_modules(self.output, sources, self.INIT)

        models = [model.label for ported in modules.values() for model in ported]
        todos = [todo for ported in modules.values() for model in ported for todo in model.todos]
        objects = [column for ported in modules.values() for model in ported for column in model.objects]
        resources = ResourceReview.review(self.scan)
        directory = self.output if self.package else os.path.dirname(os.path.abspath(self.output))
        report_path = os.path.join(directory, self.REPORT)
        with open(report_path, "w", encoding="utf-8") as handle:
            handle.write(PolarsMigrationReport.render(self.output, models, todos, resources, objects))
        return {"models": models, "module": self.output, "report": report_path, "todos": todos, "objects": objects, **resources}


def migrate_to_polars(path: str, output: str, scan: str = "", root: str = "") -> Dict[str, Any]:
    """Write a polars model for each pandas model at ``path`` (a ``.py`` module or a package directory), check parity, and write a report.

    If ``output`` ends with ``.py``, all models go into that one module. Else ``output`` is a package directory with one module for each module of
    the pandas models. The modules at ``path`` are imported by their dotted name relative to ``root`` (default: the current directory).

    Returns:
        ``models`` (the dotted names of the pandas models), ``module``, ``report`` (the path of the report), ``todos`` (the checks to port),
        ``objects`` (the ``object`` columns, which can hold lists), ``refused`` and ``csv_index`` (the findings of the resource definitions under ``scan``).

    Raises:
        ValueError: No pandas model is found, two models map to one polars model, a dtype or a validation has no polars equivalent, or a generated
            model does not compile to its pandas model.
    """
    return PolarsMigration(path, output, scan, root).run()
