"""Migrate the YAML schemas of dynamicio 8 and earlier (and everything since) to Pandera ``DataFrameModel`` source for dynamicio 11 (``dynamicio migrate``).

This is the one place that still reads the YAML schema format, which dynamicio 11 itself refuses. Users already on dynamicio 9 or 10 hold models and
need only the notes in the changelog, which a coding assistant can apply.
"""

import keyword
import os
import re
from typing import Any, Collection, Dict, List, Mapping, Sequence, Set, Tuple

import pandas as pd
import yaml

from dynamicio.config.pydantic.table_schema import DataframeSchema, SchemaColumn
from dynamicio.schema.metadata import Metric
from dynamicio.schema.models import to_dataframe_schema


class SchemaComparison:
    """Decides whether two ``DataframeSchema`` declare the same dtypes, applied validations (defaults normalised) and metrics, per column."""

    @staticmethod
    def canonical_dtype(name: str) -> str:
        """The pandas name of a dtype alias (`double` is `float64`, `int` is `int64`); unknown names are returned unchanged."""
        try:
            return str(pd.api.types.pandas_dtype(name))
        except TypeError:
            return name

    @staticmethod
    def value_order(value: Any) -> tuple:
        """Sort key that orders mixed-type values (including ``None``) deterministically."""
        return (value is None, str(value))

    @classmethod
    def _options(cls, validation) -> list:
        defaults = {"is_between": {"include_left": False, "include_right": False}, "is_in": {"match_all": True}}.get(validation.name, {})
        options = {**defaults, **validation.options}
        if "categorical_values" in options:  # a set semantically: order is irrelevant
            options["categorical_values"] = sorted(options["categorical_values"], key=cls.value_order)
        return sorted(options.items(), key=str)

    @classmethod
    def _comparable(cls, schema: DataframeSchema) -> dict:
        return {
            name: (cls.canonical_dtype(col.data_type), sorted((v.name, cls._options(v)) for v in col.validations if v.apply), [getattr(m, "value", m) for m in col.metrics])
            for name, col in schema.columns.items()
        }

    @classmethod
    def differences(cls, left: DataframeSchema, right: DataframeSchema) -> List[str]:
        """Describe, per column, how two schemas differ (empty when equivalent)."""
        a, b = cls._comparable(left), cls._comparable(right)
        found = [f"{name}: {a.get(name)} != {b.get(name)}" for name in sorted(set(a) | set(b)) if a.get(name) != b.get(name)]
        return found + ([f"name: {left.name!r} != {right.name!r}"] if left.name != right.name else [])

    @classmethod
    def equivalent(cls, left: DataframeSchema, right: DataframeSchema) -> bool:
        """True if both schemas declare the same dtypes, applied validations (defaults normalised) and metrics, per column."""
        return not cls.differences(left, right)


class ModelRenderer:
    """Renders ``DataframeSchema`` objects as the Python source of equivalent Pandera ``DataFrameModel`` classes."""

    # YAML `object` meant "anything" (strings, lists, mixed values) and dynamicio never looked inside, so it stays `object`: faithful and byte-compatible.
    DTYPES = {"int64": "int", "float64": "float", "object": "object", "bool": "bool", "datetime64[ns]": "pd.Timestamp"}
    SIMPLE_CHECKS = {"is_greater_than": "gt", "is_greater_than_or_equal": "ge", "is_lower_than": "lt", "is_lower_than_or_equal": "le"}

    # YAML schemas may reference dynamic variables ("[[ NAME ]]"); in Python that is simply a name the module has to provide.
    PLACEHOLDER = re.compile(r"""(['"])\[\[\s*(\w+)\s*\]\]\1""")

    @staticmethod
    def class_name(name: str) -> str:
        """A class name for a schema or file name."""
        return "".join(part.capitalize() for part in re.split(r"[^0-9a-zA-Z]+", name) if part) or "Schema"

    @classmethod
    def _use_names(cls, source: str) -> str:
        return cls.PLACEHOLDER.sub(lambda match: match.group(2), source)

    @classmethod
    def render_field(cls, column: SchemaColumn) -> str:
        """The ``pa.Field(...)`` call that declares ``column``."""
        kwargs: List[str] = []
        helpers: List[str] = []
        applied = [v for v in column.validations if v.apply]
        by_name = {v.name: dict(v.options) for v in applied}

        # Pandera's default is nullable=False; dynamicio's is "nulls allowed unless has_no_null_values". Always be explicit.
        kwargs.append(f"nullable={'has_no_null_values' not in by_name}")
        if "has_unique_values" in by_name:
            kwargs.append("unique=True")
        for name, options in by_name.items():
            if name in cls.SIMPLE_CHECKS:
                kwargs.append(f"{cls.SIMPLE_CHECKS[name]}={options['threshold']!r}")
            # A string bound is a `[[ VARIABLE ]]` placeholder; Pandera's `in_range` compares the bounds on definition, so those use `strictly_between`.
            elif name == "is_between" and not isinstance(options["lower"], str) and not isinstance(options["upper"], str):
                kwargs.append(
                    f"in_range={{'min_value': {options['lower']!r}, 'max_value': {options['upper']!r}, "
                    f"'include_min': {options.get('include_left', False)!r}, 'include_max': {options.get('include_right', False)!r}}}"
                )
            elif name == "is_between":
                helpers.append(
                    f"strictly_between({options['lower']!r}, {options['upper']!r}, "
                    f"include_lower={options.get('include_left', False)!r}, include_upper={options.get('include_right', False)!r})"
                )
            elif name == "is_in" and options.get("match_all", True):
                kwargs.append(f"isin={sorted(options['categorical_values'], key=SchemaComparison.value_order)!r}")
            elif name == "is_in":
                helpers.append(f"exactly_values({sorted(options['categorical_values'], key=SchemaComparison.value_order)!r})")
            elif name == "has_acceptable_percentage_of_nulls":
                helpers.append(f"null_fraction_below({options['threshold']!r})")
            elif name not in ("has_no_null_values", "has_unique_values"):
                raise ValueError(f"Column {column.name!r}: no model equivalent for the validation {name!r}")
        if column.metrics:
            members = ", ".join(f"Metric.{Metric(getattr(m, 'value', m)).name}" for m in column.metrics)
            helpers.insert(0, f"metrics({members})")
        if helpers:
            kwargs.append(f"metadata={' | '.join(helpers)}")
        return f"pa.Field({', '.join(kwargs)})"

    @classmethod
    def _class_source(cls, schema: DataframeSchema, class_name: str = "", table: bool = False) -> str:
        lines = [f"class {class_name or cls.class_name(schema.name)}(pa.DataFrameModel):", f'    """Migrated from the dynamicio schema {schema.name!r}."""', ""]
        lines.append(f"    __dataset_name__ = {schema.name!r}")
        if table:  # the old schema name was the table name of the Postgres resources that used it
            lines.append(f"    __table_name__ = {schema.name!r}  # the exact name of the table in the database")
        lines.append("")
        for name, column in schema.columns.items():
            attribute = name if name.isidentifier() and not keyword.iskeyword(name) else f"column_{re.sub(r'[^0-9a-zA-Z_]', '_', name)}"
            field = cls.render_field(column)
            if attribute != name:
                field = field[:-1] + f", alias={name!r})"
            canonical = SchemaComparison.canonical_dtype(column.data_type)
            dtype = cls.DTYPES.get(canonical, f"pd.api.types.pandas_dtype({canonical!r})")
            lines.append(f"    {attribute}: Series[{dtype}] = {field}")
        lines += ["", "    class Config:", '        strict = "filter"  # dynamicio dropped the columns the schema does not list', ""]
        return "\n".join(lines)

    @classmethod
    def render_model(cls, schema: DataframeSchema, class_name: str = "", table: bool = False) -> str:
        """Return Python source for a ``pa.DataFrameModel`` equivalent to ``schema``; the class is named after the schema unless ``class_name`` is given.

        With ``table`` the model also names its Postgres table, which the YAML schemas did through their name.
        """
        return cls._use_names(cls._class_source(schema, class_name, table))

    @classmethod
    def render_module(cls, schemas: Mapping[str, DataframeSchema], tables: Collection[str] = ()) -> str:
        """Return a module defining one model per schema; the models named in ``tables`` also name their Postgres table."""
        body = "\n\n".join(cls._class_source(s, name, name in tables) for name, s in schemas.items())
        header = [
            "import pandera.pandas as pa",
            "from pandera.typing import Series",
            "",
            "# `object` is what the YAML schemas declared: any Python values, including lists. Where you know a column only holds strings, declare it",
            "# `Series[str]` (or `Series[pd.StringDtype]`, `string[pyarrow]` for a smaller column; the last two give `pd.NA` instead of `NaN` for missing values).",
            "",
            "",
            "",
        ]
        if "pd." in body:
            header.insert(0, "import pandas as pd")
        helpers = [name for name in ("Metric", "exactly_values", "metrics", "null_fraction_below", "strictly_between") if re.search(rf"\b{name}\b", body)]
        if helpers:
            position = header.index("from pandera.typing import Series") + 1
            header[position:position] = ["", f"from dynamicio.schema import {', '.join(helpers)}"]
        names = sorted({match.group(2) for match in cls.PLACEHOLDER.finditer(body)})
        for name in names:
            header.insert(-1, f'{name} = "[[ {name} ]]"  # TODO: dynamic variable in the old YAML schema; import its real value')
        if names:
            header[-1:-1] = ["", ""]
        return "\n".join(header) + cls._use_names(body)

    @staticmethod
    def compile_module(source: str) -> Dict[str, Any]:
        """Execute generated module source and return its namespace."""
        namespace: Dict[str, Any] = {}
        exec(compile(source, "<migrated schemas>", "exec"), namespace)  # pylint: disable=exec-used
        return namespace


class LegacyDeclarations:
    """Finds what still declares schemas the old way: YAML schema files and the code and resource definitions that point at them."""

    CALL_SITE_PATTERNS = (
        (".py", re.compile(r"SCHEMA_FROM_FILE|SafeDynamicSchemaLoader|dynamicio_schema|^\s*schema(_validations|_metrics)?\s*=|metadata=\{\s*[\"']dynamicio[\"']|^\s*class Config\b")),
        (".yaml", re.compile(r"^\s*schema:\s*$|^\s*file_path:.*schema")),
        (".yml", re.compile(r"^\s*schema:\s*$|^\s*file_path:.*schema")),
    )

    @staticmethod
    def find_yaml_schemas(path: str) -> Dict[str, DataframeSchema]:
        """Load every YAML file under ``path`` that looks like a dynamicio schema (top-level ``name`` and ``columns``)."""
        found: Dict[str, DataframeSchema] = {}
        for root, _, files in sorted(os.walk(path)):
            for file_name in sorted(files):
                if not file_name.endswith((".yaml", ".yml")):
                    continue
                file_path = os.path.join(root, file_name)
                with open(file_path, encoding="utf-8") as handle:
                    try:
                        content = yaml.safe_load(handle)
                    except yaml.YAMLError:
                        continue
                if isinstance(content, dict) and {"name", "columns"} <= set(content):
                    try:
                        found[file_path] = DataframeSchema(**content)
                    except ValueError:
                        continue
        return found

    @staticmethod
    def find_postgres_schema_files(root: str) -> Set[str]:
        """File names of the YAML schemas that a resource definition under ``root`` binds to a Postgres environment (they need a ``__table_name__``)."""
        found: Set[str] = set()
        for current, dirs, files in sorted(os.walk(root)):
            dirs[:] = sorted(d for d in dirs if not d.startswith(".") and d not in ("node_modules", "__pycache__", "venv"))
            for file_name in sorted(files):
                if not file_name.endswith((".yaml", ".yml")):
                    continue
                with open(os.path.join(current, file_name), encoding="utf-8", errors="replace") as handle:
                    try:
                        content = yaml.safe_load(handle)
                    except yaml.YAMLError:
                        continue
                if not isinstance(content, dict):
                    continue
                for binding in content.values():
                    if not isinstance(binding, dict) or not isinstance(binding.get("schema"), dict) or not binding["schema"].get("file_path"):
                        continue
                    if any(isinstance(env, dict) and env.get("type") == "postgres" for key, env in binding.items() if key != "schema"):
                        found.add(os.path.basename(str(binding["schema"]["file_path"])))
        return found

    @classmethod
    def find_call_sites(cls, root: str, exclude: Sequence[str] = ()) -> List[Tuple[str, int, str]]:
        """Locate code and resource-definition lines that still declare schemas the old way: (file, line number, text)."""
        sites: List[Tuple[str, int, str]] = []
        for current, dirs, files in sorted(os.walk(root)):
            dirs[:] = sorted(d for d in dirs if not d.startswith(".") and d not in ("node_modules", "__pycache__", "venv"))
            for file_name in sorted(files):
                file_path = os.path.join(current, file_name)
                if file_path in exclude:
                    continue
                for extension, pattern in cls.CALL_SITE_PATTERNS:
                    if file_name.endswith(extension):
                        with open(file_path, encoding="utf-8", errors="replace") as handle:
                            sites.extend((file_path, number, line.rstrip()) for number, line in enumerate(handle, 1) if pattern.search(line))
        return sites


class MigrationPrompt:
    """The Markdown prompt that hands the remaining call-site edits to an LLM."""

    TEMPLATE = """\
# dynamicio 11 migration: update the call sites

`dynamicio migrate` converted the YAML schemas into Pandera models in `{module}`. dynamicio 11 declares every schema as a
Pandera `DataFrameModel` and refuses dict schemas, `SCHEMA_FROM_FILE` and `schema:` blocks in resource definitions. Finish the
migration by editing the call sites below. Do not change validation or metric behaviour: dataset names, metric log lines and
validation messages must stay identical.

## Generated models

{models}

## What to change

1. IO classes using `schema = SCHEMA_FROM_FILE` : either set `schema = <Model>` when the class serves one dataset, or use
   `schema = SCHEMA_FROM_ARGUMENT` (from `dynamicio.core`) and pass `schema=<Model>` when constructing it. Remove the
   `SCHEMA_FROM_FILE` import.
2. Remove the `schema:` / `file_path:` blocks from the resource definition YAML files (resources stay in YAML).
3. Names marked `# TODO` at the top of the models module stood for dynamic variables (`[[ NAME ]]`) in the old YAML. Replace
   each placeholder with an import of the real value, e.g. `from my_project.environment import LOWER_THAN_LIMIT`.
4. Dict-style schemas (`schema = {{...}}`, `schema_validations`, `schema_metrics`) were not converted. Translate them into
   `pa.DataFrameModel` classes following the generated ones: always pass `nullable=` explicitly (Pandera defaults to
   `False`, dynamicio allows nulls unless `has_no_null_values` is declared). Declare metrics and the validations that have no
   `pa.Field` argument with the helpers of `dynamicio.schema` (`metrics(Metric.MIN)`, `null_fraction_below(0.2)`,
   `exactly_values([...])`, `strictly_between(lower, upper)`, combined with `|`), never with a `metadata` dict. Name the dataset
   with `__dataset_name__ = "<the old schema name>"` in the model, and set `__table_name__ = "<exact table name>"` on every
   model used with a Postgres resource.
5. A model whose `Config` only sets `name` should use `__dataset_name__` instead, and `Config` can go.
6. Run the tests; metric and validation tests must pass unchanged.

## Call sites to review

{sites}
"""

    @classmethod
    def render(cls, module: str, models: Mapping[str, str], sites: Sequence[Tuple[str, int, str]]) -> str:
        """Return the prompt for ``module``, listing the generated ``models`` and the call ``sites`` still to edit."""
        model_lines = "\n".join(f"- `{name}` (from `{source}`)" for name, source in models.items()) or "- (none)"
        site_lines = "\n".join(f"- `{path}:{number}`: `{text.strip()}`" for path, number, text in sites) or "- (none found)"
        return cls.TEMPLATE.format(module=module, models=model_lines, sites=site_lines)


class Migration:
    """Converts every YAML schema under a path into dynamicio 11 Pandera models, verifies parity and writes the prompt for the remaining call-site edits."""

    def __init__(self, path: str, output: str, scan: str = ""):
        self.path = path
        self.output = output
        self.scan = scan
        self.package = not output.endswith(".py")
        self.postgres_classes: Set[str] = set()
        self.by_class: Dict[str, Tuple[str, DataframeSchema]] = {}
        self.modules: Dict[str, Dict[str, DataframeSchema]] = {}

    def _module_name(self, file_path: str) -> str:
        """Name of the module a schema file goes to in package mode: its directory below ``path`` (``common`` for files directly in ``path``)."""
        relative = os.path.relpath(os.path.dirname(file_path), self.path)
        return "common" if relative == "." else re.sub(r"\W+", "_", relative).strip("_").lower()

    def _group(self, schemas: Mapping[str, DataframeSchema]) -> None:
        postgres_files = LegacyDeclarations.find_postgres_schema_files(self.scan or os.getcwd())
        for file_path, schema in schemas.items():
            # Named after the file: schema names need not be unique (and are what metric lines report), file names are.
            name = ModelRenderer.class_name(os.path.splitext(os.path.basename(file_path))[0])
            if name in self.by_class:
                name = ModelRenderer.class_name(os.path.basename(os.path.dirname(file_path))) + name
            if name in self.by_class:
                raise ValueError(f"Schemas {self.by_class[name][0]} and {file_path} both map to class {name}")
            self.by_class[name] = (file_path, schema)
            if os.path.basename(file_path) in postgres_files:
                self.postgres_classes.add(name)
            self.modules.setdefault(self._module_name(file_path) if self.package else "", {})[name] = schema

    def _verify_parity(self, sources: Dict[str, str]) -> None:
        """Compile every generated module and check each model against the YAML schema it came from."""
        for module, source in sources.items():
            namespace = ModelRenderer.compile_module(source)
            for name in self.modules[module]:
                file_path, schema = self.by_class[name]
                problems = SchemaComparison.differences(to_dataframe_schema(namespace[name]), schema)
                if problems:
                    raise ValueError(f"Generated model {name} is not equivalent to {file_path}:\n" + "\n".join(problems))

    @staticmethod
    def _todo_names(sources: Dict[str, str]) -> List[str]:
        """Names of the generated fields that carry a ``# TODO`` for the user to review."""
        return [line.split(" =")[0] for source in sources.values() for line in source.splitlines() if "# TODO" in line]

    def _write(self, sources: Dict[str, str]) -> Dict[str, str]:
        """Write the generated modules (a package directory, or the single output file) and return ``{path: source}`` of what was written."""
        if self.package:
            os.makedirs(self.output, exist_ok=True)
            written = {os.path.join(self.output, f"{name}.py"): source for name, source in sources.items()}
            init_path = os.path.join(self.output, "__init__.py")
            if not os.path.exists(init_path):
                written[init_path] = '"""Pandera models of the datasets, one module per group of the former YAML schemas."""\n'
        else:
            written = {self.output: sources[""]}
        for file_path, source in written.items():
            with open(file_path, "w", encoding="utf-8") as handle:
                handle.write(source)
        return written

    def run(self) -> Dict[str, Any]:
        """Run the migration; see :func:`migrate`."""
        schemas = LegacyDeclarations.find_yaml_schemas(self.path)
        if not schemas:
            raise ValueError(f"No dynamicio YAML schemas found under {self.path}")
        self._group(schemas)

        sources = {name: ModelRenderer.render_module(group, self.postgres_classes) for name, group in self.modules.items()}
        self._verify_parity(sources)

        written = self._write(sources)
        directory = self.output if self.package else os.path.dirname(os.path.abspath(self.output))
        prompt_path = os.path.join(directory, "MIGRATION_PROMPT.md")
        sites = LegacyDeclarations.find_call_sites(self.scan or os.getcwd(), exclude=[os.path.abspath(p) for p in written])
        with open(prompt_path, "w", encoding="utf-8") as handle:
            handle.write(MigrationPrompt.render(self.output, {name: file_path for name, (file_path, _) in self.by_class.items()}, sites))
        return {"models": list(self.by_class), "module": self.output, "prompt": prompt_path, "call_sites": len(sites), "todos": self._todo_names(sources)}


def migrate(path: str, output: str, scan: str = "") -> Dict[str, Any]:
    """Convert every YAML schema under ``path`` into Pandera models at ``output``, verify parity and write a prompt next to it.

    If ``output`` ends with ``.py`` all models go into that one module. Otherwise ``output`` is a package directory and the models are grouped
    into one module per sub-directory of ``path``, mirroring how the YAML schemas were organised (for example ``schemas/stage/*.yaml`` becomes
    ``<output>/stage.py``).

    Raises:
        ValueError: If no schemas are found, two schemas map to the same class name, or a generated model does not compile back to its source schema.
    """
    return Migration(path, output, scan).run()


render_model = ModelRenderer.render_model
render_module = ModelRenderer.render_module
compile_module = ModelRenderer.compile_module
differences = SchemaComparison.differences
schemas_equivalent = SchemaComparison.equivalent
find_yaml_schemas = LegacyDeclarations.find_yaml_schemas
find_call_sites = LegacyDeclarations.find_call_sites
render_prompt = MigrationPrompt.render
