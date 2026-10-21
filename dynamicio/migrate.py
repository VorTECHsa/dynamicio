"""Migrate YAML-defined ``DataframeSchema`` files to equivalent Pandera ``DataFrameModel`` source (``dynamicio migrate``)."""

import keyword
import os
import re
from typing import Any, Dict, List, Mapping, Sequence, Tuple

import yaml

from dynamicio.config.pydantic.table_schema import DataframeSchema, SchemaColumn
from dynamicio.pandera_schema import to_dataframe_schema

_DTYPES = {"int64": "int", "float64": "float", "object": "str", "bool": "bool", "datetime64[ns]": "pd.Timestamp"}


def _class_name(name: str) -> str:
    return "".join(part.capitalize() for part in re.split(r"[^0-9a-zA-Z]+", name) if part) or "Schema"


def _render_column(column: SchemaColumn) -> str:
    kwargs: List[str] = []
    extra: Dict[str, Any] = {}
    applied = [v for v in column.validations if v.apply]
    by_name = {v.name: dict(v.options) for v in applied}

    # Pandera's default is nullable=False; dynamicio's is "nulls allowed unless has_no_null_values". Always be explicit.
    kwargs.append(f"nullable={'has_no_null_values' not in by_name}")
    if "has_unique_values" in by_name:
        kwargs.append("unique=True")
    simple = {"is_greater_than": "gt", "is_greater_than_or_equal": "ge", "is_lower_than": "lt", "is_lower_than_or_equal": "le"}
    for name, options in by_name.items():
        if name in simple:
            kwargs.append(f"{simple[name]}={options['threshold']!r}")
        elif name == "is_between":
            kwargs.append(
                f"in_range={{'min_value': {options['lower']!r}, 'max_value': {options['upper']!r}, "
                f"'include_min': {options.get('include_left', False)!r}, 'include_max': {options.get('include_right', False)!r}}}"
            )
        elif name == "is_in" and options.get("match_all", True):
            kwargs.append(f"isin={sorted(options['categorical_values'])!r}")
        elif name not in ("has_no_null_values", "has_unique_values"):
            extra.setdefault("validations", {})[name] = options
    if column.metrics:
        extra["metrics"] = [m.value if hasattr(m, "value") else m for m in column.metrics]
    if extra:
        kwargs.append(f"metadata={{'dynamicio': {extra!r}}}")
    return f"pa.Field({', '.join(kwargs)})"


# YAML schemas may reference dynamic variables ("[[ NAME ]]"); in Python that is simply a name the module has to provide.
_PLACEHOLDER = re.compile(r"""(['"])\[\[\s*(\w+)\s*\]\]\1""")


def _use_names(source: str) -> str:
    return _PLACEHOLDER.sub(lambda match: match.group(2), source)


def render_model(schema: DataframeSchema, class_name: str = "") -> str:
    """Return Python source for a ``pa.DataFrameModel`` equivalent to ``schema``; the class is named after the schema unless ``class_name`` is given."""
    return _use_names(_render_model(schema, class_name))


def _render_model(schema: DataframeSchema, class_name: str = "") -> str:
    lines = [f"class {class_name or _class_name(schema.name)}(pa.DataFrameModel):", f'    """Migrated from the dynamicio schema {schema.name!r}."""', ""]
    for name, column in schema.columns.items():
        attribute = name if name.isidentifier() and not keyword.iskeyword(name) else f"column_{re.sub(r'[^0-9a-zA-Z_]', '_', name)}"
        field = _render_column(column)
        if attribute != name:
            field = field[:-1] + f", alias={name!r})"
        dtype = _DTYPES.get(column.data_type, f"pd.api.types.pandas_dtype({column.data_type!r})")
        lines.append(f"    {attribute}: Series[{dtype}] = {field}")
    lines += ["", "    class Config:", f"        name = {schema.name!r}", ""]
    return "\n".join(lines)


def render_module(schemas: Mapping[str, DataframeSchema]) -> str:
    """Return a module defining one model per schema."""
    body = "\n\n".join(_render_model(s, cls) for cls, s in schemas.items())
    header = ["import pandera.pandas as pa", "from pandera.typing import Series", "", ""]
    if "pd." in body:
        header.insert(0, "import pandas as pd")
    names = sorted({match.group(2) for match in _PLACEHOLDER.finditer(body)})
    for name in names:
        header.insert(-1, f'{name} = "[[ {name} ]]"  # TODO: dynamic variable in the old YAML schema; import its real value')
    if names:
        header.insert(-1, "")
    return "\n".join(header) + _use_names(body)


def _options(validation) -> list:
    defaults = {"include_left": False, "include_right": False} if validation.name == "is_between" else {}
    return sorted({**defaults, **validation.options}.items(), key=str)


def _comparable(schema: DataframeSchema) -> dict:
    return {
        name: (col.data_type, sorted((v.name, _options(v)) for v in col.validations if v.apply), [getattr(m, "value", m) for m in col.metrics]) for name, col in schema.columns.items()
    }


def schemas_equivalent(left: DataframeSchema, right: DataframeSchema) -> bool:
    """True if both schemas declare the same dtypes, applied validations (defaults normalised) and metrics, per column."""
    return left.name == right.name and _comparable(left) == _comparable(right)


def compile_module(source: str) -> Dict[str, Any]:
    """Execute generated module source and return its namespace."""
    namespace: Dict[str, Any] = {}
    exec(compile(source, "<migrated schemas>", "exec"), namespace)  # pylint: disable=exec-used
    return namespace


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


_CALL_SITE_PATTERNS = (
    (".py", re.compile(r"SCHEMA_FROM_FILE|dynamicio_schema|^\s*schema(_validations|_metrics)?\s*=")),
    (".yaml", re.compile(r"^\s*schema:\s*$|^\s*file_path:.*schema")),
    (".yml", re.compile(r"^\s*schema:\s*$|^\s*file_path:.*schema")),
)


def find_call_sites(root: str, exclude: Sequence[str] = ()) -> List[Tuple[str, int, str]]:
    """Locate code and resource-definition lines that still declare schemas the old way: (file, line number, text)."""
    sites: List[Tuple[str, int, str]] = []
    for current, dirs, files in sorted(os.walk(root)):
        dirs[:] = sorted(d for d in dirs if not d.startswith(".") and d not in ("node_modules", "__pycache__", "venv"))
        for file_name in sorted(files):
            file_path = os.path.join(current, file_name)
            if file_path in exclude:
                continue
            for extension, pattern in _CALL_SITE_PATTERNS:
                if file_name.endswith(extension):
                    with open(file_path, encoding="utf-8", errors="replace") as handle:
                        sites.extend((file_path, number, line.rstrip()) for number, line in enumerate(handle, 1) if pattern.search(line))
    return sites


PROMPT_TEMPLATE = """\
# dynamicio v10 migration: update the call sites

`dynamicio migrate` converted the YAML schemas into Pandera models in `{module}`. Finish the migration by editing the
call sites below. Do not change validation or metric behaviour: dataset names (`Config.name`), metric log lines and
validation messages must stay identical.

## Generated models

{models}

## What to change

1. IO classes using `schema = SCHEMA_FROM_FILE` : either set `schema = <Model>` when the class serves one dataset, or use
   `schema = SCHEMA_FROM_ARGUMENT` (from `dynamicio.core`) and pass `schema=<Model>` when constructing it.
2. Remove the `schema:` / `file_path:` blocks from the resource definition YAML files (resources stay in YAML).
3. Names marked `# TODO` at the top of the models module stood for dynamic variables (`[[ NAME ]]`) in the old YAML. Replace
   each placeholder with an import of the real value, e.g. `from my_project.environment import LOWER_THAN_LIMIT`.
4. Dict-style schemas (`schema = {{...}}`, `schema_validations`, `schema_metrics`) were not converted. Translate them into
   `pa.DataFrameModel` classes following the generated ones: always pass `nullable=` explicitly (Pandera defaults to
   `False`, dynamicio allows nulls unless `has_no_null_values` is declared); put anything without a Pandera equivalent in
   `pa.Field(metadata={{"dynamicio": {{"validations": {{...}}, "metrics": [...]}}}})`.
5. Run the tests; metric and validation tests must pass unchanged.

## Call sites to review

{sites}
"""


def render_prompt(module: str, models: Mapping[str, str], sites: Sequence[Tuple[str, int, str]]) -> str:
    """Return the Markdown prompt that hands the remaining call-site edits to an LLM."""
    model_lines = "\n".join(f"- `{cls}` (from `{source}`)" for cls, source in models.items()) or "- (none)"
    site_lines = "\n".join(f"- `{path}:{number}`: `{text.strip()}`" for path, number, text in sites) or "- (none found)"
    return PROMPT_TEMPLATE.format(module=module, models=model_lines, sites=site_lines)


def migrate(path: str, output: str, scan: str = "") -> Dict[str, Any]:
    """Convert every YAML schema under ``path`` into one models module at ``output``, verify parity and write a prompt next to it.

    Raises:
        ValueError: If no schemas are found, two schemas map to the same class name, or a generated model does not compile back to its source schema.
    """
    schemas = find_yaml_schemas(path)
    if not schemas:
        raise ValueError(f"No dynamicio YAML schemas found under {path}")
    by_class: Dict[str, Tuple[str, DataframeSchema]] = {}
    for file_path, schema in schemas.items():
        # Named after the file: schema names need not be unique (and are what metric lines report), file names are.
        cls = _class_name(os.path.splitext(os.path.basename(file_path))[0])
        if cls in by_class:
            cls = _class_name(os.path.basename(os.path.dirname(file_path))) + cls
        if cls in by_class:
            raise ValueError(f"Schemas {by_class[cls][0]} and {file_path} both map to class {cls}")
        by_class[cls] = (file_path, schema)

    source = render_module({cls: schema for cls, (_, schema) in by_class.items()})
    namespace = compile_module(source)
    for cls, (file_path, schema) in by_class.items():
        if not schemas_equivalent(to_dataframe_schema(namespace[cls]), schema):
            raise ValueError(f"Generated model {cls} is not equivalent to {file_path}")

    todos = [line.split(" =")[0] for line in source.splitlines() if "# TODO" in line]
    with open(output, "w", encoding="utf-8") as handle:
        handle.write(source)
    prompt_path = os.path.join(os.path.dirname(os.path.abspath(output)), "MIGRATION_PROMPT.md")
    sites = find_call_sites(scan or os.getcwd(), exclude=[os.path.abspath(output)])
    with open(prompt_path, "w", encoding="utf-8") as handle:
        handle.write(render_prompt(output, {cls: file_path for cls, (file_path, _) in by_class.items()}, sites))
    return {"models": list(by_class), "module": output, "prompt": prompt_path, "call_sites": len(sites), "todos": todos}
