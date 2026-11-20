"""Implements the dynamicio Command Line Interface (CLI)."""

import argparse
import glob
import os
import sys
from typing import Optional, Sequence

import pandas as pd  # type: ignore

# Application Imports
from dynamicio.config.pydantic import DataframeSchema
from dynamicio.engines.engine_setting import EngineSetting
from dynamicio.errors import InvalidDatasetTypeError
from dynamicio.schema.migrate import ModelRenderer, migrate


def parse_args(args: Optional[Sequence] = None) -> argparse.Namespace:
    """Arguments parser for dynamicio cli.py.

    Args:
        args: List of args to be parsed. Defaults to None, in which case
            sys.argv[1:] is used.

    Returns:
        An instance of ArgumentParser populated with the provided args.
    """
    parser = argparse.ArgumentParser(prog="dynamicio", description="Generate a Pandera model (a .py module) for each dataset")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "-b",
        "--batch",
        action="store_true",
        help="flag, used to generate multiple schemas provided a datasets directory.",
    )
    group.add_argument(
        "-s",
        "--single",
        action="store_true",
        help="flag, used to generate a schema provided a single dataset.",
    )
    parser.add_argument("-p", "--path", required=True, help="the path to the dataset/datasets-directory.", type=str)
    parser.add_argument("-o", "--output", required=True, help="the path to the output directory for the generated models.", type=str)
    return parser.parse_args(args)


def generate_schema_for(dataset: str) -> DataframeSchema:
    """Generate a schema for a dataset.

    Args:
        dataset: The path to the dataset for which we want to generate a schema

    Returns:
        The schema of the dataset: its name (the file name without extension) and each column with the dtype pandas read.

    Raises:
        InvalidDatasetTypeError: If the dataset type is not supported by dynamicio.
    """
    dataset_name, file_type = os.path.splitext(os.path.basename(dataset))

    if file_type == ".parquet":
        df = pd.read_parquet(dataset)
    elif file_type == ".csv":
        df = pd.read_csv(dataset)
    elif file_type == ".json":
        df = pd.read_json(dataset)
    elif file_type == ".h5":
        df = pd.read_hdf(dataset)
    else:
        raise InvalidDatasetTypeError(dataset)

    print(f"Generating schema for: {dataset}")
    columns = {str(column): {"type": d_type.name, "validations": {}, "metrics": []} for column, d_type in zip(df.columns, df.dtypes)}
    return DataframeSchema(name=dataset_name, columns=columns)


def write_model_for(schema: DataframeSchema, output: str) -> str:
    """Write the Pandera model of ``schema`` to ``<output>/<module>.py`` and return its source; the module is named after the dataset, made importable."""
    source = ModelRenderer.render_module({ModelRenderer.class_name(schema.name): schema}, docstring="The dtypes of the dataset {name!r}: declare nullability and validations by hand.")
    with open(os.path.join(output, f"{ModelRenderer.module_name(schema.name)}.py"), "w", encoding="utf-8") as file:
        file.write(source)
    return source


def main(args: argparse.Namespace):
    """Main function for dynamicio cli.py.

    Args:
        args: Parsed args.
    """
    if args.batch:
        dataset_files = glob.glob(os.path.join(args.path, "*.*"))
        for dataset in dataset_files:
            try:
                schema = generate_schema_for(dataset)
            except InvalidDatasetTypeError as exception:
                print(f"Skipping {exception.message}! You may want to remove this file from the datasets directory")
            else:
                write_model_for(schema, args.output)

    if args.single:
        print(write_model_for(generate_schema_for(str(args.path)), args.output))


def run_migrate(argv: Sequence[str]) -> None:
    """The ``dynamicio migrate`` subcommand: YAML schemas (dynamicio 8 and earlier) to the Pandera models of dynamicio 11, plus an LLM prompt for the call sites."""
    parser = argparse.ArgumentParser(prog="dynamicio migrate", description="Convert YAML schemas into Pandera models")
    parser.add_argument("path", help="directory searched for YAML schema files")
    parser.add_argument(
        "-o", "--output", default="schemas.py", help="a models module (ends with .py) or a package directory with one module per sub-directory of the YAML schemas (default: schemas.py)"
    )
    parser.add_argument("--scan", default="", help="directory searched for call sites (or, with --engine polars, resource definitions) to list (default: the current directory)")
    parser.add_argument(
        "--engine",
        choices=("pandas", "polars"),
        default="pandas",
        help="pandas: YAML schemas to pandas models (the default). polars: the pandas models of a module or a package directory to polars models",
    )
    args = parser.parse_args(argv)
    if args.engine == "polars":
        run_polars_migrate(args.path, args.output, args.scan)
        return
    report = migrate(args.path, args.output, args.scan)
    print(f"Wrote {len(report['models'])} models to {report['module']}: {', '.join(report['models'])}")
    if report["todos"]:
        print(f"Dynamic variables to import by hand: {', '.join(report['todos'])}")
    print(f"Wrote {report['prompt']} ({report['call_sites']} call sites to review). Give it to your LLM or edit them by hand.")


def run_polars_migrate(path: str, output: str, scan: str) -> None:
    """``dynamicio migrate --engine polars``: the pandas models at ``path`` to polars models, plus a report of the checks and resources to change.

    Examples:
        Run it from the root of the repo, because it imports the modules of the models by their dotted name:

        ```bash
        dynamicio migrate --engine polars src/resources/schemas -o src/resources/polars_schemas --scan src/resources
        ```
    """
    EngineSetting.require_polars()
    from dynamicio.schema.polars_migrate import migrate_to_polars  # pylint: disable=import-outside-toplevel

    report = migrate_to_polars(path, output, scan)
    print(f"Wrote {len(report['models'])} polars models to {report['module']}: {', '.join(report['models'])}")
    print(
        f"Wrote {report['report']}: {len(report['todos'])} checks to port, {len(report['objects'])} object columns to review, {len(report['refused'])} refused resources, {len(report['csv_index'])} CSV resources without index."
    )


def run():
    """Entry point for the dynamicio cli.py."""
    if sys.argv[1:2] == ["migrate"]:
        run_migrate(sys.argv[2:])
        return
    args = parse_args()
    main(args)
