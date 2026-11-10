"""Implements the dynamicio Command Line Interface (CLI)."""

import argparse
import glob
import os
import pprint
import sys
from typing import Mapping, MutableMapping, Optional, Sequence

import pandas as pd  # type: ignore
import yaml

# Application Imports
from dynamicio.errors import InvalidDatasetTypeError
from dynamicio.schema.migrate import migrate


def parse_args(args: Optional[Sequence] = None) -> argparse.Namespace:
    """Arguments parser for dynamicio cli.py.

    Args:
        args: List of args to be parsed. Defaults to None, in which case
            sys.argv[1:] is used.

    Returns:
        An instance of ArgumentParser populated with the provided args.
    """
    parser = argparse.ArgumentParser(prog="dynamicio", description="Generate dataset schemas")
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
    parser.add_argument("-o", "--output", required=True, help="the path to the schemas output directory.", type=str)
    return parser.parse_args(args)


def generate_schema_for(dataset: str) -> Mapping:
    """Generate a schema for a dataset.

    Args:
        dataset: The path to the dataset for which we want to generate a schema

    Returns:
        A dictionary containing the schema for the dataset, or None if the dataset is not valid.

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
    json_schema: MutableMapping = {"name": dataset_name, "columns": {}}
    for column, d_type in zip(list(df.columns), list(df.dtypes)):
        json_schema["columns"][column] = {"type": "", "validations": {}, "metrics": []}
        json_schema["columns"][column]["type"] = d_type.name

    return json_schema


def main(args: argparse.Namespace):
    """Main function for dynamicio cli.py.

    Args:
        args: Parsed args.
    """
    if args.batch:
        dataset_files = glob.glob(os.path.join(args.path, "*.*"))
        for dataset in dataset_files:
            try:
                json_schema = generate_schema_for(dataset)
            except InvalidDatasetTypeError as exception:
                print(f"Skipping {exception.message}! You may want to remove this file from the datasets directory")
            else:
                with open(os.path.join(args.output, f"{json_schema['name']}.yaml"), "w") as file:  # pylint: disable=unspecified-encoding]
                    file.write("---\n")
                    yaml.safe_dump(json_schema, file)

    if args.single:
        json_schema = generate_schema_for(str(args.path))
        with open(os.path.join(args.output, f"{json_schema['name']}.yaml"), "w") as file:  # pylint: disable=unspecified-encoding]
            file.write("---\n")
            yaml.safe_dump(json_schema, file)
        pprint.pprint(json_schema)


def run_migrate(argv: Sequence[str]) -> None:
    """The ``dynamicio migrate`` subcommand: YAML schemas to Pandera models, plus an LLM prompt for the call sites."""
    parser = argparse.ArgumentParser(prog="dynamicio migrate", description="Convert YAML schemas into Pandera models")
    parser.add_argument("path", help="directory searched for YAML schema files")
    parser.add_argument(
        "-o", "--output", default="schemas.py", help="a models module (ends with .py) or a package directory with one module per sub-directory of the YAML schemas (default: schemas.py)"
    )
    parser.add_argument("--scan", default="", help="directory searched for call sites to list in the prompt (default: the current directory)")
    args = parser.parse_args(argv)
    report = migrate(args.path, args.output, args.scan)
    print(f"Wrote {len(report['models'])} models to {report['module']}: {', '.join(report['models'])}")
    if report["todos"]:
        print(f"Dynamic variables to import by hand: {', '.join(report['todos'])}")
    print(f"Wrote {report['prompt']} ({report['call_sites']} call sites to review). Give it to your LLM or edit them by hand.")


def run():
    """Entry point for the dynamicio cli.py."""
    if sys.argv[1:2] == ["migrate"]:
        run_migrate(sys.argv[2:])
        return
    args = parse_args()
    main(args)
