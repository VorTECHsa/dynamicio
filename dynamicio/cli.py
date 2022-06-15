"""Implements the dynamicio Command Line Interface (CLI)."""
import argparse
import glob
import os
import pprint
from typing import Mapping, MutableMapping, Optional, Sequence

import pandas as pd  # type: ignore
import yaml

from dynamicio.errors import InvalidDatasetTypeError


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
                with open(os.path.join(args.output, f"{json_schema['name']}.yaml"), "w") as yml:  # pylint: disable=unspecified-encoding]
                    yaml.safe_dump(json_schema, yml)

    if args.single:
        json_schema = generate_schema_for(str(args.path))
        with open(os.path.join(args.output, f"{json_schema['name']}.yaml"), "w") as yml:  # pylint: disable=unspecified-encoding]
            yaml.safe_dump(json_schema, yml)
        pprint.pprint(json_schema)


def run():
    """Entry point for the dynamicio cli.py."""
    args = parse_args()
    main(args)
