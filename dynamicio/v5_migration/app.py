# pylint: skip-file
# noqa
# type: ignore

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import typer
import yaml
from rich import print as rich_print

from dynamicio.v5_migration.resource_migration import (
    convert_single_resource_file,
    is_resource_dict,
    resources_import_str,
)
from dynamicio.v5_migration.schema_migration import convert_single_schema_file, is_schema_dict, schema_import_str

app = typer.Typer()


@app.command()
def convert_everything(source: Path, destination: Path):
    """Converts every item as far as possible. Paths can be dirs or files."""
    schemas_source_destination, schemas_to_be_written = gather_schema_migration_actions(source, destination)
    resources_source_destination, resources_to_be_written = gather_resource_migration_actions(source, destination)

    source_destination_pairs = schemas_source_destination + resources_source_destination
    files_to_be_written = schemas_to_be_written + resources_to_be_written

    confirm_migration_actions(source_destination_pairs, files_to_be_written)
    write_files(files_to_be_written)


@dataclass
class SourceDestinationPair:
    source: Path
    destination: Path


@dataclass
class FilesToBeWritten:
    target_file: Path
    target_content: str


@app.command()
def convert_resources(source: Path, destination: Path):
    """Converts only resource yamls."""
    files_to_be_written, source_destination_pairs = gather_resource_migration_actions(destination, source)

    confirm_migration_actions(source_destination_pairs, files_to_be_written)
    write_files(files_to_be_written)


def gather_resource_migration_actions(source: Path, destination: Path):
    source_content, source_path = handle_source_path(source)
    source_content = {source: contents for source, contents in source_content.items() if is_resource_dict(contents)}
    source_destination_pairs: list[SourceDestinationPair]
    files_to_be_written: list[FilesToBeWritten]
    source_destination_pairs, files_to_be_written = generate_source_destination_actions(
        source_path,
        source_content,
        destination,
        resources_import_str,
        convert_single_resource_file,
    )
    return source_destination_pairs, files_to_be_written


@app.command()
def convert_schemas(source: Path, destination: Path):
    """Converts only schemas."""
    source_destination_pairs, files_to_be_written = gather_schema_migration_actions(source, destination)

    confirm_migration_actions(source_destination_pairs, files_to_be_written)
    write_files(files_to_be_written)


def gather_schema_migration_actions(
    source: Path, destination: Path
) -> tuple[list[SourceDestinationPair], list[FilesToBeWritten]]:
    """Gathers the source destination pairs and files to be written."""

    source_content, source_path = handle_source_path(source)
    source_content = {source: contents for source, contents in source_content.items() if is_schema_dict(contents)}

    source_destination_pairs: list[SourceDestinationPair]
    files_to_be_written: list[FilesToBeWritten]
    source_destination_pairs, files_to_be_written = generate_source_destination_actions(
        source_path,
        source_content,
        destination,
        schema_import_str,
        convert_single_schema_file,
    )

    return source_destination_pairs, files_to_be_written


#  ------------------


def generate_source_destination_actions(
    source_path: Path,
    source_content: dict[Path, dict],
    destination: Path,
    import_str: str,
    contents_to_code_conversion_func: Callable[[dict], str],
) -> tuple[list[SourceDestinationPair], list[FilesToBeWritten]]:
    """Generates the source destination pairs and files to be written."""
    source_destination_pairs: list[SourceDestinationPair] = []
    files_to_be_written: list[FilesToBeWritten] = []

    if destination.suffix == ".py":
        python_str = import_str

        for _source, contents in source_content.items():
            python_str += contents_to_code_conversion_func(contents)
            source_destination_pairs.append(SourceDestinationPair(_source, destination))

        files_to_be_written.append(FilesToBeWritten(destination.with_suffix(".py"), python_str))

    elif destination.suffix == "":
        for _source, contents in source_content.items():
            python_str = import_str
            python_str += contents_to_code_conversion_func(contents)

            sub_path = _source.relative_to(source_path)
            destination_path = destination / sub_path.with_suffix(".py")

            source_destination_pairs.append(SourceDestinationPair(_source, destination_path))
            files_to_be_written.append(FilesToBeWritten(destination_path, python_str))
    else:
        raise ValueError(
            f"Destination {destination} is not a directory or python file. Found suffix {destination.suffix}."
        )
    return source_destination_pairs, files_to_be_written


def handle_source_path(source: Path) -> tuple[dict[Path, dict], Path]:
    """returns a tuple of source_content and source_path

    source_content is a dict of paths and their yaml contents.
    source_path is the path of the source directory or parent directory of source if source is a path.
    """
    if source.is_file():
        sources = [source]
        source_path = source.parent
    elif source.is_dir():
        sources = list(source.glob("**/*.yaml"))
        source_path = source
    else:
        raise ValueError(f"Source {source} is not a file or directory")

    source_content = {source: yaml.safe_load(source.open()) for source in sources}
    return source_content, source_path


def write_files(files_to_be_written: list[FilesToBeWritten]):
    """Writes the files to be written."""
    for write_file in files_to_be_written:
        write_file.target_file.parent.mkdir(parents=True, exist_ok=True)
        write_file.target_file.write_text(write_file.target_content)


def confirm_migration_actions(
    source_destination_pairs: list[SourceDestinationPair],
    files_to_be_written: list[FilesToBeWritten],
):
    """Confirms the migration actions."""
    rich_print(f"[bold red]Found [green]{len(source_destination_pairs)}[/green] source destination pairs:[/bold red]")
    for pair in source_destination_pairs:
        rich_print(f"[blue] - [/blue]{pair.source} -> {pair.destination}")

    rich_print(f"[bold red]Found [green]{len(files_to_be_written)}[/green] files to be written:[/bold red]")

    for write_file in files_to_be_written:
        loc = write_file.target_content.count("\n")
        rich_print(f"[bold blue] - [/bold blue]{write_file.target_file} - ({loc} lines of code.)")

    typer.confirm("\nProceed writing?")
