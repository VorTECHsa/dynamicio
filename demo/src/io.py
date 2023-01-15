"""Responsible for configuring io operations for input data."""
# pylint: disable=too-few-public-methods
__all__ = ["InputIO", "StagedFoo", "StagedBar"]

from pandera import SchemaModel, String, Float, Field
from pandera.typing import Series

from dynamicio import UnifiedIO, WithLocal, WithPostgres, WithS3File
from dynamicio.core import SCHEMA_FROM_FILE, DynamicDataIO


class Foo(SchemaModel):
    column_a: Series[String] = Field(unique=True, report_duplicates="all", logging={"metrics": ["Counts"], "dataset_name": "Foo", "column": "column_a"})
    column_b: Series[String] = Field(nullable=False, logging={"metrics": ["CountsPerLabel"], "dataset_name": "Foo", "column": "column_a"})
    column_c: Series[Float] = Field(gt=1000)
    column_d: Series[Float] = Field(lt=1000, logging={"metrics": ["Min", "Max", "Mean", "Std", "Variance"], "dataset_name": "Foo", "column": "column_a"})


class InputIO(UnifiedIO):
    """UnifiedIO subclass for V6 data."""

    schema = SCHEMA_FROM_FILE


class StagedFoo(WithS3File, WithLocal, DynamicDataIO):
    """UnifiedIO subclass for staged foos."""

    schema = {
        "column_a": "object",
        "column_b": "object",
        "column_c": "int64",
        "column_d": "int64",
    }


class StagedBar(WithLocal, WithPostgres, DynamicDataIO):
    """UnifiedIO subclass for cargo movements volumes data."""

    schema = {
        "column_a": "object",
        "column_b": "object",
        "column_c": "int64",
        "column_d": "int64",
    }
