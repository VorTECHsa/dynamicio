"""Responsible for configuring io operations for input data."""
# pylint: disable=too-few-public-methods
__all__ = ["InputIO", "StagedFoo", "StagedBar"]

from sqlalchemy.ext.declarative import declarative_base

from dynamicio import UnifiedIO, WithLocal, WithPostgres, WithS3File
from dynamicio.core import SCHEMA_FROM_FILE, DynamicDataIO

Base = declarative_base()


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
