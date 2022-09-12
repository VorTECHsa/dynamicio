"""Responsible for configuring io operations for input data."""
# pylint: disable=too-few-public-methods
__all__ = ["StagedFoo", "StagedBar"]

from sqlalchemy import Column, Float, String
from sqlalchemy.ext.declarative import declarative_base

from dynamicio import UnifiedIO, WithKafka, WithLocal, WithPostgres, WithS3File
from dynamicio.core import SCHEMA_FROM_FILE, DynamicDataIO

Base = declarative_base()


class StagedFoo(WithS3File, WithLocal, DynamicDataIO):
    """UnifiedIO subclass for staged foos6."""

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
