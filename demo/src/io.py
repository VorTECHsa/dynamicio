"""Responsible for configuring io operations for input data."""
# pylint: disable=too-few-public-methods
__all__ = ["Foo", "Bar", "StagedFoo", "StagedBar", "BarDataModel", "FinalFoo", "FinalBar"]

from sqlalchemy import Column, Float, String
from sqlalchemy.ext.declarative import declarative_base

from dynamicio import UnifiedIO, WithKafka, WithLocal, WithPostgres, WithS3File
from dynamicio.core import SCHEMA_FROM_FILE, DynamicDataIO

Base = declarative_base()


class Foo(UnifiedIO):
    """UnifiedIO subclass for V6 data."""

    schema = SCHEMA_FROM_FILE


class Bar(UnifiedIO):
    """UnifiedIO subclass for cargo movements volumes data."""

    schema = SCHEMA_FROM_FILE


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


class FinalFoo(UnifiedIO):
    """UnifiedIO subclass for V6 data."""

    schema = SCHEMA_FROM_FILE


class FinalBar(WithLocal, WithKafka, DynamicDataIO):
    """UnifiedIO subclass for cargo movements volumes data."""

    schema = SCHEMA_FROM_FILE


class BarDataModel(Base):
    """Sql_alchemy model for Bar table."""

    __tablename__ = "bar"

    column_a = Column(String(64), primary_key=True)
    column_b = Column(String(64))
    column_c = Column(Float)
    column_d = Column(Float)
