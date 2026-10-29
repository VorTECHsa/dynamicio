"""Responsible for configuring io operations for input data."""
# pylint: disable=too-few-public-methods
__all__ = ["InputIO", "StagedFoo", "StagedBar"]

from demo.src.resources.schemas.raw import StagedBarModel, StagedFooModel
from dynamicio import UnifiedIO, WithLocal, WithPostgres, WithS3File
from dynamicio.core import SCHEMA_FROM_ARGUMENT, DynamicDataIO
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class InputIO(UnifiedIO):
    """UnifiedIO subclass for the input and processed datasets; each instance takes its Pandera model: ``InputIO(config, schema=Foo)``."""

    schema = SCHEMA_FROM_ARGUMENT


class StagedFoo(WithS3File, WithLocal, DynamicDataIO):
    """IO class for staged foos: it needs S3 and local mixins, so the model is fixed on the class."""

    schema = StagedFooModel


class StagedBar(WithLocal, WithPostgres, DynamicDataIO):
    """IO class for staged bars: it needs local and postgres mixins, so the model is fixed on the class."""

    schema = StagedBarModel
