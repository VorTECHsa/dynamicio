"""The IO class of the polars demo pipeline. Each IO object takes its polars model: ``PolarsIO(config, schema=Foo)``."""
__all__ = ["PolarsIO"]

from dynamicio import UnifiedIO
from dynamicio.core import SCHEMA_FROM_ARGUMENT


class PolarsIO(UnifiedIO):
    """UnifiedIO for all the datasets of the pipeline. A polars model on the pandas engine raises ``ModelEngineMismatchError``."""

    schema = SCHEMA_FROM_ARGUMENT
