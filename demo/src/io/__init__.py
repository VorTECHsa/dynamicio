"""Responsible for configuring io operations for input data."""
# pylint: disable=too-few-public-methods
__all__ = ["InputIO", "StagedFooIO", "StagedBarIO"]


from dynamicio import UnifiedIO, WithS3File, WithLocal, DynamicDataIO, WithPostgres


class InputIO(UnifiedIO):
    """UnifiedIO subclass for V6 data."""


class StagedFooIO(WithS3File, WithLocal, DynamicDataIO):
    """UnifiedIO subclass for staged foos."""


class StagedBarIO(WithLocal, WithPostgres, DynamicDataIO):
    """UnifiedIO subclass for cargo movements volumes data."""
