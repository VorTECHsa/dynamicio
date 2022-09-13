"""Add module docstring...."""
import logging

from demo.src import constants, input_config, raw_config
from demo.src.io import InputIO, StagedBar, StagedFoo

logger = logging.getLogger(__name__)


def main() -> None:
    """The entry point for the Airflow Staging task.

    Returns:
        Void function.
    """
    # LOAD DATA
    logger.info("Loading data from live sources...")

    bar_df = InputIO(source_config=input_config.get(source_key="BAR"), apply_schema_validations=True, log_schema_metrics=True).read()
    foo_df = InputIO(source_config=input_config.get(source_key="FOO"), apply_schema_validations=True, log_schema_metrics=True).read()

    logger.info("Data successfully loaded from live sources...")

    # TRANSFORM  DATA
    logger.info("Apply transformations...")

    # TODO: Apply your transformations

    logger.info("Transformations applied successfully...")

    # SINK DATA
    logger.info("Begin sinking data to staging area:")
    StagedFoo(source_config=raw_config.get(source_key="STAGED_FOO"), **constants.TO_PARQUET_KWARGS).write(foo_df)
    StagedBar(source_config=raw_config.get(source_key="STAGED_BAR")).write(bar_df)
    logger.info("Data staging is complete...")
