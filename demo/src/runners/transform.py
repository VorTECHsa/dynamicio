"""Add module docstring...."""
import logging

import demo.src.environment
from demo.src import constants, processed_config, raw_config
from demo.src.io import FinalBar, FinalFoo, StagedBar, StagedFoo

logger = logging.getLogger(__name__)


def main() -> None:
    """The entry point for the Airflow Staging task.

    Returns:
        Void function.
    """
    # LOAD DATA
    logger.info("Loading data from live sources...")

    bar_df = StagedBar(
        source_config=raw_config.get(source_key="STAGED_BAR"),
    ).read()
    foo_df = StagedFoo(
        source_config=raw_config.get(source_key="STAGED_FOO"),
    ).read()

    logger.info("Data successfully loaded from live sources...")

    # TRANSFORM  DATA
    logger.info("Apply transformations...")
    # do transformations
    logger.info("Transformations applied successfully...")

    # SINK DATA
    logger.info(f"Begin sinking data to staging area: S3:{demo.src.environment.S3_YOUR_OUTPUT_BUCKET}:live/data/raw")
    FinalFoo(source_config=processed_config.get(source_key="FINAL_FOO"), apply_schema_validations=True, log_schema_metrics=True).write(foo_df)
    FinalBar(source_config=processed_config.get(source_key="FINAL_BAR"), apply_schema_validations=True, log_schema_metrics=True).write(bar_df)
    logger.info("Data staging is complete...")
