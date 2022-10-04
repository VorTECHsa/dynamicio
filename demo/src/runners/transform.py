"""Add module docstring...."""
import asyncio
import logging

import demo.src.environment
from demo.src import processed_config, raw_config
from demo.src.io import InputIO, StagedBar, StagedFoo

logger = logging.getLogger(__name__)


async def main() -> None:
    """The entry point for the Airflow Staging task.

    Returns:
        Void function.
    """
    # LOAD DATA
    logger.info("Loading data from live sources...")

    [bar_df, foo_df] = await asyncio.gather(
        StagedBar(source_config=raw_config.get(source_key="STAGED_BAR")).async_read(), StagedFoo(source_config=raw_config.get(source_key="STAGED_FOO")).async_read()
    )

    logger.info("Data successfully loaded from live sources...")

    # TRANSFORM  DATA
    logger.info("Apply transformations...")

    # TODO: Apply your transformations

    logger.info("Transformations applied successfully...")

    # SINK DATA
    logger.info(f"Begin sinking data to staging area: S3:{demo.src.environment.S3_YOUR_OUTPUT_BUCKET}:live/data/raw")
    await asyncio.gather(
        InputIO(source_config=processed_config.get(source_key="FINAL_FOO"), apply_schema_validations=True, log_schema_metrics=True).async_write(foo_df),
        InputIO(source_config=processed_config.get(source_key="FINAL_BAR"), apply_schema_validations=True, log_schema_metrics=True).async_write(bar_df),
    )
    logger.info("Data staging is complete...")
