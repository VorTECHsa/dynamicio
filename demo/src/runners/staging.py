"""Add module docstring...."""
import logging

from demo.src import constants, staging_input_config, staging_output_config
from demo.src.io import InputIO, StagedFooIO, StagedBarIO
from demo.src.io.schemas import Bar, Foo

logger = logging.getLogger(__name__)


def main() -> None:
    """The entry point for the Airflow Staging task.

    Returns:
        Void function.
    """
    # LOAD DATA
    logger.info("Loading data from live sources...")

    bar_df = UnifiedIO.read(
        resource_definition=staging_input_config.get(source_key="BAR"),
        schema=Bar,
        apply_schema_validations=True,
        log_schema_metrics=True
    )
    foo_df = InputIO(resource_definition=staging_input_config.get(source_key="FOO"), schema=Foo, apply_schema_validations=True, log_schema_metrics=True).read()

    logger.info("Data successfully loaded from live sources...")

    # TRANSFORM  DATA
    logger.info("Apply transformations...")

    # TODO: Apply your transformations

    logger.info("Transformations applied successfully...")

    # SINK DATA
    logger.info("Begin sinking data to staging area:")
    UnifiedIO(resource_definition=staging_output_config.get(source_key="STAGED_FOO"), **constants.TO_PARQUET_KWARGS).write(foo_df)
    UnifiedIO(resource_definition=staging_output_config.get(source_key="STAGED_BAR")).write(bar_df)
    logger.info("Data staging is complete...")



 # 1. Use UnifiedIO as IO class for loading writing data
 # 2. Introduce pandera schema as extra field in UnifiedIO parameters
 # 3. Write script to generate pandera schema from dataframe (extra propose logging and validations too)
 # 4. Write script to migrate from yamls schemas to pandera schemas
 # 5. Write validations that are not available through pandera standard validations but we use after we identify what is missing!
 # 6. Confirm everything works
