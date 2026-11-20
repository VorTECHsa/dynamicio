"""The staging task on the polars engine: read the inputs with validations and metrics, and write them to the staging area."""
import logging

from demo.polars_pipeline.src import staging_input_config, staging_output_config
from demo.polars_pipeline.src.resources.io import PolarsIO
from demo.polars_pipeline.src.resources.schemas.input import Bar, Foo
from demo.polars_pipeline.src.resources.schemas.raw import StagedBarModel, StagedFooModel

logger = logging.getLogger(__name__)


def main() -> None:
    """Read FOO and BAR as polars frames, and write them with the staged models, which cast the floats to Int64."""
    bar = PolarsIO(source_config=staging_input_config.get(source_key="BAR"), schema=Bar, apply_schema_validations=True, log_schema_metrics=True).read()
    foo = PolarsIO(source_config=staging_input_config.get(source_key="FOO"), schema=Foo, apply_schema_validations=True, log_schema_metrics=True).read()

    PolarsIO(source_config=staging_output_config.get(source_key="STAGED_FOO"), schema=StagedFooModel).write(foo)
    PolarsIO(source_config=staging_output_config.get(source_key="STAGED_BAR"), schema=StagedBarModel).write(bar)
    logger.info("Data staging is complete...")
