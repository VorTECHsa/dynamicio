"""The transform task on the polars engine: scan the staged files and sink the processed files, without an eager frame."""
import logging

from demo.polars.src import transform_input_config, transform_output_config
from demo.polars.src.resources.io import PolarsIO
from demo.polars.src.resources.schemas.processed import FinalBar, FinalFoo
from demo.polars.src.resources.schemas.raw import StagedBarModel, StagedFooModel

logger = logging.getLogger(__name__)


def main() -> None:
    """Scan STAGED_FOO and STAGED_BAR, and sink them to FINAL_FOO and FINAL_BAR.

    ``scan()`` does not validate, so the validations and the metrics of the final models run in ``sink()``. A failed validation raises
    ``SchemaValidationError``, and the final file does not change.
    """
    foo = PolarsIO(source_config=transform_input_config.get(source_key="STAGED_FOO"), schema=StagedFooModel).scan()
    bar = PolarsIO(source_config=transform_input_config.get(source_key="STAGED_BAR"), schema=StagedBarModel).scan()

    PolarsIO(source_config=transform_output_config.get(source_key="FINAL_FOO"), schema=FinalFoo, apply_schema_validations=True, log_schema_metrics=True).sink(foo)
    PolarsIO(source_config=transform_output_config.get(source_key="FINAL_BAR"), schema=FinalBar, apply_schema_validations=True, log_schema_metrics=True).sink(bar)
    logger.info("Data processing is complete...")
