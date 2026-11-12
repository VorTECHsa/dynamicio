# Using asyncio

`Dynamic(i/o)` supports use of `asyncio` to speed up `I/O bound` operations through leveraging multithreading. 

An example can be found in the second of the two demo tasks, namely, the `transform.py` task.
```python
"""Add module docstring...."""
import asyncio
import logging

from demo.src import transform_input_config, transform_output_config
from demo.src.resources.io import InputIO, StagedBar, StagedFoo

logger = logging.getLogger(__name__)


async def main() -> None:
    """The entry point for the Airflow Staging task.

    Returns:
        Void function.
    """
    # LOAD DATA
    logger.info("Loading data from live sources...")

    [bar_df, foo_df] = await asyncio.gather(
        StagedBar(source_config=transform_input_config.get(source_key="STAGED_BAR")).async_read(),
        StagedFoo(source_config=transform_input_config.get(source_key="STAGED_FOO")).async_read()
    )

    logger.info("Data successfully loaded from live sources...")

    # TRANSFORM  DATA
    logger.info("Apply transformations...")

    # TODO: Apply your transformations

    logger.info("Transformations applied successfully...")

    # SINK DATA
    logger.info("Begin sinking data to the processed area:")
    await asyncio.gather(
        InputIO(source_config=transform_output_config.get(source_key="FINAL_FOO"), schema=FinalFoo, apply_schema_validations=True, log_schema_metrics=True).async_write(foo_df),
        InputIO(source_config=transform_output_config.get(source_key="FINAL_BAR"), schema=FinalBar, apply_schema_validations=True, log_schema_metrics=True).async_write(bar_df),
    )
    logger.info("Data staging is complete...")

```
In short, you simply need to utilise the `async_read()` or the `async_write()` methods instead, plus await and gather your calls.
