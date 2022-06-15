"""Set config IOs."""
__all__ = ["input_config", "raw_config", "processed_config"]

import logging
import os

from demo.src import environment
from demo.src.environment import ENVIRONMENT, RESOURCES
from dynamicio.config import IOConfig

logging.basicConfig(level=logging.INFO)
logging.getLogger("kafka").setLevel(logging.WARNING)


input_config = IOConfig(
    path_to_source_yaml=(os.path.join(RESOURCES, "definitions/input.yaml")),
    env_identifier=ENVIRONMENT,
    dynamic_vars=environment,
)
raw_config = IOConfig(
    path_to_source_yaml=(os.path.join(RESOURCES, "definitions/raw.yaml")),
    env_identifier=ENVIRONMENT,
    dynamic_vars=environment,
)
processed_config = IOConfig(
    path_to_source_yaml=(os.path.join(RESOURCES, "definitions/processed.yaml")),
    env_identifier=ENVIRONMENT,
    dynamic_vars=environment,
)
