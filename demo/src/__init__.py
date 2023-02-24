"""Set config IOs."""
__all__ = ["staging_input_config", "staging_output_config", "transform_input_config", "transform_output_config"]

import logging
import os

from demo.src import environment
from demo.src.environment import ENVIRONMENT, RESOURCES
from dynamicio.config import IOConfig

logging.basicConfig(level=logging.INFO)
logging.getLogger("kafka").setLevel(logging.WARNING)


staging_input_config = IOConfig(
    path_to_source_yaml=(os.path.join(RESOURCES, "definitions/staging_input.yaml")),
    env_identifier=ENVIRONMENT,
    dynamic_vars=environment,
)
staging_output_config = IOConfig(
    path_to_source_yaml=(os.path.join(RESOURCES, "definitions/staging_output.yaml")),
    env_identifier=ENVIRONMENT,
    dynamic_vars=environment,
)
transform_input_config = IOConfig(
    path_to_source_yaml=(os.path.join(RESOURCES, "definitions/transform_input.yaml")),
    env_identifier=ENVIRONMENT,
    dynamic_vars=environment,
)
transform_output_config = IOConfig(
    path_to_source_yaml=(os.path.join(RESOURCES, "definitions/transform_output.yaml")),
    env_identifier=ENVIRONMENT,
    dynamic_vars=environment,
)
