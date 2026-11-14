# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
"""The pipeline tests run LOCAL, so this builds the IO objects of the Postgres bindings in DEV and PROD too: their models must name the table."""
import os

import pytest

from demo.src.config import environment
from demo.src.resources.io import InputIO
from demo.src.resources.schemas.input import Bar
from dynamicio.config import IOConfig


@pytest.mark.unit
@pytest.mark.parametrize("environment_name", ["DEV", "PROD"])
@pytest.mark.parametrize("definitions, source_key, model", [("staging_input", "BAR", Bar)])
def test_the_model_of_a_postgres_binding_names_its_table(environment_name, definitions, source_key, model):
    config = IOConfig(os.path.join(environment.RESOURCES, f"definitions/{definitions}.yaml"), env_identifier=environment_name, dynamic_vars=environment)

    io = InputIO(source_config=config.get(source_key=source_key), schema=model)  # TableNameNotFoundError without `table=`

    assert io.schema.table_name == "bar"
