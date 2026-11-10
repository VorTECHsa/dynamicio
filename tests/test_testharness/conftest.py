# pylint: disable=missing-module-docstring, missing-function-docstring, redefined-outer-name
from types import ModuleType

import pytest

from dynamicio.config import IOConfig

DEFINITION = """
---
OUT:
  LOCAL:
    type: "local"
    local:
      file_path: "[[ ROOT ]]/out_{day}.parquet"
      file_type: "parquet"
    expected:
      path: "[[ ROOT ]]/expected/out_{day}.parquet"
  PROD:
    type: "kafka"
    kafka:
      kafka_server: "broker"
      kafka_topic: "topic"
NO_EXPECTED:
  LOCAL:
    type: "local"
    local:
      file_path: "[[ ROOT ]]/no_expected.csv"
      file_type: "csv"
"""

PARAMS = {"day": "monday"}


@pytest.fixture
def config(tmp_path):
    (tmp_path / "expected").mkdir()
    (tmp_path / "definition.yaml").write_text(DEFINITION)
    module = ModuleType("vars")
    module.ROOT = str(tmp_path)
    return IOConfig(str(tmp_path / "definition.yaml"), env_identifier="LOCAL", dynamic_vars=module)


@pytest.fixture
def prod_config(config):
    return IOConfig(config.path_to_source_yaml, env_identifier="PROD", dynamic_vars=config.dynamic_vars)
