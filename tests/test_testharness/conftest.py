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
  VARIANT:
    type: "local"
    local:
      file_path: "[[ ROOT ]]/variant_{day}.parquet"
      file_type: "parquet"
    expected:
      path: "[[ ROOT ]]/expected/variant_{day}.parquet"
NO_EXPECTED:
  LOCAL:
    type: "local"
    local:
      file_path: "[[ ROOT ]]/no_expected.csv"
      file_type: "csv"
JSON_RECORDS:
  LOCAL:
    type: "local"
    local:
      file_path: "[[ ROOT ]]/records.json"
      file_type: "json"
    options:
      orient: "records"
    expected:
      path: "[[ ROOT ]]/expected/records.json"
JSON_LINES:
  LOCAL:
    type: "local"
    local:
      file_path: "[[ ROOT ]]/lines.json"
      file_type: "json"
    options:
      orient: "records"
      lines: true
    expected:
      path: "[[ ROOT ]]/expected/lines.json"
CSV_WITH_JSON_REFERENCE:
  LOCAL:
    type: "local"
    local:
      file_path: "[[ ROOT ]]/mixed.csv"
      file_type: "csv"
    expected:
      path: "[[ ROOT ]]/expected/mixed.json"
      file_type: "json"
JSON_INDEX:
  LOCAL:
    type: "local"
    local:
      file_path: "[[ ROOT ]]/index.json"
      file_type: "json"
    expected:
      path: "[[ ROOT ]]/expected/index.json"
      options:
        orient: "index"
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


TASK_DEFINITION = """
---
FIRST:
  LOCAL:
    type: "local"
    local: {file_path: "[[ ROOT ]]/first.parquet", file_type: "parquet"}
    expected: {path: "[[ ROOT ]]/expected/first.parquet"}
  DEV:
    type: "kafka"
    kafka: {kafka_server: "broker", kafka_topic: "topic"}
SECOND:
  LOCAL:
    type: "local"
    local: {file_path: "[[ ROOT ]]/second.parquet", file_type: "parquet"}
    expected: {path: "[[ ROOT ]]/expected/second.parquet"}
UNCHECKED:
  LOCAL:
    type: "local"
    local: {file_path: "[[ ROOT ]]/unchecked.parquet", file_type: "parquet"}
BY_DAY:
  LOCAL:
    type: "local"
    local: {file_path: "[[ ROOT ]]/by_{day}.parquet", file_type: "parquet"}
    expected: {path: "[[ ROOT ]]/expected/by_{day}.parquet"}
"""


@pytest.fixture
def task_config(tmp_path):
    """A config shaped like the output definitions of a task: some resources declare `expected`, one is not meant to be checked, one has a placeholder."""
    (tmp_path / "expected").mkdir()
    (tmp_path / "task.yaml").write_text(TASK_DEFINITION)
    module = ModuleType("vars")
    module.ROOT = str(tmp_path)
    return IOConfig(str(tmp_path / "task.yaml"), env_identifier="LOCAL", dynamic_vars=module)
