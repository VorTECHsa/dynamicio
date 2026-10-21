"""Shared builders for the behaviour-contract tests.

Every contract is exercised through BOTH ways of declaring a schema that exist today (a YAML schema file and an in-code
``schema`` dict with ``schema_validations`` / ``schema_metrics``), so that a future schema API can be held to the same
assertions.
"""

# pylint: disable=missing-class-docstring, missing-function-docstring
import logging
import textwrap

import pandas as pd
import pytest
import yaml

from dynamicio import UnifiedIO
from dynamicio.config import IOConfig
from dynamicio.core import SCHEMA_FROM_FILE

# One schema exercising every validator and metric, with one non-applied validation.
SCHEMA_SPEC = {
    "name": "contract_dataset",
    "columns": {
        "a": {
            "type": "int64",
            "validations": {
                "has_unique_values": {"apply": True, "options": {}},
                "is_greater_than": {"apply": True, "options": {"threshold": 1}},
                "is_between": {"apply": True, "options": {"lower": 0, "upper": 5}},
            },
            "metrics": ["Min", "Counts", "UniqueCounts"],
        },
        "b": {
            "type": "object",
            "validations": {
                "has_no_null_values": {"apply": True, "options": {}},
                "is_in": {"apply": True, "options": {"categorical_values": ["x", "y"]}},
            },
            "metrics": ["CountsPerLabel"],
        },
        "c": {
            "type": "float64",
            "validations": {
                "has_no_null_values": {"apply": True, "options": {}},
                "has_acceptable_percentage_of_nulls": {"apply": True, "options": {"threshold": 0.3}},
                "is_lower_than_or_equal": {"apply": False, "options": {"threshold": 1}},
            },
            "metrics": ["Mean", "Max", "Std", "Variance"],
        },
    },
}


def _dict_schema_io():
    columns = SCHEMA_SPEC["columns"]

    class ContractDictIO(UnifiedIO):
        schema = {name: col["type"] for name, col in columns.items()}
        schema_validations = {name: col["validations"] for name, col in columns.items()}
        schema_metrics = {name: col["metrics"] for name, col in columns.items()}

    return ContractDictIO


class ContractYamlIO(UnifiedIO):
    schema = SCHEMA_FROM_FILE


@pytest.fixture(params=["yaml_schema", "dict_schema"])
def make_io(request, tmp_path):
    """Returns ``make_io(df_to_read=None, io_class=None, **io_kwargs)`` for a LOCAL csv resource, for both schema declaration styles.

    The dataset name that metrics are logged under is ``contract_dataset`` upper-cased for the YAML style (taken from the
    schema's ``name``) and the snake-cased class name for the dict style, so tests that check it use ``dataset_name``.
    """
    schema_file = tmp_path / "schema.yaml"
    schema_file.write_text(yaml.safe_dump(SCHEMA_SPEC, sort_keys=False))
    csv_file = tmp_path / "data.csv"

    definitions = tmp_path / "definitions.yaml"
    definitions.write_text(textwrap.dedent(f"""\
            ---
            CONTRACT:
              LOCAL:
                type: "local"
                local:
                  file_path: "{csv_file}"
                  file_type: "csv"
                  options:
                    index: false
              schema:
                file_path: "{schema_file}"
            """))

    class _NoVars:  # IOConfig only needs an object to look dynamic variables up on
        pass

    def _make(df_to_read: pd.DataFrame = None, io_class=None, **io_kwargs):
        if df_to_read is not None:
            df_to_read.to_csv(csv_file, index=False)
        resource = IOConfig(str(definitions), env_identifier="LOCAL", dynamic_vars=_NoVars).get(source_key="CONTRACT")
        io_class = io_class or (ContractYamlIO if request.param == "yaml_schema" else _dict_schema_io())
        return io_class(source_config=resource, **io_kwargs)

    _make.csv_file = csv_file
    _make.style = request.param
    _make.dataset_name = "CONTRACT_DATASET" if request.param == "yaml_schema" else "CONTRACT_DICT_IO"
    return _make


@pytest.fixture
def valid_df():
    return pd.DataFrame({"a": [2, 3, 4], "b": ["x", "y", "x"], "c": [0.5, 1.5, 2.5]})


@pytest.fixture
def invalid_df():
    """Violates: a unique/greater_than/between, b not-null/is_in, c not-null/null-percentage."""
    return pd.DataFrame({"a": [1, 1, 9], "b": ["x", "z", None], "c": [None, None, 2.5]})


@pytest.fixture
def metric_records(caplog):
    """Yields the messages logged by the metrics logger (what a log shipper sees before the JSON formatter wraps it)."""
    logging.getLogger("dynamicio.metrics").propagate = True
    caplog.set_level(logging.INFO)
    yield lambda: [r.getMessage() for r in caplog.records if r.getMessage().startswith('{"message": "METRIC"')]
    logging.getLogger("dynamicio.metrics").propagate = False
