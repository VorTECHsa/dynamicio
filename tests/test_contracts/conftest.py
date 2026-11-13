"""Shared builders for the behaviour-contract tests.

Every contract is exercised through the one way to declare a schema: a Pandera ``DataFrameModel`` (``ContractModel``), so that a
change of the schema API is held to the same assertions.
"""

# pylint: disable=missing-class-docstring, missing-function-docstring
import logging
import textwrap

import pandas as pd
import pandera.pandas as pa
import pytest
from pandera.typing import Series

from dynamicio import UnifiedIO
from dynamicio.config import IOConfig
from dynamicio.schema import Metric, metrics, null_fraction_below


class ContractModel(pa.DataFrameModel):
    """The SCHEMA_SPEC schema, declared as a Pandera model (``apply: false`` validations are simply omitted)."""

    __dataset_name__ = "contract_dataset"

    a: Series[int] = pa.Field(
        nullable=True,
        unique=True,
        gt=1,
        in_range={"min_value": 0, "max_value": 5, "include_min": False, "include_max": False},
        metadata=metrics(Metric.MIN, Metric.COUNTS, Metric.UNIQUE_COUNTS),
    )
    b: Series[str] = pa.Field(nullable=False, isin=["x", "y"], metadata=metrics(Metric.COUNTS_PER_LABEL))
    c: Series[float] = pa.Field(
        nullable=False,
        metadata=metrics(Metric.MEAN, Metric.MAX, Metric.STD, Metric.VARIANCE) | null_fraction_below(0.3),
    )


class ContractPanderaIO(UnifiedIO):
    schema = ContractModel


@pytest.fixture
def make_io(tmp_path):
    """Returns ``make_io(df_to_read=None, io_class=None, **io_kwargs)`` for a LOCAL csv resource.

    The dataset name that metrics are logged under is the ``__dataset_name__`` of ``ContractModel``, upper-cased: ``dataset_name``.
    """
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
            """))

    class _NoVars:  # IOConfig only needs an object to look dynamic variables up on
        pass

    def _make(df_to_read: pd.DataFrame = None, io_class=None, **io_kwargs):
        if df_to_read is not None:
            df_to_read.to_csv(csv_file, index=False)
        resource = IOConfig(str(definitions), env_identifier="LOCAL", dynamic_vars=_NoVars).get(source_key="CONTRACT")
        io_class = io_class or ContractPanderaIO
        return io_class(source_config=resource, **io_kwargs)

    _make.csv_file = csv_file
    _make.dataset_name = "CONTRACT_DATASET"
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
