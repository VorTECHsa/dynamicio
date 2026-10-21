"""Spike checks: Pandera models compile to the existing schema, and YAML schemas migrate to equivalent models."""

# pylint: disable=missing-function-docstring, missing-class-docstring, exec-used, too-few-public-methods
import pandas as pd
import pandera.pandas as pa
import pytest
from pandera.typing import Series

from dynamicio.config.pydantic.table_schema import DataframeSchema
from dynamicio.migrate import render_model
from dynamicio.pandera_schema import to_dataframe_schema
from tests.test_contracts.conftest import SCHEMA_SPEC


def _options(validation):
    defaults = {"include_left": False, "include_right": False} if validation.name == "is_between" else {}
    return sorted({**defaults, **validation.options}.items(), key=str)


def _comparable(schema):
    return {
        name: (col.data_type, sorted((v.name, _options(v)) for v in col.validations if v.apply), [getattr(m, "value", m) for m in col.metrics]) for name, col in schema.columns.items()
    }


@pytest.mark.unit
def test_pandera_default_nullable_false_adds_a_not_null_validation_the_yaml_never_had():
    class Defaults(pa.DataFrameModel):
        a: Series[int]

    assert [v.name for v in to_dataframe_schema(Defaults).columns["a"].validations] == ["has_no_null_values"]


@pytest.mark.unit
def test_yaml_schema_migrates_to_a_model_that_compiles_back_to_the_same_schema():
    original = DataframeSchema(**SCHEMA_SPEC)
    namespace = {"pd": pd, "pa": pa, "Series": Series}

    exec(render_model(original), namespace)  # noqa: S102 - generated source under test
    model = namespace["ContractDataset"]

    assert _comparable(to_dataframe_schema(model)) == _comparable(original)
