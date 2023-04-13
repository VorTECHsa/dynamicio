# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, R0801
# flake8: noqa: I101
from typing import Type

import pandas as pd
import pytest
from pandera import Field, SchemaModel
from pandera.errors import SchemaError
from pandera.typing import Series

import dynamicio  # TODO: load this automatically on package installation - entrypoint?


@pytest.fixture
def sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "column_a": {
                0: "id1",
                1: "id2",
                2: "id3",
                3: "id4",
                4: "id5",
                5: "id6",
                6: "id7",
                7: "id8",
                8: "id9",
                9: "id10",
            },
            "column_b": {
                0: "Label_A",
                1: "Label_A",
                2: "Label_B",
                3: "Label_C",
                4: "Label_A",
                5: "Label_B",
                6: "Label_C",
                7: "Label_A",
                8: "Label_A",
                9: "Label_B",
            },
            "column_c": {
                0: 1001.0,
                1: 1002.0,
                2: 1003.0,
                3: 1004.0,
                4: 1005.0,
                5: 1006.0,
                6: 1007.0,
                7: 1008.0,
                8: 1009.0,
                9: 1010.0,
            },
        }
    )


null_threshold = 0.5


class BarSchema(SchemaModel):
    column_b: Series[str] = Field(nullable=True, has_all_values=["Label_A", "Label_B", "Label_C"])
    column_c: Series[float] = Field(nullable=True, has_acceptable_percentage_of_nulls=null_threshold)

    class Config:
        strict = "filter"
        coerce = True


@pytest.fixture(params=[True, False], ids=lambda v: f"is_na={v}")
def percentage_nulls_schema(request) -> Type[SchemaModel]:
    class PNullsSchema(SchemaModel):
        column_c: Series[float] = Field(
            nullable=True, has_acceptable_percentage_of_nulls=null_threshold, ignore_na=request.param
        )

        class Config:
            strict = "filter"
            coerce = True

    return PNullsSchema


def test_validators_pass(sample_df: pd.DataFrame):
    BarSchema.validate(sample_df)


def test_has_acceptable_percentage_of_nulls_full_null(sample_df: pd.DataFrame):
    sample_df.loc[0, "column_c"] = None
    BarSchema.validate(sample_df)

    with pytest.raises(SchemaError):
        sample_df["column_c"] = None
        BarSchema.validate(sample_df)


def test_has_acceptable_percentage_of_nulls_passes_if_just_under_thershold(sample_df: pd.DataFrame):
    threshold_n = int(len(sample_df) * null_threshold)

    for i in range(0, threshold_n - 1):
        sample_df.loc[i, "column_c"] = None
    BarSchema.validate(sample_df)


def test_has_acceptable_percentage_of_nulls_raises_if_threshold_not_met(sample_df: pd.DataFrame):
    threshold_n = int(len(sample_df) * null_threshold)

    for i in range(0, threshold_n):
        sample_df.loc[i, "column_c"] = None
    with pytest.raises(SchemaError):
        BarSchema.validate(sample_df)


def test_has_all_values_allows_none(sample_df: pd.DataFrame):
    sample_df.loc[0, "column_b"] = None
    BarSchema.validate(sample_df)


def test_has_all_values_allows_raises_on_value_outside_allowed(sample_df: pd.DataFrame):
    sample_df.loc[0, "column_b"] = "Label_D"
    with pytest.raises(SchemaError):
        BarSchema.validate(sample_df)


def test_has_all_values_allows_raises_on_not_all_values_present(sample_df: pd.DataFrame):
    sample_df["column_b"] = "Label_A"
    with pytest.raises(SchemaError):
        BarSchema.validate(sample_df)


class NaIgnoreFalseSchema(SchemaModel):
    column_b: Series[str] = Field(nullable=True, isin=["Label_A", "Label_B", "Label_C"], ignore_na=False)

    class Config:
        strict = "filter"
        coerce = True


def test_has_all_values_passes_with_na_ignore_false(sample_df: pd.DataFrame):
    NaIgnoreFalseSchema.validate(sample_df)


def test_has_all_values_raises_with_na_ignore_false_and_none_value(sample_df: pd.DataFrame):
    sample_df.loc[0, "column_b"] = None
    with pytest.raises(SchemaError):
        NaIgnoreFalseSchema.validate(sample_df)


def test_has_acceptable_percentage_of_nulls_passes_regardless_of_ignore_na(
    sample_df: pd.DataFrame, percentage_nulls_schema
):
    percentage_nulls_schema.validate(sample_df)


def test_has_acceptable_percentage_of_nulls_raises_with_na_ignore_false_and_threshold_not_met(
    sample_df: pd.DataFrame, percentage_nulls_schema
):
    threshold_n = int(len(sample_df) * null_threshold)

    for i in range(0, threshold_n):
        sample_df.loc[i, "column_c"] = None
    with pytest.raises(SchemaError):
        percentage_nulls_schema.validate(sample_df)
