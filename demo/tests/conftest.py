# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import pandas as pd
import pytest

from demo.tests import constants


@pytest.fixture(scope="class")
def expected_staged_foo_df():
    return pd.read_parquet(f"{constants.E2E_TEST_RESOURCES}/data/raw/expected/staged_foo.parquet")


@pytest.fixture(scope="class")
def expected_staged_bar_df():
    return pd.read_parquet(f"{constants.E2E_TEST_RESOURCES}/data/raw/expected/staged_bar.parquet")


@pytest.fixture(scope="class")
def expected_final_foo_df():
    return pd.read_parquet(f"{constants.E2E_TEST_RESOURCES}/data/processed/expected/final_foo.parquet")


@pytest.fixture(scope="class")
def expected_final_bar_df():
    return pd.read_parquet(f"{constants.E2E_TEST_RESOURCES}/data/processed/expected/final_bar.parquet")
