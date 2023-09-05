import pandas as pd
import pytest


@pytest.fixture
def test_df():
    return pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"], "c": [True, False, True]})


@pytest.fixture
def injectable_string():
    return "{var1}/{var2}"


@pytest.fixture
def failing_injections():
    return {"var1": Exception()}


@pytest.fixture
def passing_injections():
    return {"var1": "hello", "var2": "there"}


@pytest.fixture(
    params=[
        "sample.csv",
        "sample.parquet",
        "sample.json",
        "sample.h5",
    ]
)
def file_name(request):
    return request.param
