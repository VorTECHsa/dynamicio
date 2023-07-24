import shutil
from pathlib import Path
from typing import Generator

import pandas as pd
import pytest

from tests.constants import TEST_RESOURCES


@pytest.fixture(scope="session")
def output_dir_path() -> Generator[Path, None, None]:
    output_dir_path = Path(TEST_RESOURCES / "data/temp/output")
    try:
        yield output_dir_path
    finally:
        if output_dir_path.exists():
            shutil.rmtree(output_dir_path)


@pytest.fixture
def test_df():
    return pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"], "c": [True, False, True]})


@pytest.fixture
def injectable_string():
    return "{var1}/[[var2]]"


@pytest.fixture
def failing_injections():
    return {"var1": "hello"}


@pytest.fixture
def passing_injections():
    return {"var1": "hello", "var2": "there"}
