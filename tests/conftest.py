import shutil
from pathlib import Path
from typing import Generator

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
