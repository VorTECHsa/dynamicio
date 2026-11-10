# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import os

import pandas as pd
import pytest

from dynamicio.testharness import OutputFiles
from tests.test_testharness.conftest import PARAMS


class TestOutputFiles:
    @pytest.mark.unit
    def test_expected_is_optional_and_per_environment(self, config, prod_config):
        assert config.get("OUT").expected.path.endswith("expected/out_{day}.parquet")
        assert config.get("NO_EXPECTED").expected is None
        assert prod_config.get("OUT").expected is None

    @pytest.mark.unit
    def test_paths_are_filled_from_params(self, config):
        files = OutputFiles(config, "OUT", PARAMS)

        assert files.generated_path.endswith("out_monday.parquet")
        assert files.expected_path.endswith("expected/out_monday.parquet")

    @pytest.mark.unit
    def test_a_resource_without_expected_cannot_be_checked(self, config):
        with pytest.raises(ValueError, match="declares no `expected` path"):
            _ = OutputFiles(config, "NO_EXPECTED").expected_path

    @pytest.mark.unit
    def test_only_single_file_local_resources_can_be_checked(self, prod_config):
        with pytest.raises(ValueError, match="not a single-file local resource"):
            _ = OutputFiles(prod_config, "OUT").generated_path

    @pytest.mark.unit
    def test_generated_and_expected_are_read_and_the_generated_can_be_removed_or_promoted(self, config):
        files = OutputFiles(config, "OUT", PARAMS)
        pd.DataFrame({"a": [1]}).to_parquet(files.generated_path)

        files.promote_generated()
        assert files.expected_exists()
        pd.testing.assert_frame_equal(files.read_generated(), files.read_expected())

        files.remove_generated()
        files.remove_generated()  # absent files are fine
        assert not os.path.exists(files.generated_path)

    @pytest.mark.unit
    def test_an_unsupported_file_type_is_rejected(self):
        with pytest.raises(ValueError, match="Unsupported file type"):
            OutputFiles.read_file("x.txt", "txt")
