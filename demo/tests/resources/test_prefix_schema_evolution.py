"""Showcases reading an S3 path prefix whose older objects predate a column that was added to the schema later."""
# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, too-few-public-methods
# noqa
import io
import os
from unittest.mock import patch

import pandas as pd
import pytest

from demo.src.config import environment
from demo.src.resources.io import InputIO
from demo.src.resources.schemas import Events
from dynamicio.config import IOConfig


def _parquet(df: pd.DataFrame) -> io.BytesIO:
    buffer = io.BytesIO()
    df.to_parquet(buffer)
    buffer.seek(0)
    return buffer


class TestPrefixSchemaEvolution:
    @pytest.mark.unit
    def test_objects_written_before_a_column_was_added_are_read_with_nulls_for_it(self):
        # Given an S3 prefix holding an old object (no `model_action`) and a recent one
        old = pd.DataFrame({"event_id": [1, 2], "vessel": ["a", "b"]})
        recent = pd.DataFrame({"event_id": [3], "vessel": ["c"], "model_action": ["create"]})
        config = IOConfig(os.path.join(environment.RESOURCES, "definitions/input.yaml"), env_identifier="actual", dynamic_vars=environment).get(source_key="EVENTS")

        # When (`s3_read_down` is what fetches and parses every object under the prefix; here it serves the two above)
        with patch("dynamicio.mixins.with_s3.s3_read_down", side_effect=lambda _url, reader, **_: [reader(_parquet(old)), reader(_parquet(recent))]):
            df = InputIO(source_config=config, schema=Events).read()

        # Then the column is kept, nulled for the old object, and the schema's column order is preserved
        assert df.columns.tolist() == ["event_id", "vessel", "model_action"]
        assert df["model_action"].isna().tolist() == [True, True, False]

    @pytest.mark.unit
    def test_a_column_missing_from_every_object_is_an_error(self):
        # Given a prefix where no object has `model_action`
        old = pd.DataFrame({"event_id": [1, 2], "vessel": ["a", "b"]})
        config = IOConfig(os.path.join(environment.RESOURCES, "definitions/input.yaml"), env_identifier="actual", dynamic_vars=environment).get(source_key="EVENTS")

        # When / Then
        with patch("dynamicio.mixins.with_s3.s3_read_down", side_effect=lambda _url, reader, **_: [reader(_parquet(old))]):
            with pytest.raises(ValueError, match="missing from every parquet object"):
                InputIO(source_config=config, schema=Events).read()

    @pytest.mark.unit
    def test_the_sample_environment_reads_the_same_resource_from_local_files(self):
        # Given
        config = IOConfig(os.path.join(environment.RESOURCES, "definitions/input.yaml"), env_identifier="sample", dynamic_vars=environment).get(source_key="EVENTS")

        # When
        df = InputIO(source_config=config, schema=Events).read()

        # Then
        assert df.columns.tolist() == ["event_id", "vessel", "model_action"] and len(df) == 3
