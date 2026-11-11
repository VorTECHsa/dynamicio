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
from demo.src.resources.schemas.input import Orders
from dynamicio.config import IOConfig


def _parquet(df: pd.DataFrame) -> io.BytesIO:
    buffer = io.BytesIO()
    df.to_parquet(buffer)
    buffer.seek(0)
    return buffer


class TestPrefixSchemaEvolution:
    @pytest.mark.unit
    def test_objects_written_before_a_column_was_added_are_read_with_nulls_for_it(self):
        # Given an S3 prefix holding an old object (no `status`) and a recent one
        old = pd.DataFrame({"order_id": [1, 2], "customer": ["a", "b"]})
        recent = pd.DataFrame({"order_id": [3], "customer": ["c"], "status": ["open"]})
        config = IOConfig(os.path.join(environment.RESOURCES, "definitions/staging_input.yaml"), env_identifier="DEV", dynamic_vars=environment).get(source_key="ORDERS")

        # When (`s3_read_down` is what fetches and parses every object under the prefix; here it serves the two above)
        with patch("dynamicio.mixins.with_s3.s3_read_down", side_effect=lambda _url, reader, **_: [reader(_parquet(old)), reader(_parquet(recent))]):
            df = InputIO(source_config=config, schema=Orders).read()

        # Then the column is kept, nulled for the old object, and the schema's column order is preserved
        assert df.columns.tolist() == ["order_id", "customer", "status"]
        assert df["status"].isna().tolist() == [True, True, False]

    @pytest.mark.unit
    def test_a_column_missing_from_every_object_is_an_error(self):
        # Given a prefix where no object has `status`
        old = pd.DataFrame({"order_id": [1, 2], "customer": ["a", "b"]})
        config = IOConfig(os.path.join(environment.RESOURCES, "definitions/staging_input.yaml"), env_identifier="DEV", dynamic_vars=environment).get(source_key="ORDERS")

        # When / Then
        with patch("dynamicio.mixins.with_s3.s3_read_down", side_effect=lambda _url, reader, **_: [reader(_parquet(old))]):
            with pytest.raises(ValueError, match="missing from every parquet object"):
                InputIO(source_config=config, schema=Orders).read()

    @pytest.mark.unit
    def test_the_sample_environment_reads_the_same_resource_from_local_files(self):
        # Given
        config = IOConfig(os.path.join(environment.RESOURCES, "definitions/staging_input.yaml"), env_identifier="LOCAL", dynamic_vars=environment).get(source_key="ORDERS")

        # When
        df = InputIO(source_config=config, schema=Orders).read()

        # Then
        assert df.columns.tolist() == ["order_id", "customer", "status"] and len(df) == 3
