"""The polars pipeline reads the inputs of the pandas demo and must write the frames and the files of the pandas demo.

``tests/data`` is a copy of the inputs and of the expected outputs of ``demo/pandas``, so that each demo has its own data.
"""
# pylint: disable=missing-class-docstring, missing-function-docstring, unused-argument
import json
import os

import pandas as pd
import polars as pl
import pyarrow.parquet as pq
import pytest
from pandera.typing import Series

from demo.polars.src import staging_input_config, staging_output_config, transform_input_config, transform_output_config
from demo.polars.src.resources.io import PolarsIO
from demo.polars.src.runners import staging, transform
from dynamicio.engines import EngineSetting
from dynamicio.errors import ModelEngineMismatchError, SchemaValidationError
from dynamicio.schema import DataModel as PandasDataModel
from dynamicio.testharness import ExpectedOutputs


class PandasFoo(PandasDataModel, dataset="foo"):
    column_a: Series[str]


OUTPUTS = [(staging_output_config, "STAGED_FOO"), (staging_output_config, "STAGED_BAR"), (transform_output_config, "FINAL_FOO"), (transform_output_config, "FINAL_BAR")]


class Files:
    @staticmethod
    def paths(config, key):
        resource = config.get(source_key=key)
        return resource.local.file_path, resource.expected.path

    @staticmethod
    def pandas_metadata(path):
        return json.loads(pq.read_schema(path).metadata[b"pandas"])


class TestPipeline:
    @pytest.mark.end_to_end
    def test_the_engine_is_polars(self):
        assert EngineSetting.current().name == "polars"

    @pytest.mark.end_to_end
    def test_the_outputs_are_the_expected_outputs_of_the_pandas_demo(self, clean_outputs):
        with ExpectedOutputs() as outputs:
            staging.main()
            transform.main()

            outputs.check_all(staging_output_config)
            outputs.check_all(transform_output_config)

    @pytest.mark.end_to_end
    @pytest.mark.parametrize("config, key", OUTPUTS, ids=[key for _, key in OUTPUTS])
    def test_pandas_reads_the_frame_of_the_pandas_demo_from_each_file(self, clean_outputs, tmp_path, config, key):
        """The expected files are from an old pandas, so the pandas metadata is compared with a file that ``DataFrame.to_parquet`` writes now."""
        # Given
        staging.main()
        transform.main()
        generated, expected = Files.paths(config, key)

        # When
        frame = pd.read_parquet(generated)

        # Then
        pd.testing.assert_frame_equal(frame, pd.read_parquet(expected), check_exact=True)
        assert pq.read_schema(generated).equals(pq.read_schema(expected), check_metadata=False)
        pd.read_parquet(expected).to_parquet(tmp_path / "pandas.parquet")
        assert Files.pandas_metadata(generated) == Files.pandas_metadata(tmp_path / "pandas.parquet")

    @pytest.mark.end_to_end
    def test_the_transform_keeps_the_rows_of_each_staged_file(self, clean_outputs):
        # Given
        staging.main()

        # When
        transform.main()

        # Then
        for staged, final in (("STAGED_FOO", "FINAL_FOO"), ("STAGED_BAR", "FINAL_BAR")):
            rows = pl.read_parquet(transform_input_config.get(source_key=staged).local.file_path)
            assert pl.read_parquet(transform_output_config.get(source_key=final).local.file_path).equals(rows.cast({"column_c": pl.Float64, "column_d": pl.Float64}))


class TestValidations:
    @pytest.mark.end_to_end
    def test_a_failed_validation_in_the_sink_writes_no_final_file(self, clean_outputs):
        # Given a staged file with a column_c that is not greater than 1000
        staging.main()
        path = transform_input_config.get(source_key="STAGED_FOO").local.file_path
        pl.read_parquet(path).with_columns(pl.lit(999, dtype=pl.Int64).alias("column_c")).write_parquet(path)

        # When / Then
        with pytest.raises(SchemaValidationError):
            transform.main()
        assert not os.path.exists(transform_output_config.get(source_key="FINAL_FOO").local.file_path)

    @pytest.mark.end_to_end
    def test_a_failed_validation_in_the_read_stops_the_staging(self, clean_outputs, tmp_path):
        # Given a FOO file with a duplicate column_a
        foo = tmp_path / "foo.csv"
        pd.read_csv(staging_input_config.get(source_key="FOO").local.file_path, index_col=0).assign(column_a="id1").to_csv(foo)
        config = staging_input_config.get(source_key="FOO").model_copy(deep=True)
        config.local.file_path = str(foo)

        # When / Then
        with pytest.raises(SchemaValidationError):
            PolarsIO(source_config=config, schema=staging.Foo, apply_schema_validations=True).read()

    @pytest.mark.end_to_end
    def test_a_pandas_model_is_refused_on_the_polars_engine(self):
        with pytest.raises(ModelEngineMismatchError):
            PolarsIO(source_config=staging_input_config.get(source_key="FOO"), schema=PandasFoo)
