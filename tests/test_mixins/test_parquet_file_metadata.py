# pylint: disable=missing-module-docstring,missing-class-docstring,missing-function-docstring
import io
import os
from unittest.mock import patch

import pandas as pd
import pyarrow.parquet as pq
import pytest

import dynamicio.mixins.with_s3
from dynamicio.config import IOConfig
from tests import constants
from tests.mocking.io import WriteS3IO


class TestParquetFileMetadata:
    """`file_metadata` adds key-value pairs to the footer of a parquet file, everything else stays as `df.to_parquet` writes it."""

    DF = pd.DataFrame({"col_1": [3, 2, 1], "col_2": ["a", "b", "c"]}, index=pd.Index([10, 20, 30], name="key"))

    @staticmethod
    def _config(env):
        return IOConfig(path_to_source_yaml=os.path.join(constants.TEST_RESOURCES, "definitions/processed.yaml"), env_identifier=env, dynamic_vars=constants).get(
            source_key="WRITE_TO_S3_PARQUET"
        )

    def _write_cloud(self, **options):
        """Write through the public IO call of a CLOUD resource into an in-memory object store and return the bytes."""
        store = {}

        def upload(fobj, bucket, key, acl="bucket-owner-full-control"):  # pylint: disable=unused-argument
            store[(bucket, key)] = fobj.getvalue()

        with patch.object(dynamicio.mixins.with_s3, "_upload_from_memory", side_effect=upload):
            WriteS3IO(source_config=self._config("CLOUD"), **options).write(self.DF)
        body = next(iter(store.values()))
        return body

    @pytest.mark.unit
    def test_the_pairs_reach_the_footer_of_an_s3_file_written_through_the_public_call(self):
        # When
        body = self._write_cloud(file_metadata={"as_of": "2026-11-11T00:00:00+00:00", "run": 7})

        # Then
        metadata = pq.read_schema(io.BytesIO(body)).metadata
        assert metadata[b"as_of"] == b"2026-11-11T00:00:00+00:00" and metadata[b"run"] == b"7"

    @pytest.mark.unit
    def test_the_data_index_and_pandas_metadata_are_those_of_a_plain_write(self):
        # Given
        plain = io.BytesIO()
        self.DF.to_parquet(plain)

        # When
        body = self._write_cloud(file_metadata={"as_of": "x"})

        # Then
        assert pq.read_table(io.BytesIO(body)).equals(pq.read_table(plain))
        pd.testing.assert_frame_equal(pd.read_parquet(io.BytesIO(body)), self.DF)
        assert pq.read_schema(io.BytesIO(body)).pandas_metadata == pq.read_schema(plain).pandas_metadata

    @pytest.mark.unit
    def test_index_false_and_writer_options_are_honoured(self):
        # When
        body = self._write_cloud(file_metadata={"as_of": "x"}, index=False, compression="gzip")

        # Then
        assert pd.read_parquet(io.BytesIO(body)).columns.tolist() == ["col_1", "col_2"]
        assert pq.ParquetFile(io.BytesIO(body)).metadata.row_group(0).column(0).compression == "GZIP"

    @pytest.mark.unit
    def test_a_local_file_gets_the_pairs_too(self, tmp_path):
        # Given
        config = self._config("LOCAL")
        config.local.file_path = str(tmp_path / "out.parquet")

        # When
        WriteS3IO(source_config=config, file_metadata={"as_of": "x"}).write(self.DF)

        # Then
        assert pq.read_schema(tmp_path / "out.parquet").metadata[b"as_of"] == b"x"

    @pytest.mark.unit
    def test_without_file_metadata_the_footer_has_no_extra_keys(self):
        # When
        body = self._write_cloud()

        # Then
        assert set(pq.read_schema(io.BytesIO(body)).metadata) == {b"pandas"}

    @pytest.mark.unit
    def test_file_metadata_with_fastparquet_is_rejected(self, tmp_path):
        # Given
        config = self._config("LOCAL")
        config.local.file_path = str(tmp_path / "out.parquet")

        # When / Then
        with pytest.raises(ValueError, match="needs the pyarrow engine"):
            WriteS3IO(source_config=config, file_metadata={"as_of": "x"}, engine="fastparquet").write(self.DF)

    @pytest.mark.unit
    def test_file_metadata_with_awswrangler_only_options_is_rejected(self):
        # When / Then
        with pytest.raises(ValueError, match="awswrangler-only options"):
            WriteS3IO(source_config=self._config("CLOUD"), file_metadata={"as_of": "x"}, s3_additional_kwargs={"ACL": "private"}).write(self.DF)
