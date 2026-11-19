# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pandera.pandas as pa
import pyarrow as pyarrow_module
import pyarrow.parquet as pq
import pytest
import yaml
from pandera.typing import Series

# Application Imports
from dynamicio import UnifiedIO
from dynamicio.config import IOConfig
from dynamicio.core import SCHEMA_FROM_ARGUMENT
from dynamicio.engines import EngineSetting
from dynamicio.errors import EngineResourceError, S3KeyNotFoundError
from dynamicio.parquet_footer import ParquetFooter, S3Tail
from tests import constants


class Rows(pa.DataFrameModel):
    __dataset_name__ = "rows"

    a: Series[int]
    b: Series[str] = pa.Field(nullable=True)


class FooterIO(UnifiedIO):
    schema = SCHEMA_FROM_ARGUMENT


@pytest.fixture(autouse=True)
def default_engine():
    EngineSetting.reset()
    yield
    EngineSetting.reset()


class Footers:
    """The resources and the files of the footer tests."""

    @staticmethod
    def io(tmp_path: Path, source: str = "local", file_type: str = "parquet", options: Optional[Dict[str, Any]] = None, **section: Any) -> FooterIO:
        section = section or {"file_path": str(tmp_path / f"data.{file_type}")}
        environment: Dict[str, Any] = {"type": source, "s3" if source.startswith("s3") else "local": {**section, "file_type": file_type}}
        path = tmp_path / f"definitions_{uuid.uuid4().hex}.yaml"
        path.write_text(yaml.safe_dump({"X": {"LOCAL": environment}}))
        config = IOConfig(str(path), env_identifier="LOCAL", dynamic_vars=FooterIO).get(source_key="X")
        return FooterIO(source_config=config, schema=Rows, **(options or {}))

    @staticmethod
    def frame(rows: int = 5) -> pd.DataFrame:
        return pd.DataFrame({"a": range(rows), "b": [f"v{i}" for i in range(rows)]})

    @staticmethod
    def calls(client) -> List[Any]:
        """The ``Range`` of each ``GetObject`` of a client, and the names of its other S3 operations."""
        calls: List[Any] = []

        def record(params, model, **_):
            calls.append(params.get("Range") if model.name == "GetObject" else model.name)

        client.meta.events.register("before-parameter-build.s3", record)
        return calls


class TestLocalFooter:
    @pytest.mark.unit
    def test_the_footer_has_the_schema_the_metadata_and_the_rows_of_the_file(self, tmp_path):
        # Given
        Footers.frame().to_parquet(tmp_path / "data.parquet", row_group_size=2)

        # When
        footer = Footers.io(tmp_path).footer()

        # Then
        assert isinstance(footer, ParquetFooter)
        assert footer.schema.equals(pq.read_schema(tmp_path / "data.parquet"), check_metadata=True)
        assert (footer.rows, footer.row_groups) == (5, 3)
        assert b"pandas" in footer.metadata

    @pytest.mark.unit
    def test_the_footer_keeps_custom_metadata(self, tmp_path):
        # Given
        table = pyarrow_module.table({"a": [1]}).replace_schema_metadata({b"as_of": b"2026-11-19"})
        pq.write_table(table, tmp_path / "data.parquet")

        # When
        footer = Footers.io(tmp_path).footer()

        # Then
        assert footer.metadata == {b"as_of": b"2026-11-19"}

    @pytest.mark.unit
    def test_a_file_without_metadata_gives_an_empty_dict(self, tmp_path):
        # Given
        pq.write_table(pyarrow_module.table({"a": [1]}), tmp_path / "data.parquet")

        # When / Then
        assert Footers.io(tmp_path).footer().metadata == {}

    @pytest.mark.unit
    def test_the_file_path_template_is_resolved_with_the_options(self, tmp_path):
        # Given
        Footers.frame(3).to_parquet(tmp_path / "data_7.parquet")

        # When
        footer = Footers.io(tmp_path, options={"shard": 7}, file_path=str(tmp_path / "data_{shard}.parquet")).footer()

        # Then
        assert footer.rows == 3

    @pytest.mark.unit
    def test_a_missing_file_raises_file_not_found_error(self, tmp_path):
        # When / Then
        with pytest.raises(FileNotFoundError):
            Footers.io(tmp_path).footer()

    @pytest.mark.unit
    def test_a_missing_file_with_missing_ok_gives_none(self, tmp_path):
        # When / Then
        assert Footers.io(tmp_path, options={"missing_ok": True}).footer() is None

    @pytest.mark.unit
    def test_a_file_that_is_not_parquet_raises_the_arrow_error(self, tmp_path):
        # Given
        (tmp_path / "data.parquet").write_bytes(b"not parquet")

        # When / Then
        with pytest.raises(pyarrow_module.ArrowInvalid):
            Footers.io(tmp_path).footer()

    @pytest.mark.unit
    @pytest.mark.parametrize("source, file_type", [("local", "csv"), ("local", "json"), ("local_batch", "parquet"), ("s3_path_prefix", "parquet")])
    def test_a_resource_that_is_not_one_parquet_file_raises_engine_resource_error(self, tmp_path, source, file_type):
        # Given
        section = {"path_prefix": "data/"} if source == "s3_path_prefix" else {"path_prefix": f"{tmp_path}/"} if source == "local_batch" else {}
        if source.startswith("s3"):
            section["bucket"] = constants.MOCK_BUCKET
        io = Footers.io(tmp_path, source, file_type, **section)

        # When / Then
        with pytest.raises(EngineResourceError, match="footer\\(\\) reads the parquet file"):
            io.footer()

    @pytest.mark.unit
    def test_the_footer_does_not_fix_the_engine(self, tmp_path):
        # Given
        Footers.frame().to_parquet(tmp_path / "data.parquet")

        # When
        Footers.io(tmp_path).footer()

        # Then
        assert not EngineSetting._frozen  # pylint: disable=protected-access


class TestS3Footer:
    @pytest.mark.unit
    def test_the_footer_of_an_object_is_the_footer_of_the_file(self, moto_s3_bucket, tmp_path):
        # Given
        Footers.frame().to_parquet(tmp_path / "data.parquet", row_group_size=2)
        moto_s3_bucket.upload_file(str(tmp_path / "data.parquet"), constants.MOCK_BUCKET, "data.parquet")
        local = Footers.io(tmp_path).footer()

        # When
        footer = Footers.io(tmp_path, "s3_file", bucket=constants.MOCK_BUCKET, file_path="data.parquet").footer()

        # Then
        assert footer.schema.equals(local.schema, check_metadata=True)
        assert (footer.rows, footer.row_groups) == (local.rows, local.row_groups)

    @pytest.mark.unit
    def test_a_small_footer_takes_one_ranged_get_of_the_end(self, moto_s3_bucket, tmp_path):
        # Given
        Footers.frame(100_000).to_parquet(tmp_path / "data.parquet")
        moto_s3_bucket.upload_file(str(tmp_path / "data.parquet"), constants.MOCK_BUCKET, "data.parquet")
        calls = Footers.calls(moto_s3_bucket)

        # When
        Footers.io(tmp_path, "s3_file", bucket=constants.MOCK_BUCKET, file_path="data.parquet").footer()

        # Then
        assert calls == [f"bytes=-{S3Tail.TAIL_BYTES}"]

    @pytest.mark.unit
    def test_a_footer_larger_than_the_tail_takes_a_second_ranged_get(self, moto_s3_bucket, tmp_path):
        # Given
        table = pyarrow_module.table({"a": list(range(10))}).replace_schema_metadata({b"big": b"x" * (3 * S3Tail.TAIL_BYTES)})
        pq.write_table(table, tmp_path / "data.parquet")
        moto_s3_bucket.upload_file(str(tmp_path / "data.parquet"), constants.MOCK_BUCKET, "data.parquet")
        calls = Footers.calls(moto_s3_bucket)

        # When
        footer = Footers.io(tmp_path, "s3_file", bucket=constants.MOCK_BUCKET, file_path="data.parquet").footer()

        # Then
        assert footer.metadata[b"big"] == b"x" * (3 * S3Tail.TAIL_BYTES)
        assert len(calls) == 2 and calls[0] == f"bytes=-{S3Tail.TAIL_BYTES}" and calls[1].startswith("bytes=")

    @pytest.mark.unit
    def test_an_object_smaller_than_the_tail_is_read_in_one_get(self, moto_s3_bucket, tmp_path):
        # Given
        Footers.frame(1).to_parquet(tmp_path / "data.parquet")
        moto_s3_bucket.upload_file(str(tmp_path / "data.parquet"), constants.MOCK_BUCKET, "data.parquet")
        calls = Footers.calls(moto_s3_bucket)

        # When
        footer = Footers.io(tmp_path, "s3_file", bucket=constants.MOCK_BUCKET, file_path="data.parquet").footer()

        # Then
        assert footer.rows == 1
        assert len(calls) == 1

    @pytest.mark.unit
    def test_a_missing_key_raises_s3_key_not_found_error(self, moto_s3_bucket, tmp_path):  # pylint: disable=unused-argument
        # When / Then
        with pytest.raises(S3KeyNotFoundError):
            Footers.io(tmp_path, "s3_file", bucket=constants.MOCK_BUCKET, file_path="missing.parquet").footer()

    @pytest.mark.unit
    def test_a_missing_key_with_missing_ok_gives_none(self, moto_s3_bucket, tmp_path):  # pylint: disable=unused-argument
        # When / Then
        assert Footers.io(tmp_path, "s3_file", options={"missing_ok": True}, bucket=constants.MOCK_BUCKET, file_path="missing.parquet").footer() is None

    @pytest.mark.unit
    def test_a_missing_bucket_raises_the_client_error(self, moto_s3_bucket, tmp_path):  # pylint: disable=unused-argument
        # When / Then
        with pytest.raises(Exception, match="NoSuchBucket") as error:
            Footers.io(tmp_path, "s3_file", bucket="missing-bucket", file_path="data.parquet").footer()
        assert not isinstance(error.value, FileNotFoundError)

    @pytest.mark.unit
    def test_an_empty_object_raises_the_arrow_error(self, moto_s3_bucket, tmp_path):
        # Given
        moto_s3_bucket.put_object(Bucket=constants.MOCK_BUCKET, Key="data.parquet", Body=b"")

        # When / Then
        with pytest.raises(pyarrow_module.ArrowInvalid):
            Footers.io(tmp_path, "s3_file", bucket=constants.MOCK_BUCKET, file_path="data.parquet").footer()


class TestS3Tail:
    @pytest.mark.unit
    def test_reads_seeks_and_tells_as_a_file(self, moto_s3_bucket):
        # Given
        content = bytes(range(256)) * 1024
        moto_s3_bucket.put_object(Bucket=constants.MOCK_BUCKET, Key="blob", Body=content)
        tail = S3Tail(constants.MOCK_BUCKET, "blob")

        # When / Then
        assert tail.size == len(content) and tail.start == len(content) - S3Tail.TAIL_BYTES
        assert tail.seek(-4, 2) == len(content) - 4 and tail.read() == content[-4:]
        assert tail.read() == b""
        assert tail.seek(10) == 10 and tail.read(5) == content[10:15] and tail.tell() == 15
        assert tail.seek(5, 1) == 20 and tail.read(3) == content[20:23]
        assert tail.seek(len(content) + 10) == len(content) + 10 and tail.read(1) == b""
        assert tail.seek(-1) == 0 and tail.readable() and tail.seekable()
