# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import pyarrow as pyarrow_module
import pyarrow.parquet as pq
import pytest

# Application Imports
from dynamicio.engines import EngineSetting
from dynamicio.errors import S3KeyNotFoundError
from dynamicio.mixins import with_s3
from dynamicio.parquet_footer import S3Tail
from tests.integration.resources import Resources
from tests.test_parquet_footer import FooterIO, Footers, Rows

pytestmark = [pytest.mark.integration, pytest.mark.floci]


@pytest.fixture(autouse=True)
def default_engine():
    EngineSetting.reset()
    yield
    EngineSetting.reset()


class TestParquetFooterOnS3:
    @staticmethod
    def io(tmp_path, bucket, key, **options):
        return FooterIO(source_config=Resources.s3_file(tmp_path, bucket, key, "parquet"), schema=Rows, **options)

    def test_the_footer_of_an_object_is_the_footer_of_the_file(self, floci, prefix, tmp_path):
        # Given
        Footers.frame(1000).to_parquet(tmp_path / "data.parquet", row_group_size=300)
        floci.s3_client().upload_file(str(tmp_path / "data.parquet"), floci.bucket, f"{prefix}/data.parquet")

        # When
        footer = self.io(tmp_path, floci.bucket, f"{prefix}/data.parquet").footer()

        # Then
        assert footer.schema.equals(pq.read_schema(tmp_path / "data.parquet"), check_metadata=True)
        assert (footer.rows, footer.row_groups) == (1000, 4)

    def test_a_footer_larger_than_the_tail_takes_a_second_ranged_get(self, floci, prefix, tmp_path):
        # Given
        table = pyarrow_module.table({"a": [1, 2]}).replace_schema_metadata({b"big": b"x" * (2 * S3Tail.TAIL_BYTES)})
        pq.write_table(table, tmp_path / "data.parquet")
        floci.s3_client().upload_file(str(tmp_path / "data.parquet"), floci.bucket, f"{prefix}/data.parquet")
        calls = Footers.calls(with_s3._shared_s3_client())  # pylint: disable=protected-access

        # When
        footer = self.io(tmp_path, floci.bucket, f"{prefix}/data.parquet").footer()

        # Then
        assert footer.metadata[b"big"] == b"x" * (2 * S3Tail.TAIL_BYTES)
        assert len(calls) == 2 and all(call.startswith("bytes=") for call in calls)

    def test_a_missing_key_raises_s3_key_not_found_error_and_missing_ok_gives_none(self, floci, prefix, tmp_path):
        # When / Then
        with pytest.raises(S3KeyNotFoundError):
            self.io(tmp_path, floci.bucket, f"{prefix}/missing.parquet").footer()
        assert self.io(tmp_path, floci.bucket, f"{prefix}/missing.parquet", missing_ok=True).footer() is None

    def test_an_empty_object_raises_the_arrow_error(self, floci, prefix, tmp_path):
        # Given
        floci.s3_client().put_object(Bucket=floci.bucket, Key=f"{prefix}/data.parquet", Body=b"")

        # When / Then
        with pytest.raises(pyarrow_module.ArrowInvalid):
            self.io(tmp_path, floci.bucket, f"{prefix}/data.parquet").footer()
