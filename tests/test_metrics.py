from unittest import mock
from unittest.mock import call

from pandera import Field, SchemaModel
from pandera.typing import Series

from dynamicio import ParquetResource
from dynamicio.metrics import Metric
from tests.constants import TEST_RESOURCES


class ParquetSampleSchema(SchemaModel):
    """Schema for sample parquet file."""

    id: Series[int]
    foo_name: Series[str] = Field(log_statistics={"metrics": [Metric.COUNTS_PER_LABEL]})
    bar: Series[int] = Field(
        log_statistics={
            "metrics": [
                Metric.MIN,
                Metric.MAX,
                Metric.MEAN,
                Metric.STD,
                Metric.VARIANCE,
                Metric.COUNTS,
                Metric.UNIQUE_COUNTS,
            ]
        }
    )


def test_metrics_logged_successfully():
    test_path = TEST_RESOURCES / "data/input/parquet_sample.parquet"

    resource = ParquetResource(path=test_path, pa_schema=ParquetSampleSchema)

    with mock.patch("dynamicio.metrics.log_metric") as log_metric:
        _ = resource.read()
        log_metric.assert_has_calls(
            [
                call(column="foo_name", metric=Metric.COUNTS_PER_LABEL, value=8),
                call(column="foo_name", metric=Metric.COUNTS_PER_LABEL, value=7),
                call(column="bar", metric=Metric.MIN, value=1),
                call(column="bar", metric=Metric.MAX, value=15),
                call(column="bar", metric=Metric.MEAN, value=8),
                call(column="bar", metric=Metric.STD, value=4.47213595499958),
                call(column="bar", metric=Metric.VARIANCE, value=20),
                call(column="bar", metric=Metric.COUNTS, value=15),
                call(column="bar", metric=Metric.UNIQUE_COUNTS, value=15),
            ]
        )
