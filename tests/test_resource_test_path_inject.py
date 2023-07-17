# import all resources
from typing import List

import pytest

from dynamicio import (
    CsvResource,
    HdfResource,
    JsonResource,
    KafkaResource,
    ParquetResource,
    PostgresResource,
    S3CsvResource,
    S3HdfResource,
    S3JsonResource,
    S3ParquetResource,
)


def get_every_resource_instance_with_injectable_test_path() -> List:
    resources = []
    for resource in [
        CsvResource,
        HdfResource,
        ParquetResource,
        JsonResource,
    ]:
        resources.append(resource(path="some_file.extension", test_path="{var1}"))
    for resource in [
        S3CsvResource,
        S3HdfResource,
        S3JsonResource,
        S3ParquetResource,
    ]:
        resources.append(resource(bucket="bucket", path="some_file.extension", test_path="{var1}"))

    resources.append(KafkaResource(topic="topic", server="server", test_path="{var1}"))
    resources.append(
        PostgresResource(db_user="user", db_host="host", db_name="name", table_name="table", test_path="{var1}")
    )
    return resources


@pytest.fixture(params=get_every_resource_instance_with_injectable_test_path())
def resource_instance(request):
    return request.param


def test_resource_test_path_inject(resource_instance):
    assert str(resource_instance.test_path) == "{var1}"
    resource_instance = resource_instance.inject(var1="aoeu")
    assert str(resource_instance.test_path) == "aoeu"
