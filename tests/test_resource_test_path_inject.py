# import all resources
import pytest

from dynamicio import LocalFileResource, S3Resource


@pytest.fixture(params=[LocalFileResource, S3Resource])
def resource_instance(request, file_name):
    return request.param(bucket="bucket", path="some_file.extension", test_path="{var1}")


def test_resource_test_path_inject(resource_instance):
    assert str(resource_instance.test_path) == "{var1}"
    resource_instance = resource_instance.inject(var1="aoeu")
    assert str(resource_instance.test_path) == "aoeu"
