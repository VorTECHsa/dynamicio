from pathlib import Path

import pytest

from dynamicio.inject import InjectionError


def test_s3_resource_inject_fail(resource_class, injectable_string, failing_injections, test_df):
    s3_resource = resource_class(bucket=injectable_string, path=injectable_string)
    file_resource = s3_resource.inject(**failing_injections)
    with pytest.raises(InjectionError):
        file_resource.read()
    with pytest.raises(InjectionError):
        file_resource.write(test_df)


def test_s3_resource_inject_success(
    file_name, read_func, resource_class, injectable_string, passing_injections, test_df, tmpdir
):
    resource = resource_class(bucket=str(tmpdir / injectable_string), path=Path(injectable_string) / "file.extension")
    resource = resource.inject(**passing_injections)
    Path(resource.full_path).parent.mkdir(parents=True, exist_ok=True)
    resource.write(test_df)
    resource.read()
