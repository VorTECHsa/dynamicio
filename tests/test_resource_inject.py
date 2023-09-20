from pathlib import Path

from dynamicio import LocalFileResource


def test_file_resource_inject_success(injectable_string, passing_injections, test_df, tmpdir, file_name):
    file_resource = LocalFileResource(
        path=Path(tmpdir / injectable_string) / file_name,
    )
    file_resource = file_resource.inject(**passing_injections)
    file_resource.write(test_df)
    file_resource.read()
