from pathlib import Path

import pandas as pd
import pytest
from uhura.modes import fixture_builder_mode, task_test_mode

from dynamicio import (
    CsvResource,
    HdfResource,
    JsonResource,
    ParquetResource,
    S3CsvResource,
    S3HdfResource,
    S3JsonResource,
    S3ParquetResource,
)

# get test df

# get temp dir
# write to df to temp dir
# read from temp dir
# assert df is the same

# with fixture builder mode
# read from temp dir
# write to temp dir
# with test mode read from temp dir
# with test mode write to temp dir
# with test mode write to temp dir but wrong data


@pytest.fixture
def test_df():
    return pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]})


@pytest.fixture(
    params=[
        (CsvResource, S3CsvResource),
        (JsonResource, S3JsonResource),
        (ParquetResource, S3ParquetResource),
        (HdfResource, S3HdfResource),
    ]
)
def resources(request, tmpdir):
    file_resource = request.param[0](path=tmpdir / "actual" / "randomfilename.withextension")
    s3_resource = request.param[1](bucket="bucket", path="randomfilename.withextension")
    return file_resource, s3_resource


@pytest.fixture
def file_resource(resources):
    return resources[0]


@pytest.fixture
def s3_resource(resources):
    return resources[1]


def test_can_write_and_read(test_df, file_resource, s3_resource, tmpdir):
    file_resource.write(test_df)
    pd.testing.assert_frame_equal(file_resource.read(), test_df)

    with fixture_builder_mode(input_path=tmpdir / "uhura" / "input", known_good_path=tmpdir / "uhura" / "output"):
        file_resource.read()
        file_resource.write(test_df)

    with task_test_mode(input_path=tmpdir / "uhura" / "input", known_good_path=tmpdir / "uhura" / "output"):
        df = file_resource.read()
        file_resource.write(df)
        with pytest.raises(AssertionError):
            file_resource.write(df.drop("a", axis=1))

    # Write for s3 test
    file_resource.path = Path(tmpdir / "uhura" / "input" / "s3" / "bucket" / "randomfilename.withextension")
    file_resource.write(test_df)
    file_resource.path = Path(tmpdir / "uhura" / "output" / "s3" / "bucket" / "randomfilename.withextension")
    file_resource.write(test_df)

    with task_test_mode(input_path=tmpdir / "uhura" / "input", known_good_path=tmpdir / "uhura" / "output"):
        df = s3_resource.read()
        s3_resource.write(df)
        with pytest.raises(AssertionError):
            s3_resource.write(df.drop("a", axis=1))
