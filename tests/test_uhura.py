import pandas as pd
import pytest
from uhura.modes import fixture_builder_mode, task_test_mode

from dynamicio import CsvResource, HdfResource, JsonResource, ParquetResource

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


@pytest.fixture(params=[CsvResource, JsonResource, ParquetResource, HdfResource])
def file_resource(request, tmpdir):
    return request.param(path=tmpdir / "actual" / "randomfilename.withextension")


def test_can_write_and_read(test_df, file_resource, tmpdir):
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
