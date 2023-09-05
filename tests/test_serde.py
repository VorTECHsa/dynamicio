from unittest.mock import MagicMock

import pandas as pd
import pytest

from dynamicio.io.serde import CsvSerde, HdfSerde, JsonSerde, ParquetSerde


@pytest.fixture(params=[CsvSerde, JsonSerde, ParquetSerde, HdfSerde])
def serde_class(request):
    return request.param


@pytest.fixture
def serde_instance(serde_class):
    _serde_instance = serde_class()
    return _serde_instance


def test_serde_read_write(serde_instance, test_df, tmp_path):
    serde_instance.write_to_file(tmp_path / "file", test_df)
    read_write_df = serde_instance.read_from_file(tmp_path / "file")
    pd.testing.assert_frame_equal(read_write_df, test_df)


def test_serde_validation_callback_called(serde_class, tmp_path, test_df):
    validation_callback = MagicMock()
    validation_callback.return_value = test_df
    serde_instance = serde_class(validations=[validation_callback])
    serde_instance.write_to_file(tmp_path / "file", test_df)
    validation_callback.assert_not_called()
    serde_instance.read_from_file(tmp_path / "file")
    validation_callback.assert_called_once()
