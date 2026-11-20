# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from dynamicio.mixins import with_s3
from dynamicio.mixins.utils import JsonIndex
from dynamicio.mixins.with_local import WithLocal


class TestJsonIndexPop:
    @pytest.mark.unit
    @pytest.mark.parametrize("orient", ["index", "columns", None])
    def test_the_option_is_removed_and_returned_for_orient_index_and_columns(self, orient):
        # Given
        options = {"index_column": "key", "indent": 2}

        # When
        index_column = JsonIndex.pop(options, orient)

        # Then
        assert index_column == "key"
        assert options == {"indent": 2}

    @pytest.mark.unit
    def test_a_resource_without_the_option_gives_none(self):
        assert JsonIndex.pop({"orient": "records"}, "records") is None

    @pytest.mark.unit
    @pytest.mark.parametrize("orient, lines", [("records", False), ("split", False), ("index", True)])
    def test_another_orient_or_json_lines_is_refused(self, orient, lines):
        with pytest.raises(ValueError, match="applies to a JSON object with orient index or columns only"):
            JsonIndex.pop({"index_column": "key"}, orient, lines)

    @pytest.mark.unit
    def test_a_name_that_is_not_a_string_is_refused(self):
        with pytest.raises(ValueError, match="must be the name of a column"):
            JsonIndex.pop({"index_column": ["key"]}, "index")


class TestJsonIndexRecords:
    @pytest.mark.unit
    @pytest.mark.parametrize("orient", ["index", "columns"])
    @pytest.mark.parametrize("document", [b'[{"a": 1}]', b'{"r1": null}', b'{"r1": [1]}', b"1"])
    def test_a_json_that_is_not_an_object_of_objects_raises_a_value_error(self, orient, document):
        with pytest.raises(ValueError, match="must hold an object of objects"):
            JsonIndex.records(document, orient, "key")


class TestPandasJsonIndex:
    @staticmethod
    def schema(*columns: str) -> MagicMock:
        schema = MagicMock()
        schema.columns = dict.fromkeys(columns)
        schema.column_names = list(columns)
        return schema

    @staticmethod
    def frame() -> pd.DataFrame:
        return pd.DataFrame({"key": ["data", "other"], "release": ["current", "old"]})

    @pytest.mark.unit
    @pytest.mark.parametrize("orient", ["index", "columns"])
    def test_a_local_file_has_the_keys_of_the_column_and_reads_them_back_into_it(self, tmp_path, orient):
        # Given
        path = str(tmp_path / "data.json")

        # When
        WithLocal._write_json_file(self.frame(), path, orient=orient, index_column="key")  # pylint: disable=protected-access
        result = WithLocal._read_json_file(path, self.schema("key", "release"), orient=orient, index_column="key")  # pylint: disable=protected-access

        # Then
        assert (tmp_path / "data.json").read_text() == self.frame().set_index("key").to_json(orient=orient)
        pd.testing.assert_frame_equal(result, self.frame())

    @pytest.mark.unit
    def test_an_s3_object_has_the_keys_of_the_column(self):
        # When
        with patch.object(with_s3.wr.s3, "to_json") as to_json:
            with_s3.WithS3File._write_s3_json_file(self.frame(), "s3://b/k.json", orient="index", index_column="key")  # pylint: disable=protected-access

        # Then
        pd.testing.assert_frame_equal(to_json.call_args.kwargs["df"], self.frame().set_index("key"))
        assert to_json.call_args.kwargs["index"] is True
        assert "index_column" not in to_json.call_args.kwargs

    @pytest.mark.unit
    def test_an_s3_object_reads_its_keys_into_the_column(self):
        # Given
        raw = self.frame().set_index("key")
        raw.index.name = None

        # When
        with patch.object(with_s3.wr.s3, "read_json", return_value=raw) as read_json:
            result = with_s3.WithS3File._read_s3_json_file("s3://b/k.json", self.schema("key", "release"), orient="index", index_column="key")  # pylint: disable=protected-access

        # Then
        assert "index_column" not in read_json.call_args.kwargs
        pd.testing.assert_frame_equal(result, self.frame())

    @pytest.mark.unit
    def test_an_s3_object_with_the_default_orient_records_refuses_the_option(self):
        with pytest.raises(ValueError, match="not to orient 'records'"):
            with_s3.WithS3File._write_s3_json_file(self.frame(), "s3://b/k.json", index_column="key")  # pylint: disable=protected-access
