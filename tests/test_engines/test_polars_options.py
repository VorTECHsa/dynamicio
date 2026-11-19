# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import csv

import pytest

from dynamicio.engines.polars_options import PolarsOptions
from dynamicio.errors import DynamicIOError, EngineResourceError


class TestFileTypes:
    @pytest.mark.unit
    @pytest.mark.parametrize("file_type", ["parquet", "csv"])
    def test_a_polars_file_type_without_options_is_accepted(self, file_type):
        PolarsOptions.check(file_type, "local", {})

    @pytest.mark.unit
    @pytest.mark.parametrize("file_type", ["hdf", "pickle", "xlsx"])
    def test_a_file_type_without_a_polars_reader_is_refused(self, file_type):
        with pytest.raises(EngineResourceError, match=f"not {file_type} files. Use the pandas engine for {file_type}"):
            PolarsOptions.check(file_type, "local", {})

    @pytest.mark.unit
    def test_the_error_is_a_dynamicio_error_and_a_value_error(self):
        with pytest.raises(DynamicIOError):
            PolarsOptions.check("hdf", "local", {})
        with pytest.raises(ValueError):
            PolarsOptions.check("hdf", "local", {})


class TestUnknownOptions:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "file_type, options",
        [
            ("parquet", {"filters": [("a", "=", 1)]}),
            ("csv", {"dtype": {"a": str}}),
            ("csv", {"parse_dates": ["a"]}),
            ("json", {"orient": "records", "typ": "series"}),
        ],
    )
    def test_an_option_without_a_polars_entry_is_refused(self, file_type, options):
        unknown = sorted(name for name in options if name != "orient")
        with pytest.raises(EngineResourceError, match=f"no {file_type} option {', '.join(unknown)}"):
            PolarsOptions.check(file_type, "local", options)

    @pytest.mark.unit
    def test_the_unknown_options_are_named_in_sorted_order(self):
        with pytest.raises(EngineResourceError, match="no csv option b_option, z_option"):
            PolarsOptions.check("csv", "local", {"z_option": 1, "b_option": 2, "sep": ","})

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "file_type, options",
        [
            ("parquet", {"columns": ["a"], "compression": "zstd", "row_group_size": 10, "engine": "pyarrow", "index": False}),
            ("csv", {"sep": ";", "na_values": ["-"], "nrows": 2, "index": False, "float_format": "%.2f", "encoding": "utf-8"}),
            ("json", {"orient": "records", "lines": True, "date_unit": "s", "indent": 2}),
        ],
    )
    def test_the_read_and_write_options_of_a_file_type_are_accepted(self, file_type, options):
        PolarsOptions.check(file_type, "local", options)


class TestParquetOptions:
    @pytest.mark.unit
    @pytest.mark.parametrize("engine", [None, "auto", "pyarrow"])
    def test_the_pyarrow_engines_are_accepted(self, engine):
        PolarsOptions.check("parquet", "s3_file", {"engine": engine})

    @pytest.mark.unit
    def test_fastparquet_is_refused(self):
        with pytest.raises(EngineResourceError, match="not with fastparquet"):
            PolarsOptions.check("parquet", "local", {"engine": "fastparquet"})

    @pytest.mark.unit
    def test_a_written_index_is_refused(self):
        with pytest.raises(EngineResourceError, match="has no index"):
            PolarsOptions.check("parquet", "local", {"index": True})


class TestCsvOptions:
    @pytest.mark.unit
    @pytest.mark.parametrize("header", [0, None, "infer", True, False])
    def test_a_header_on_the_first_line_or_no_header_is_accepted(self, header):
        PolarsOptions.check("csv", "local", {"header": header})

    @pytest.mark.unit
    @pytest.mark.parametrize("header", [1, [0, 1]])
    def test_a_header_on_another_line_is_refused(self, header):
        with pytest.raises(EngineResourceError, match="header on the first line or no header"):
            PolarsOptions.check("csv", "local", {"header": header})

    @pytest.mark.unit
    @pytest.mark.parametrize("skiprows", [[1, 3], (lambda row: row > 2)])
    def test_skiprows_that_is_not_a_number_is_refused(self, skiprows):
        with pytest.raises(EngineResourceError, match="skips a number of rows"):
            PolarsOptions.check("csv", "local", {"skiprows": skiprows})

    @pytest.mark.unit
    def test_an_encoding_other_than_utf8_is_refused(self):
        with pytest.raises(EngineResourceError, match="reads UTF-8 CSV files, not encoding='latin-1'"):
            PolarsOptions.check("csv", "local", {"encoding": "latin-1"})

    @pytest.mark.unit
    def test_an_escape_character_is_refused(self):
        with pytest.raises(EngineResourceError, match="without an escape character, not escapechar='\\\\\\\\'"):
            PolarsOptions.check("csv", "local", {"escapechar": "\\"})

    @pytest.mark.unit
    def test_doublequote_false_is_refused(self):
        with pytest.raises(EngineResourceError, match="cannot read doublequote=False"):
            PolarsOptions.check("csv", "local", {"doublequote": False})

    @pytest.mark.unit
    @pytest.mark.parametrize("options", [{"escapechar": None}, {"doublequote": True}])
    def test_the_default_escape_options_are_accepted(self, options):
        PolarsOptions.check("csv", "local", options)

    @pytest.mark.unit
    @pytest.mark.parametrize("quoting", [csv.QUOTE_MINIMAL, csv.QUOTE_ALL, csv.QUOTE_NONNUMERIC, csv.QUOTE_NONE])
    def test_the_quoting_values_that_polars_reads_as_pandas_are_accepted(self, quoting):
        PolarsOptions.check("csv", "local", {"quoting": quoting})

    @pytest.mark.unit
    @pytest.mark.parametrize("quoting", [4, 5, "all"])
    def test_another_quoting_value_is_refused(self, quoting):
        with pytest.raises(EngineResourceError, match=f"not quoting={quoting!r}"):
            PolarsOptions.check("csv", "local", {"quoting": quoting})


class TestJsonOptions:
    @pytest.mark.unit
    def test_the_default_orient_of_a_local_file_is_columns_and_is_refused(self):
        with pytest.raises(EngineResourceError, match="not columns. Set the orient option to records"):
            PolarsOptions.check("json", "local", {})

    @pytest.mark.unit
    def test_the_default_orient_of_an_s3_object_is_records_and_is_accepted(self):
        PolarsOptions.check("json", "s3_file", {})

    @pytest.mark.unit
    @pytest.mark.parametrize("orient", ["split", "index", "columns", "values", "table"])
    def test_an_orient_other_than_records_is_refused(self, orient):
        with pytest.raises(EngineResourceError, match=f"not {orient}"):
            PolarsOptions.check("json", "s3_file", {"orient": orient})


class TestReadArguments:
    @pytest.mark.unit
    def test_the_csv_read_options_become_polars_arguments(self):
        # Given
        options = {"sep": ";", "nrows": 5, "skiprows": 1, "na_values": ["-"], "quotechar": "'", "comment": "#", "encoding": "UTF-8"}

        # When
        arguments = PolarsOptions.read_arguments("csv", options)

        # Then
        assert arguments == {"separator": ";", "n_rows": 5, "skip_rows": 1, "null_values": ["-"], "quote_char": "'", "comment_prefix": "#", "encoding": "utf8"}

    @pytest.mark.unit
    def test_quote_none_reads_without_a_quote_character(self):
        assert PolarsOptions.read_arguments("csv", {"quoting": csv.QUOTE_NONE}) == {"quote_char": None}

    @pytest.mark.unit
    @pytest.mark.parametrize("quoting", [csv.QUOTE_MINIMAL, csv.QUOTE_ALL, csv.QUOTE_NONNUMERIC])
    def test_another_quoting_value_keeps_the_quote_character(self, quoting):
        assert not PolarsOptions.read_arguments("csv", {"quoting": quoting})

    @pytest.mark.unit
    def test_delimiter_is_the_separator(self):
        assert PolarsOptions.read_arguments("csv", {"delimiter": "|"}) == {"separator": "|"}

    @pytest.mark.unit
    @pytest.mark.parametrize("header, has_header", [(0, True), ("infer", True), (True, True), (None, False), (False, False)])
    def test_the_header_option_becomes_has_header(self, header, has_header):
        assert PolarsOptions.read_arguments("csv", {"header": header}) == {"has_header": has_header}

    @pytest.mark.unit
    def test_the_write_options_and_the_options_that_the_engine_reads_itself_are_left_out(self):
        assert not PolarsOptions.read_arguments("csv", {"index": False, "float_format": "%.2f", "usecols": ["a"]})
        assert not PolarsOptions.read_arguments("parquet", {"columns": ["a"], "engine": "pyarrow", "compression": "zstd"})
        assert not PolarsOptions.read_arguments("json", {"orient": "records", "lines": True})


class TestWriteOptions:
    @pytest.mark.unit
    def test_the_read_options_are_left_out_of_the_write_options(self):
        # Given
        options = {"columns": ["a"], "compression": "zstd", "row_group_size": 10}

        # When
        write_options = PolarsOptions.write_options("parquet", options)

        # Then
        assert write_options == {"compression": "zstd", "row_group_size": 10}

    @pytest.mark.unit
    def test_the_csv_write_options_keep_the_pandas_names(self):
        assert PolarsOptions.write_options("csv", {"sep": ";", "nrows": 3, "index": False}) == {"sep": ";", "index": False}
