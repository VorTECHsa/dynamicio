"""The option maps of the polars engine (decision 6 of ADR 0010): the 11.x option names of a file resource, and the polars arguments that they become."""

import csv
from typing import Any, Dict, Mapping, Optional

from dynamicio.errors import EngineResourceError
from dynamicio.mixins.utils import ParquetIndex


class PolarsOptions:
    """The 11.x options that the polars engine accepts for each file type, and the polars argument of each read option.

    A resource definition has one set of options for its reads and its writes, so a file type accepts the options of its reader and of its writer. A read
    ignores the options of the writer, and a write ignores the options of the reader, as on the pandas engine. An option that neither has raises
    ``EngineResourceError`` when the user builds the resource.

    The CSV and JSON writers of the polars engine call the pandas writers of 11.1.0, so that the bytes are equal (rule 2 of ADR 0010). Their options are
    the options of these pandas writers. The parquet writer calls ``pyarrow.parquet.write_table``, as ``DataFrame.to_parquet`` does.
    """

    FILE_TYPES = ("parquet", "csv", "json")

    # The polars argument of each 11.x read option. None: the engine reads the option itself, or the option has no effect on polars.
    READ: Dict[str, Dict[str, Optional[str]]] = {
        "parquet": {"columns": None, "engine": None},
        "csv": {
            "sep": "separator",
            "delimiter": "separator",
            "header": "has_header",
            "nrows": "n_rows",
            "skiprows": "skip_rows",
            "na_values": "null_values",
            "quotechar": "quote_char",
            "comment": "comment_prefix",
            "encoding": "encoding",
            "names": "new_columns",
            "usecols": None,
            "decimal": None,
        },
        "json": {"orient": None, "lines": None, "convert_dates": None, "index_column": None},
    }

    WRITE: Dict[str, frozenset] = {
        "parquet": frozenset(
            {
                "engine",
                "index",
                "index_columns",
                "compression",
                "compression_level",
                "row_group_size",
                "version",
                "use_dictionary",
                "write_statistics",
                "data_page_size",
                "data_page_version",
                "coerce_timestamps",
                "use_deprecated_int96_timestamps",
                "allow_truncated_timestamps",
                "use_byte_stream_split",
                "column_encoding",
                "use_compliant_nested_type",
                "flavor",
                "write_page_index",
                "write_batch_size",
                "dictionary_pagesize_limit",
                "store_schema",
            }
        ),
        "csv": frozenset(
            {
                "sep",
                "na_rep",
                "float_format",
                "columns",
                "header",
                "index",
                "index_label",
                "mode",
                "encoding",
                "compression",
                "quoting",
                "quotechar",
                "lineterminator",
                "chunksize",
                "date_format",
                "doublequote",
                "escapechar",
                "decimal",
                "errors",
                "storage_options",
            }
        ),
        "json": frozenset(
            {"orient", "date_format", "double_precision", "force_ascii", "date_unit", "default_handler", "lines", "compression", "index", "indent", "mode", "index_column"}
        ),
    }

    # The options of an `s3_path_prefix` resource for each file type: `no_disk_space` of the reads, and the options of the partitioned parquet writes
    PREFIX = frozenset({"no_disk_space", "partition_cols", "mode", "allow_delete"})

    ENCODINGS = {"utf-8": "utf8", "utf8": "utf8", "UTF-8": "utf8"}
    # The `quoting` values that the polars reader reads as `pandas.read_csv` does. QUOTE_NONE reads quotes as text, so the read has no quote character
    CSV_QUOTING = (csv.QUOTE_MINIMAL, csv.QUOTE_ALL, csv.QUOTE_NONNUMERIC, csv.QUOTE_NONE)
    PANDAS_PARQUET_ENGINES = (None, "auto", "pyarrow")

    @classmethod
    def check(cls, file_type: str, source: str, options: Mapping[str, Any]) -> None:
        """Refuse the options of a file resource that the polars engine cannot apply, when the user builds the resource and at each read and write.

        Args:
            file_type: The file type of the resource.
            source: The backend of the resource, for the default JSON orient and the options of a prefix.
            options: The options of the resource, without the fields of its path template.

        Raises:
            EngineResourceError: The file type has no polars reader and writer, an option has no polars entry, or an option has a value that polars does not have.
        """
        if file_type not in cls.FILE_TYPES:
            raise EngineResourceError(f"the polars engine reads and writes {', '.join(cls.FILE_TYPES)} files, not {file_type} files. Use the pandas engine for {file_type}.")
        prefix = cls.PREFIX if source == "s3_path_prefix" else frozenset()
        unknown = sorted(name for name in options if name not in cls.READ[file_type] and name not in cls.WRITE[file_type] and name not in prefix)
        if unknown:
            raise EngineResourceError(f"the polars engine has no {file_type} option {', '.join(unknown)}. Remove the option, or use the pandas engine.")
        getattr(cls, f"check_{file_type}")(source, options)

    @classmethod
    def check_parquet(cls, _source: str, options: Mapping[str, Any]) -> None:
        """Refuse fastparquet, a written index and an ``index_columns`` value that is not valid: a polars frame has no index.

        ``index_columns`` names the columns that the file holds as its pandas index (``utils.ParquetIndex``), as a pandas frame with these columns as its
        index writes them.
        """
        if options.get("engine") not in cls.PANDAS_PARQUET_ENGINES:
            raise EngineResourceError(f"the polars engine writes parquet with pyarrow, not with {options['engine']}. Remove the engine option.")
        if options.get("index"):
            raise EngineResourceError("a polars frame has no index, so the polars engine cannot write one. Remove the index option, or set it to false, or use index_columns.")
        try:
            ParquetIndex.pop(dict(options))
        except ValueError as error:
            raise EngineResourceError(str(error)) from error

    @classmethod
    def check_csv(cls, _source: str, options: Mapping[str, Any]) -> None:
        """Refuse the CSV read values that have no polars equivalent."""
        header = options.get("header", 0)
        if not (header is None or isinstance(header, bool) or header == "infer" or (isinstance(header, int) and header == 0)):
            raise EngineResourceError(f"the polars engine reads a CSV header on the first line or no header, not header={options['header']!r}")
        if not isinstance(options.get("skiprows", 0), int):
            raise EngineResourceError(f"the polars engine skips a number of rows, not skiprows={options['skiprows']!r}")
        if isinstance(options.get("na_values"), Mapping):
            raise EngineResourceError("the polars engine reads null strings for all columns, not for each column. Set na_values to a string or a list of strings.")
        names = options.get("names")
        if names is not None and not (isinstance(names, (list, tuple)) and all(isinstance(name, str) for name in names)):
            raise EngineResourceError(f"the polars engine takes a list of column names, not names={names!r}")
        if options.get("decimal", ".") not in (".", ","):
            raise EngineResourceError(f"the polars engine reads a decimal point or a decimal comma, not decimal={options['decimal']!r}")
        if "encoding" in options and options["encoding"] not in cls.ENCODINGS:
            raise EngineResourceError(f"the polars engine reads UTF-8 CSV files, not encoding={options['encoding']!r}")
        # The pandas reader also applies these writer options. The polars reader has no escape character, and always reads a doubled quote as one quote
        if options.get("escapechar") is not None:
            raise EngineResourceError(f"the polars engine reads CSV files without an escape character, not escapechar={options['escapechar']!r}. Remove the escapechar option.")
        if options.get("doublequote", True) is not True:
            raise EngineResourceError("the polars engine reads a doubled quote character as one quote character, so it cannot read doublequote=False. Remove the doublequote option.")
        if options.get("quoting", csv.QUOTE_MINIMAL) not in cls.CSV_QUOTING:
            raise EngineResourceError(f"the polars engine reads CSV files with quoting QUOTE_MINIMAL, QUOTE_ALL, QUOTE_NONNUMERIC or QUOTE_NONE, not quoting={options['quoting']!r}")

    @classmethod
    def check_json(cls, source: str, options: Mapping[str, Any]) -> None:
        """Refuse a JSON orient other than records, index and columns, and orient index or columns without ``index_column`` (decision 9 of ADR 0010).

        The default orient is records on S3 and columns on a local file, as on pandas. The keys of orient index and columns are the index of a pandas
        frame. A polars frame has no index, so the ``index_column`` option names the column of the keys.
        """
        orient = cls.json_orient(source, options)
        index_column = options.get("index_column")
        if orient == "records" and index_column is None:
            return
        if orient not in ("index", "columns") or options.get("lines", False):
            if index_column is not None:
                raise EngineResourceError(f"the index_column option applies to orient index or columns without lines, not to orient {orient}. Remove the index_column option.")
            raise EngineResourceError(f"the polars engine reads and writes JSON with orient records, index or columns, not {orient}. Set the orient option to records.")
        if not isinstance(index_column, str):
            raise EngineResourceError(
                f"the polars engine reads and writes JSON with orient {orient} only with the index_column option: a polars frame has no index to give the keys. "
                "Set index_column to the column of the keys, or set the orient option to records."
            )

    @staticmethod
    def json_orient(source: str, options: Mapping[str, Any]) -> str:
        """The JSON orient of a resource: the ``orient`` option, or records on S3 and columns on a local file, as on pandas."""
        return options.get("orient", "records" if source == "s3_file" else "columns")

    @classmethod
    def read_arguments(cls, file_type: str, options: Mapping[str, Any]) -> Dict[str, Any]:
        """The polars arguments of the read options of a resource. The options of the writer and the options that the engine reads itself are left out."""
        arguments: Dict[str, Any] = {}
        for name, value in options.items():
            argument = cls.READ[file_type].get(name)
            if argument:
                arguments[argument] = value
        if "has_header" in arguments:
            # `header` is also a write option: `header: false` writes no header, so the read has none
            arguments["has_header"] = arguments["has_header"] is not None and arguments["has_header"] is not False
        if "encoding" in arguments:
            arguments["encoding"] = cls.ENCODINGS[arguments["encoding"]]
        if file_type == "csv" and options.get("decimal") == ",":
            arguments["decimal_comma"] = True
        if file_type == "csv" and options.get("quoting") == csv.QUOTE_NONE:
            # `quoting` is also a write option: QUOTE_NONE writes no quotes, and `pandas.read_csv` reads a quote character as text
            arguments["quote_char"] = None
        return arguments

    @classmethod
    def write_options(cls, file_type: str, options: Mapping[str, Any]) -> Dict[str, Any]:
        """The write options of a resource. The options of the reader are left out."""
        return {name: value for name, value in options.items() if name in cls.WRITE[file_type]}
