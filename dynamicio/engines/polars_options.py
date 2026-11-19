"""The option maps of the polars engine (decision 6 of ADR 0010): the 11.x option names of a file resource, and the polars arguments that they become."""

from typing import Any, Dict, Mapping, Optional

from dynamicio.errors import EngineResourceError


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
            "usecols": None,
        },
        "json": {"orient": None, "lines": None, "convert_dates": None},
    }

    WRITE: Dict[str, frozenset] = {
        "parquet": frozenset(
            {
                "engine",
                "index",
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
        "json": frozenset({"orient", "date_format", "double_precision", "force_ascii", "date_unit", "default_handler", "lines", "compression", "index", "indent", "mode"}),
    }

    ENCODINGS = {"utf-8": "utf8", "utf8": "utf8", "UTF-8": "utf8"}
    PANDAS_PARQUET_ENGINES = (None, "auto", "pyarrow")

    @classmethod
    def check(cls, file_type: str, source: str, options: Mapping[str, Any]) -> None:
        """Refuse the options of a file resource that the polars engine cannot apply, when the user builds the resource and at each read and write.

        Args:
            file_type: The file type of the resource.
            source: The backend of the resource, for the default JSON orient.
            options: The options of the resource, without the fields of its path template.

        Raises:
            EngineResourceError: The file type has no polars reader and writer, an option has no polars entry, or an option has a value that polars does not have.
        """
        if file_type not in cls.FILE_TYPES:
            raise EngineResourceError(f"the polars engine reads and writes {', '.join(cls.FILE_TYPES)} files, not {file_type} files. Use the pandas engine for {file_type}.")
        unknown = sorted(name for name in options if name not in cls.READ[file_type] and name not in cls.WRITE[file_type])
        if unknown:
            raise EngineResourceError(f"the polars engine has no {file_type} option {', '.join(unknown)}. Remove the option, or use the pandas engine.")
        getattr(cls, f"check_{file_type}")(source, options)

    @classmethod
    def check_parquet(cls, _source: str, options: Mapping[str, Any]) -> None:
        """Refuse fastparquet and a written index: a polars frame has no index."""
        if options.get("engine") not in cls.PANDAS_PARQUET_ENGINES:
            raise EngineResourceError(f"the polars engine writes parquet with pyarrow, not with {options['engine']}. Remove the engine option.")
        if options.get("index"):
            raise EngineResourceError("a polars frame has no index, so the polars engine cannot write one. Remove the index option, or set it to false.")

    @classmethod
    def check_csv(cls, _source: str, options: Mapping[str, Any]) -> None:
        """Refuse the CSV read values that have no polars equivalent."""
        header = options.get("header", 0)
        if not (header is None or isinstance(header, bool) or header == "infer" or (isinstance(header, int) and header == 0)):
            raise EngineResourceError(f"the polars engine reads a CSV header on the first line or no header, not header={options['header']!r}")
        if not isinstance(options.get("skiprows", 0), int):
            raise EngineResourceError(f"the polars engine skips a number of rows, not skiprows={options['skiprows']!r}")
        if "encoding" in options and options["encoding"] not in cls.ENCODINGS:
            raise EngineResourceError(f"the polars engine reads UTF-8 CSV files, not encoding={options['encoding']!r}")

    @classmethod
    def check_json(cls, source: str, options: Mapping[str, Any]) -> None:
        """Refuse a JSON orient other than records (decision 9 of ADR 0010). The default orient is records on S3 and columns on a local file, as on pandas."""
        orient = options.get("orient", "records" if source == "s3_file" else "columns")
        if orient != "records":
            raise EngineResourceError(f"the polars engine reads and writes JSON with orient records, not {orient}. Set the orient option to records.")

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
        return arguments

    @classmethod
    def write_options(cls, file_type: str, options: Mapping[str, Any]) -> Dict[str, Any]:
        """The write options of a resource. The options of the reader are left out."""
        return {name: value for name, value in options.items() if name in cls.WRITE[file_type]}
