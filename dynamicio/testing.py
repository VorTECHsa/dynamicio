"""Helpers for testing a pipeline task end to end against the resource definitions it already uses.

A task writes its outputs through dynamicio. In a ``LOCAL`` environment those land on disk, so a test can run the task and then compare every output
with the reference file declared next to it, under the optional ``expected`` key of the resource definition::

    FINAL_BAR:
      LOCAL:
        type: "local"
        local:
          file_path: "[[ TEST_RESOURCES ]]/data/processed/final_bar.parquet"
          file_type: "parquet"
        expected:
          path: "[[ TEST_RESOURCES ]]/data/processed/expected/final_bar.parquet"
      PROD:
        type: "kafka"
        ...

``ExpectedOutputs`` collects *every* mismatch (rather than stopping at the first), raises one readable error at the end, removes the generated files of the
outputs that matched, and leaves the ones that did not for inspection::

    with ExpectedOutputs() as outputs:
        staging.main()
        outputs.check(raw_config, "STAGED_FOO")
        outputs.check(processed_config, "FINAL_BAR", list_columns_to_sort=["tags"])

Set ``DYNAMICIO_UPDATE_EXPECTED=1`` to write the generated outputs over the expected files instead of comparing them (review the diff before committing).
"""

__all__ = ["ExpectedOutputs", "OutputMismatchError"]

import json
import os
import shutil
from typing import Any, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from dynamicio.config import IOConfig
from dynamicio.config.pydantic.io_resources import FileType

UPDATE_EXPECTED_ENV_VAR = "DYNAMICIO_UPDATE_EXPECTED"


class OutputMismatchError(AssertionError):
    """Raised once, after all checks ran, listing every output that did not match its expected counterpart.

    Attributes:
        errors: ``(name, message)`` for each failed check, in the order the checks ran.
    """

    MAX_MESSAGE_CHARS = 1000

    def __init__(self, errors: Sequence[Tuple[str, str]]):
        self.errors: List[Tuple[str, str]] = list(errors)
        super().__init__(self._format(self.errors))

    @classmethod
    def _format(cls, errors: Sequence[Tuple[str, str]]) -> str:
        lines = [f"The following AssertionErrors occurred ({len(errors)}):"]
        for name, message in errors:
            shortened = message if len(message) <= cls.MAX_MESSAGE_CHARS else f"{message[:cls.MAX_MESSAGE_CHARS]}... (truncated)"
            lines.append(f"\n[{name}]\n{shortened}")
        return "\n".join(lines)


class ExpectedOutputs:
    """Lazily compares the outputs a task generated locally with the ``expected`` files declared in their resource definitions.

    Use it as a context manager to raise the collected errors on exit, or call :meth:`raise_if_failed` yourself.

    Args:
        keep_files: Keep the generated files even when they matched (by default they are removed, so the next run starts clean).
        update_expected: Overwrite the expected files with the generated outputs instead of comparing. Defaults to the ``DYNAMICIO_UPDATE_EXPECTED`` env var.
    """

    def __init__(self, keep_files: bool = False, update_expected: Optional[bool] = None):
        self.keep_files = keep_files
        self.update_expected = os.environ.get(UPDATE_EXPECTED_ENV_VAR, "").lower() in ("1", "true", "yes") if update_expected is None else update_expected
        self.errors: List[Tuple[str, str]] = []
        self.passed: List[str] = []
        self.updated: List[str] = []

    def __enter__(self) -> "ExpectedOutputs":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if exc_type is None:  # never mask an error raised by the task itself
            self.raise_if_failed()

    # ---- locating the files -------------------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def _format_path(path: str, params: Optional[Mapping[str, Any]]) -> str:
        """Fill ``{placeholders}`` in a path from ``params`` (for outputs whose file name carries a runtime parameter, e.g. a date)."""
        return path.format(**params) if params else path

    @classmethod
    def generated_path(cls, config: IOConfig, source_key: str, params: Optional[Mapping[str, Any]] = None) -> str:
        """The local path the task writes ``source_key`` to in ``config``'s environment."""
        local = getattr(config.get(source_key), "local", None)
        file_path = getattr(local, "file_path", None)
        if file_path is None:
            raise ValueError(f"{source_key} is not a single-file local resource in environment {config.env_identifier!r}; only those can be checked")
        return cls._format_path(file_path, params)

    @classmethod
    def expected_path(cls, config: IOConfig, source_key: str, params: Optional[Mapping[str, Any]] = None) -> str:
        """The path of the reference file declared under ``expected`` for ``source_key``."""
        expected = config.get(source_key).expected
        if expected is None:
            raise ValueError(f"{source_key} declares no `expected` path in environment {config.env_identifier!r}")
        return cls._format_path(expected.path, params)

    @classmethod
    def _file_type(cls, config: IOConfig, source_key: str, path: str, use_expected: bool) -> str:
        environment = config.get(source_key)
        declared = environment.expected.file_type if use_expected and environment.expected else None
        declared = declared or getattr(getattr(environment, "local", None), "file_type", None)
        if declared is not None:
            return FileType(declared).value
        extension = os.path.splitext(path)[1].lstrip(".").lower()
        return {"h5": "hdf"}.get(extension, extension)

    # ---- reading ------------------------------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def read_file(path: str, file_type: str) -> pd.DataFrame:
        """Read a generated or expected file with plain pandas, so no schema or cast hides a difference."""
        readers = {"parquet": pd.read_parquet, "csv": pd.read_csv, "json": pd.read_json, "hdf": pd.read_hdf}
        if file_type not in readers:
            raise ValueError(f"Unsupported file type {file_type!r} for {path}")
        return readers[file_type](path)

    @classmethod
    def read_generated(cls, config: IOConfig, source_key: str, params: Optional[Mapping[str, Any]] = None) -> pd.DataFrame:
        """The output the task generated for ``source_key``."""
        path = cls.generated_path(config, source_key, params)
        return cls.read_file(path, cls._file_type(config, source_key, path, use_expected=False))

    @classmethod
    def read_expected(cls, config: IOConfig, source_key: str, params: Optional[Mapping[str, Any]] = None) -> pd.DataFrame:
        """The reference output declared for ``source_key``."""
        path = cls.expected_path(config, source_key, params)
        return cls.read_file(path, cls._file_type(config, source_key, path, use_expected=True))

    @classmethod
    def clear(cls, config: IOConfig, *source_keys: str, params: Optional[Mapping[str, Any]] = None) -> None:
        """Remove leftovers of earlier runs (generated files of ``source_keys``) so a stale file can not pass for a fresh output."""
        for source_key in source_keys:
            path = cls.generated_path(config, source_key, params)
            if os.path.exists(path):
                os.remove(path)

    # ---- comparing ----------------------------------------------------------------------------------------------------------------------------------------

    @staticmethod
    def _sort_list_columns(df: pd.DataFrame, columns: Sequence[str]) -> pd.DataFrame:
        df = df.copy()
        for column in columns:
            df[column] = df[column].apply(lambda value: sorted(value) if isinstance(value, (list, tuple, set)) or hasattr(value, "tolist") else value)
        return df

    @staticmethod
    def _stringify_unhashable(df: pd.DataFrame) -> pd.DataFrame:
        """Dict (and list) cells can not be hashed by pandas' comparison; compare their canonical JSON text instead."""
        df = df.copy()
        for column in df.columns:
            if df[column].map(lambda value: isinstance(value, (dict, list, set))).any():
                df[column] = df[column].map(lambda value: json.dumps(value, sort_keys=True, default=str) if isinstance(value, (dict, list, set)) else value)
        return df

    def compare(
        self,
        actual: pd.DataFrame,
        expected: pd.DataFrame,
        name: str,
        *,
        list_columns_to_sort: Sequence[str] = (),
        check_like: bool = True,
        **assert_options: Any,
    ) -> bool:
        """Compare two frames, recording the failure under ``name`` instead of raising. Returns whether they matched.

        Args:
            actual: The newly generated frame.
            expected: The reference frame.
            name: What to call this check in the error report.
            list_columns_to_sort: List-valued columns whose element order is irrelevant.
            check_like: Ignore the order of rows (by index label) and columns, as ``pd.testing.assert_frame_equal(check_like=True)`` does.
            **assert_options: Passed to ``pd.testing.assert_frame_equal`` (``check_dtype``, ``rtol`` ...).
        """
        if list_columns_to_sort:
            actual, expected = self._sort_list_columns(actual, list_columns_to_sort), self._sort_list_columns(expected, list_columns_to_sort)
        try:
            try:
                pd.testing.assert_frame_equal(actual, expected, check_like=check_like, **assert_options)
            except TypeError as error:
                if "unhashable type" not in str(error):
                    raise
                pd.testing.assert_frame_equal(self._stringify_unhashable(actual), self._stringify_unhashable(expected), check_like=check_like, **assert_options)
        except AssertionError as error:
            self.errors.append((name, str(error)))
            return False
        return True

    def check(
        self,
        config: IOConfig,
        source_key: str,
        *,
        label: Optional[str] = None,
        params: Optional[Mapping[str, Any]] = None,
        list_columns_to_sort: Sequence[str] = (),
        check_like: bool = True,
        **assert_options: Any,
    ) -> bool:
        """Compare the generated output of ``source_key`` with its ``expected`` file and record any mismatch.

        On a match the generated file is removed (unless ``keep_files``); on a mismatch it is left in place to be inspected. A missing generated or expected
        file is recorded as a failure too, so one broken output never hides the others. With ``update_expected`` the generated file replaces the expected one.

        Args:
            config: The ``IOConfig`` (in its LOCAL-style environment) the task wrote the output with.
            source_key: The resource name.
            label: Name in the report (default ``source_key``, with ``params`` appended when given).
            params: Values for ``{placeholders}`` in the generated and expected paths.
            list_columns_to_sort: See :meth:`compare`.
            check_like: See :meth:`compare`.
            **assert_options: See :meth:`compare`.

        Returns:
            Whether the output matched (or was updated).
        """
        name = label or (f"{source_key}{dict(params)}" if params else source_key)
        try:
            generated = self.generated_path(config, source_key, params)
            expected = self.expected_path(config, source_key, params)
            if not os.path.exists(generated):
                raise FileNotFoundError(f"no generated output at {generated}; did the task run in environment {config.env_identifier!r}?")
            if self.update_expected:
                os.makedirs(os.path.dirname(os.path.abspath(expected)), exist_ok=True)
                shutil.copyfile(generated, expected)
                self.updated.append(name)
            else:
                if not os.path.exists(expected):
                    raise FileNotFoundError(f"no expected output at {expected}; generate it with {UPDATE_EXPECTED_ENV_VAR}=1")
                matched = self.compare(
                    self.read_file(generated, self._file_type(config, source_key, generated, use_expected=False)),
                    self.read_file(expected, self._file_type(config, source_key, expected, use_expected=True)),
                    name,
                    list_columns_to_sort=list_columns_to_sort,
                    check_like=check_like,
                    **assert_options,
                )
                if not matched:
                    return False
                self.passed.append(name)
        except (ValueError, OSError) as error:
            self.errors.append((name, f"{type(error).__name__}: {error}"))
            return False
        if not self.keep_files:
            os.remove(generated)
        return True

    def raise_if_failed(self) -> None:
        """Raise one :class:`OutputMismatchError` listing every recorded failure."""
        if self.errors:
            raise OutputMismatchError(self.errors)
