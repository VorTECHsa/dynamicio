"""A session that lazily checks the outputs of a task against their expected files."""

import os
from typing import Any, List, Mapping, Optional, Tuple

import pandas as pd

from dynamicio.config import IOConfig
from dynamicio.testharness.expectations import Expectations
from dynamicio.testharness.frame_comparison import FrameComparison
from dynamicio.testharness.output_files import OutputFiles
from dynamicio.testharness.output_mismatch_error import OutputMismatchError


class ExpectedOutputs:
    """Lazily compares the outputs a task generated locally with the ``expected`` files declared in their resource definitions.

    Use it as a context manager to raise the collected errors on exit, or call :meth:`raise_if_failed` yourself.

    Args:
        expectations: The default rules of every check (see :class:`Expectations`); a single check can refine them.
        keep_files: Keep the generated files even when they matched (by default they are removed, so the next run starts clean).
        update_expected: Overwrite the expected files with the generated outputs instead of comparing. Defaults to the ``DYNAMICIO_UPDATE_EXPECTED`` env var.

    Attributes:
        errors: ``(name, message)`` of each failed check.
        passed: Names of the checks that matched.
        updated: Names of the checks whose expected file was rewritten.
    """

    UPDATE_ENV_VAR = "DYNAMICIO_UPDATE_EXPECTED"

    def __init__(self, expectations: Optional[Expectations] = None, keep_files: bool = False, update_expected: Optional[bool] = None):
        self.expectations = expectations or Expectations()
        self.keep_files = keep_files
        self.update_expected = os.environ.get(self.UPDATE_ENV_VAR, "").lower() in ("1", "true", "yes") if update_expected is None else update_expected
        self.errors: List[Tuple[str, str]] = []
        self.passed: List[str] = []
        self.updated: List[str] = []

    def __enter__(self) -> "ExpectedOutputs":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if exc_type is None:  # never mask an error raised by the task itself
            self.raise_if_failed()

    @staticmethod
    def read_generated(config: IOConfig, source_key: str, params: Optional[Mapping[str, Any]] = None) -> pd.DataFrame:
        """The output the task generated for ``source_key``."""
        return OutputFiles(config, source_key, params).read_generated()

    @staticmethod
    def read_expected(config: IOConfig, source_key: str, params: Optional[Mapping[str, Any]] = None) -> pd.DataFrame:
        """The reference output declared for ``source_key``."""
        return OutputFiles(config, source_key, params).read_expected()

    @staticmethod
    def clear(config: IOConfig, *source_keys: str, params: Optional[Mapping[str, Any]] = None) -> None:
        """Remove leftovers of earlier runs (generated files of ``source_keys``) so a stale file can not pass for a fresh output."""
        for source_key in source_keys:
            OutputFiles(config, source_key, params).remove_generated()

    def compare(self, actual: pd.DataFrame, expected: pd.DataFrame, name: str, expectations: Optional[Expectations] = None) -> bool:
        """Compare two frames, recording the failure under ``name`` instead of raising. Returns whether they matched."""
        try:
            FrameComparison(expectations or self.expectations).compare(actual, expected)
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
        expectations: Optional[Expectations] = None,
        **rules: Any,
    ) -> bool:
        """Compare the generated output of ``source_key`` with its ``expected`` file and record any mismatch.

        On a match the generated file is removed (unless ``keep_files``); on a mismatch it is left in place to be inspected. A missing generated or expected
        file is recorded as a failure too, so one broken output never hides the others. With ``update_expected`` the generated file replaces the expected one.

        Args:
            config: The ``IOConfig`` (in its LOCAL-style environment) the task wrote the output with.
            source_key: The resource name.
            label: Name in the report (default ``source_key``, with ``params`` appended when given).
            params: Values for ``{placeholders}`` in the generated and expected paths.
            expectations: Rules for this check instead of the session's.
            **rules: Individual :class:`Expectations` fields to change for this check only, e.g. ``ignore_columns=["updated_at"]``.

        Returns:
            Whether the output matched (or was updated).
        """
        name = label or (f"{source_key}{dict(params)}" if params else source_key)
        files = OutputFiles(config, source_key, params)
        try:
            if not files.generated_exists():
                raise FileNotFoundError(f"no generated output at {files.generated_path}; did the task run in environment {files.environment_name!r}?")
            if self.update_expected:
                files.promote_generated()
                self.updated.append(name)
            else:
                if not files.expected_exists():
                    raise FileNotFoundError(f"no expected output at {files.expected_path}; generate it with {self.UPDATE_ENV_VAR}=1")
                if not self.compare(files.read_generated(), files.read_expected(), name, (expectations or self.expectations).with_(**rules)):
                    return False
                self.passed.append(name)
        except (ValueError, OSError) as error:
            self.errors.append((name, f"{type(error).__name__}: {error}"))
            return False
        if not self.keep_files:
            files.remove_generated()
        return True

    def raise_if_failed(self) -> None:
        """Raise one :class:`OutputMismatchError` listing every recorded failure."""
        if self.errors:
            raise OutputMismatchError(self.errors)
