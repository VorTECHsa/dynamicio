"""A session that lazily checks the outputs of a task against their expected files."""

import itertools
import os
import sys
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from dynamicio.config import IOConfig
from dynamicio.testharness.environment_switch import EnvironmentSwitch
from dynamicio.testharness.expectations import Expectations
from dynamicio.testharness.frame_comparison import FrameComparison
from dynamicio.testharness.output_files import OutputFiles
from dynamicio.testharness.output_mismatch_error import OutputMismatchError
from dynamicio.testharness.parquet_schema_comparison import ParquetSchemaComparison
from dynamicio.testharness.session_report import SessionReport


class ExpectedOutputs:
    """Lazily compares the outputs a task generated locally with the ``expected`` files declared in their resource definitions.

    Use it as a context manager to raise the collected errors on exit, or call :meth:`raise_if_failed` yourself.

    Args:
        expectations: The default rules of every check (see :class:`Expectations`); a single check can refine them.
        environment: The environment of the resource definitions, e.g. ``"LOCAL_E2E"``. Inside the ``with`` block every ``IOConfig`` that defines it
            selects it, so the task under test runs in it too, and the checks compare its outputs; the configs get their own environment back on exit. It
            defaults to the first environment each resource definition lists (conventionally the local one), whatever the process runs in. A single check
            can override it.
        configs: Switch only these ``IOConfig`` objects instead of every one that exists.
        keep_files: Keep the generated files even when they matched (by default they are removed, so the next run starts clean).
        update_expected: Overwrite the expected files with the generated outputs instead of comparing. Defaults to the ``DYNAMICIO_UPDATE_EXPECTED`` env var.

    Attributes:
        errors: ``(name, message)`` of each failed check.
        passed: Names of the checks that matched.
        updated: Names of the checks whose expected file was rewritten.
        skipped: Resources :meth:`check_all` left out because their paths have ``{placeholders}`` that no ``params`` fill; check those with ``check_each``.
    """

    UPDATE_ENV_VAR = "DYNAMICIO_UPDATE_EXPECTED"

    def __init__(
        self,
        expectations: Optional[Expectations] = None,
        keep_files: bool = False,
        update_expected: Optional[bool] = None,
        environment: Optional[str] = None,
        configs: Optional[Sequence[IOConfig]] = None,
    ):
        self.expectations = expectations or Expectations()
        self.environment = environment
        self.configs = configs
        self._switches: List[EnvironmentSwitch] = []
        self.keep_files = keep_files
        self.update_expected = os.environ.get(self.UPDATE_ENV_VAR, "").lower() in ("1", "true", "yes") if update_expected is None else update_expected
        self.errors: List[Tuple[str, str]] = []
        self.passed: List[str] = []
        self.updated: List[str] = []
        self.skipped: List[str] = []
        self._described: Dict[str, str] = {}

    def __enter__(self) -> "ExpectedOutputs":
        switch = EnvironmentSwitch(self.environment, *self.configs) if self.configs is not None else EnvironmentSwitch.of_all(self.environment)
        switch.__enter__()
        self._switches.append(switch)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self._switches:
            self._switches.pop().__exit__(exc_type, exc_value, traceback)
        self._publish_report()
        if exc_type is None:  # never mask an error raised by the task itself
            self.raise_if_failed()

    @property
    def report(self) -> SessionReport:
        """What this session verified so far: the outputs that matched (with their size), failed, were updated or skipped."""
        matched = {name: self._described.get(name, "") for name in self.passed}
        return SessionReport(matched, self.errors, self.updated, self.skipped, os.environ.get("PYTEST_CURRENT_TEST", "").split(" ")[0])

    def _publish_report(self) -> None:
        """Print the report and record it for the pytest plugin, so a green run says which outputs it verified."""
        report = self.report
        if not report.is_empty:
            sys.stdout.write(report.text() + "\n")
            SessionReport.recorded.append(report)

    @staticmethod
    def read_generated(config: IOConfig, source_key: str, params: Optional[Mapping[str, Any]] = None, environment: Optional[str] = None) -> pd.DataFrame:
        """The output the task generated for ``source_key``."""
        return OutputFiles(config, source_key, params, environment).read_generated()

    @staticmethod
    def read_expected(config: IOConfig, source_key: str, params: Optional[Mapping[str, Any]] = None, environment: Optional[str] = None) -> pd.DataFrame:
        """The reference output declared for ``source_key``."""
        return OutputFiles(config, source_key, params, environment).read_expected()

    @staticmethod
    def clear(config: IOConfig, *source_keys: str, params: Optional[Mapping[str, Any]] = None, environment: Optional[str] = None) -> None:
        """Remove leftovers of earlier runs (generated files of ``source_keys``) so a stale file can not pass for a fresh output."""
        for source_key in source_keys:
            OutputFiles(config, source_key, params, environment).remove_generated()

    @staticmethod
    def clear_all(config: IOConfig, params: Optional[Mapping[str, Any]] = None, environment: Optional[str] = None) -> None:
        """:meth:`clear` every output of ``config`` that declares an ``expected`` path (those with unfilled ``{placeholders}`` are left alone)."""
        for source_key in OutputFiles.declaring_expected(config, environment):
            files = OutputFiles(config, source_key, params, environment)
            if not files.placeholders:
                files.remove_generated()

    @staticmethod
    def combinations(grid: Mapping[str, Iterable[Any]]) -> List[Mapping[str, Any]]:
        """Every combination of the ``grid`` values, as ``params`` dicts."""
        return [dict(zip(grid, values)) for values in itertools.product(*grid.values())]

    @classmethod
    def clear_each(cls, config: IOConfig, source_key: str, grid: Mapping[str, Iterable[Any]], environment: Optional[str] = None) -> None:
        """:meth:`clear` the generated file of ``source_key`` for every combination of the ``grid`` values."""
        for params in cls.combinations(grid):
            cls.clear(config, source_key, params=params, environment=environment)

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
        environment: Optional[str] = None,
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
            environment: The environment to check instead of the session's.
            expectations: Rules for this check instead of the session's.
            **rules: Individual :class:`Expectations` fields to change for this check only, e.g. ``ignore_columns=["updated_at"]``.

        Returns:
            Whether the output matched (or was updated).
        """
        name = label or (f"{source_key}{dict(params)}" if params else source_key)
        files = OutputFiles(config, source_key, params, environment or self.environment)
        try:
            if not files.generated_exists():
                raise FileNotFoundError(f"no generated output at {files.generated_path}; did the task run in environment {files.environment_name!r}?")
            if self.update_expected:
                files.promote_generated()
                self.updated.append(name)
            else:
                if not files.expected_exists():
                    raise FileNotFoundError(f"no expected output at {files.expected_path}; generate it with {self.UPDATE_ENV_VAR}=1")
                rules_of_check = (expectations or self.expectations).with_(**rules)
                generated, reference = files.read_generated(), files.read_expected()
                matched = self.compare(generated, reference, name, rules_of_check)
                if rules_of_check.check_parquet_schema and files.is_parquet:
                    matched = self._compare_parquet_schema(files, name) and matched
                if not matched:
                    return False
                self.passed.append(name)
                self._described[name] = f"{len(generated)} rows x {len(generated.columns)} columns, as {os.path.basename(files.expected_path)}"
        except (ValueError, OSError) as error:
            self.errors.append((name, f"{type(error).__name__}: {error}"))
            return False
        if not self.keep_files:
            files.remove_generated()
        return True

    def _compare_parquet_schema(self, files: OutputFiles, name: str) -> bool:
        try:
            ParquetSchemaComparison.compare(files.generated_path, files.expected_path)
        except AssertionError as error:
            self.errors.append((name, str(error)))
            return False
        return True

    def check_all(
        self,
        config: IOConfig,
        *,
        skip: Sequence[str] = (),
        rules_for: Optional[Mapping[str, Mapping[str, Any]]] = None,
        params: Optional[Mapping[str, Any]] = None,
        environment: Optional[str] = None,
        expectations: Optional[Expectations] = None,
        **rules: Any,
    ) -> bool:
        """Run :meth:`check` for every resource of ``config`` that declares an ``expected`` path, so a new output is checked without touching the test.

        Resources whose paths have ``{placeholders}`` that ``params`` does not fill are left out and listed in ``skipped``; check those with ``check_each``.

        Example: ``outputs.check_all(stage_output_config, rules_for={"STAGED_CARGOES": {"list_columns_to_sort": ["product_id"]}}, skip=["STAGED_RAW"])``.

        Args:
            config: The ``IOConfig`` the task wrote the outputs with.
            skip: Resources not to check.
            rules_for: For a resource, the :class:`Expectations` fields to change for its check only.
            params: Values for ``{placeholders}`` in the paths of every resource.
            environment: The environment to check instead of the session's.
            expectations: Rules for these checks instead of the session's.
            **rules: Individual :class:`Expectations` fields to change for every check.

        Returns:
            Whether every check matched.
        """
        rules_for = rules_for or {}
        unknown = sorted((set(skip) | set(rules_for)) - set(config.sources))
        if unknown:
            raise ValueError(f"{unknown} are not resources of the config; it has {sorted(config.sources)}")
        results = []
        for source_key in OutputFiles.declaring_expected(config, environment or self.environment):
            if source_key in skip:
                continue
            if OutputFiles(config, source_key, params, environment or self.environment).placeholders:
                self.skipped.append(source_key)
                continue
            results.append(self.check(config, source_key, params=params, environment=environment, expectations=expectations, **{**rules, **rules_for.get(source_key, {})}))
        return all(results)

    def check_each(
        self,
        config: IOConfig,
        source_key: str,
        grid: Mapping[str, Iterable[Any]],
        *,
        environment: Optional[str] = None,
        expectations: Optional[Expectations] = None,
        **rules: Any,
    ) -> bool:
        """Run :meth:`check` for every combination of the ``grid`` values, for a resource whose path has several ``{placeholders}``.

        Example: ``outputs.check_each(config, "PART", {"table": ["events", "cargoes"], "shard": range(4)})`` checks 8 files and reports each by its own params.

        Args:
            config: The ``IOConfig`` the task wrote the output with.
            source_key: The resource name.
            grid: For each placeholder, the values it takes.
            environment: The environment to check instead of the session's.
            expectations: Rules for these checks instead of the session's.
            **rules: Individual :class:`Expectations` fields to change for these checks only.

        Returns:
            Whether every combination matched.
        """
        results = [self.check(config, source_key, params=params, environment=environment, expectations=expectations, **rules) for params in self.combinations(grid)]
        return all(results)

    def raise_if_failed(self) -> None:
        """Raise one :class:`OutputMismatchError` listing every recorded failure."""
        if self.errors:
            raise OutputMismatchError(self.errors, self.passed)
