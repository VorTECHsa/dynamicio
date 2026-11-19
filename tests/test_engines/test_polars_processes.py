# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import multiprocessing
import os
import subprocess
import sys
import textwrap
import threading
import time
import warnings
from unittest.mock import patch

import pytest

# Application Imports
from dynamicio.engines import EngineSetting, PandasEngine, PolarsEngine
from dynamicio.errors import EngineConfigurationError

pl = pytest.importorskip("polars")

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
FORK = pytest.mark.skipif(not hasattr(os, "fork"), reason="the platform has no fork")


class Children:
    """Run a function in a fork or a spawned process, and give its exit code."""

    TIMEOUT = 60

    @classmethod
    def fork(cls, function) -> int:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)  # Python 3.12 warns on a fork of a process with threads
            pid = os.fork()
        if pid == 0:  # pragma: no cover - the coverage of a fork is not recorded
            code = 1
            try:
                code = function()
            finally:
                os._exit(code)  # pylint: disable=protected-access
        deadline = time.monotonic() + cls.TIMEOUT
        while time.monotonic() < deadline:
            done, status = os.waitpid(pid, os.WNOHANG)
            if done:
                return os.waitstatus_to_exitcode(status)
            time.sleep(0.05)
        os.kill(pid, 9)
        os.waitpid(pid, 0)
        raise AssertionError("the fork did not end")

    @staticmethod
    def refuses_polars() -> int:
        """0 if the polars engine raises the fork error in this process."""
        try:
            EngineSetting.use()
        except EngineConfigurationError as error:
            return 0 if "fork of a process that imported polars" in str(error) else 2
        return 3

    @staticmethod
    def refuses_configure() -> int:
        try:
            EngineSetting.configure("polars")
        except EngineConfigurationError:
            return 0
        return 3

    @staticmethod
    def uses_pandas() -> int:
        return 0 if isinstance(EngineSetting.use(), PandasEngine) else 4

    @staticmethod
    def uses_polars() -> int:
        """In a spawned process: configure the polars engine and make a frame with polars."""
        EngineSetting.configure("polars")
        return 0 if isinstance(EngineSetting.use(), PolarsEngine) and pl.DataFrame({"a": [1, 2]})["a"].sum() == 3 else 5

    @staticmethod
    def fails() -> int:
        return 5

    @staticmethod
    def hangs() -> int:
        time.sleep(600)
        return 0

    @staticmethod
    def exit_with(function) -> None:
        """The target of a spawned process: ``multiprocessing`` ignores the return value of a target, so the code of ``function`` is the exit code."""
        sys.exit(function())

    @classmethod
    def spawn(cls, function) -> int:
        process = multiprocessing.get_context("spawn").Process(target=cls.exit_with, args=(function,))
        process.start()
        process.join(cls.TIMEOUT)
        if process.is_alive():
            process.terminate()
            process.join()
            raise AssertionError("the spawned process did not end")
        return process.exitcode


class TestFork:
    @FORK
    @pytest.mark.unit
    def test_a_fork_of_a_polars_process_refuses_the_polars_engine(self):
        # Given
        EngineSetting.reset("polars")
        EngineSetting.use()

        # When / Then
        assert Children.fork(Children.refuses_polars) == 0
        assert Children.fork(Children.refuses_configure) == 0

    @FORK
    @pytest.mark.unit
    def test_a_fork_of_a_configured_polars_process_before_its_first_read_refuses_the_polars_engine(self):
        # Given
        EngineSetting.reset("polars")

        # When / Then
        assert Children.fork(Children.refuses_polars) == 0

    @FORK
    @pytest.mark.unit
    def test_a_fork_of_a_pandas_process_that_imported_polars_refuses_the_polars_engine(self):
        # Given polars imported and used, and the pandas engine, which is not fixed yet
        assert pl.DataFrame({"a": [1]}).height == 1

        # When / Then
        assert Children.fork(Children.refuses_configure) == 0
        assert Children.fork(Children.uses_pandas) == 0

    @FORK
    @pytest.mark.unit
    def test_a_fork_of_a_pandas_process_uses_the_pandas_engine(self):
        # Given
        EngineSetting.use()

        # When / Then
        assert Children.fork(Children.uses_pandas) == 0

    @FORK
    @pytest.mark.unit
    def test_the_parent_keeps_the_polars_engine_after_a_fork(self):
        # Given
        EngineSetting.reset("polars")
        Children.fork(Children.refuses_polars)

        # When
        engine = EngineSetting.use()

        # Then
        assert isinstance(engine, PolarsEngine)

    @pytest.mark.unit
    def test_a_fork_without_polars_imported_can_use_the_polars_engine(self):
        # Given
        EngineSetting.reset("polars")
        modules = {name: module for name, module in sys.modules.items() if name != "polars"}

        # When
        with patch.dict(sys.modules, modules, clear=True):
            EngineSetting.after_fork_in_child()

        # Then
        assert isinstance(EngineSetting.use(), PolarsEngine)

    @pytest.mark.unit
    def test_a_fork_gets_a_new_lock_when_a_thread_of_the_parent_holds_the_old_one(self):
        # Given a lock that another thread holds
        old = EngineSetting._lock  # pylint: disable=protected-access
        held, release = threading.Event(), threading.Event()

        def hold():
            with old:
                held.set()
                release.wait(10)

        thread = threading.Thread(target=hold)
        thread.start()
        held.wait(10)

        # When
        try:
            EngineSetting.after_fork_in_child()
            engine = EngineSetting.use()
        finally:
            release.set()
            thread.join()

        # Then
        assert isinstance(engine, PandasEngine)
        assert EngineSetting._lock is not old  # pylint: disable=protected-access

    @pytest.mark.unit
    def test_reset_clears_the_mark_of_a_fork(self):
        # Given
        EngineSetting.reset("polars")
        EngineSetting.after_fork_in_child()

        # When
        EngineSetting.reset("polars")

        # Then
        assert isinstance(EngineSetting.use(), PolarsEngine)


class TestSpawn:
    @pytest.mark.unit
    def test_a_spawned_process_of_a_polars_process_uses_the_polars_engine(self):
        # Given
        EngineSetting.reset("polars")
        EngineSetting.use()

        # When / Then
        assert Children.spawn(Children.uses_polars) == 0

    @pytest.mark.unit
    def test_the_exit_code_of_a_spawned_process_is_the_code_of_its_function(self):
        assert Children.spawn(Children.fails) == 5

    @pytest.mark.unit
    def test_a_spawned_process_that_does_not_end_is_stopped(self):
        # Given
        with patch.object(Children, "TIMEOUT", 0.5):
            # When / Then
            with pytest.raises(AssertionError, match="did not end"):
                Children.spawn(Children.hangs)


class TestPolarsMaxThreads:
    SCRIPT = textwrap.dedent("""
        import os, sys, tempfile
        from pathlib import Path
        import dynamicio
        assert "polars" not in sys.modules, "dynamicio imported polars"
        os.environ["POLARS_MAX_THREADS"] = "3"
        dynamicio.configure(engine="polars")
        from tests.test_engines.test_polars_files import Files, FilesIO, Pair
        import polars as pl
        directory = Path(tempfile.mkdtemp())
        FilesIO(source_config=Files.config(directory, "parquet"), schema=Pair).write(pl.DataFrame({"a": [1, 2], "b": ["x", None]}))
        frame = FilesIO(source_config=Files.config(directory, "parquet"), schema=Pair).read()
        print("RESULT", pl.thread_pool_size(), os.environ["POLARS_MAX_THREADS"], frame.height)
        """)

    @pytest.mark.unit
    def test_polars_max_threads_set_after_the_import_of_dynamicio_sets_the_threads_of_polars(self):
        # Given
        env = {name: value for name, value in os.environ.items() if name != "POLARS_MAX_THREADS"}

        # When
        result = subprocess.run([sys.executable, "-c", self.SCRIPT], cwd=ROOT, env=env, capture_output=True, text=True, check=False, timeout=120)

        # Then
        assert result.returncode == 0, result.stderr
        assert [line for line in result.stdout.splitlines() if line.startswith("RESULT")] == ["RESULT 3 3 2"]
