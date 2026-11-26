# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import importlib.util
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]


class WithoutPolars:
    """Runs a script in a new process in which ``import polars`` fails, as in an install of dynamicio without the ``polars`` extra."""

    BLOCK = textwrap.dedent("""
        import importlib.util
        import sys

        class Block:
            def find_spec(self, name, path=None, target=None):
                if name == "polars" or name.startswith("polars."):
                    raise ModuleNotFoundError(f"No module named {name!r}", name=name)
                return None

        sys.meta_path.insert(0, Block())
        find_spec = importlib.util.find_spec
        importlib.util.find_spec = lambda name, package=None: None if name.split(".")[0] == "polars" else find_spec(name, package)
        """)

    @classmethod
    def run(cls, script: str) -> subprocess.CompletedProcess:
        return subprocess.run([sys.executable, "-c", cls.BLOCK + textwrap.dedent(script)], capture_output=True, text=True, check=False, cwd=ROOT, timeout=120)


class TestInstallWithoutPolars:
    @pytest.mark.unit
    def test_the_modules_of_dynamicio_import_without_polars(self):
        # When
        result = WithoutPolars.run("""
            import dynamicio, dynamicio.cli, dynamicio.config, dynamicio.engines, dynamicio.schema, dynamicio.testharness
            import dynamicio.schema.migrate, dynamicio.testharness.pytest_plugin
            print("polars" in sys.modules)
            """)

        # Then
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip().splitlines()[-1] == "False"

    @pytest.mark.unit
    def test_the_pandas_engine_writes_and_reads_without_polars(self, tmp_path):
        # When
        result = WithoutPolars.run(f"""
            import pandas as pd
            from pathlib import Path
            from tests.test_engines.ios import EngineCheckIO

            io = EngineCheckIO.make(Path({str(tmp_path)!r}), apply_schema_validations=True, log_schema_metrics=True)
            io.write(pd.DataFrame({{"a": [1, 2]}}))
            print(io.read()["a"].tolist())
            """)

        # Then
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip().splitlines()[-1] == "[1, 2]"

    @pytest.mark.unit
    def test_the_polars_engine_without_polars_raises_an_import_error_and_keeps_pandas(self):
        # When
        result = WithoutPolars.run("""
            import dynamicio
            from dynamicio.engines import EngineSetting

            try:
                dynamicio.configure(engine="polars")
            except ImportError as error:
                print(EngineSetting.current().name, error)
            """)

        # Then
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip().splitlines()[-1] == 'pandas The polars engine needs polars 2.1 or later: pip install "dynamicio[polars]"'

    @pytest.mark.unit
    def test_the_polars_migration_without_polars_raises_the_install_hint(self, tmp_path):
        # When
        result = WithoutPolars.run(f"""
            from dynamicio.cli import run_migrate

            try:
                run_migrate(["--engine", "polars", {str(tmp_path)!r}, "-o", {str(tmp_path / "out")!r}])
            except ImportError as error:
                print(error)
            """)

        # Then
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip().splitlines()[-1] == 'The polars engine needs polars 2.1 or later: pip install "dynamicio[polars]"'

    @pytest.mark.unit
    def test_a_polars_model_module_without_polars_raises_an_import_error(self):
        # When
        result = WithoutPolars.run("""
            try:
                import dynamicio.schema.polars
            except ImportError as error:
                print("ImportError", error.name)
            """)

        # Then
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip().splitlines()[-1] == "ImportError polars"


class TestInstallWithPolars:
    @pytest.mark.unit
    @pytest.mark.skipif(importlib.util.find_spec("polars") is None, reason="needs the polars extra")
    def test_configure_does_not_import_polars(self):
        """polars reads POLARS_MAX_THREADS when it is first imported, so the check of the version must not import it."""
        # When
        result = subprocess.run(
            [sys.executable, "-c", "import sys, dynamicio; dynamicio.configure(engine='polars'); print('polars' in sys.modules)"],
            capture_output=True,
            text=True,
            check=False,
            cwd=ROOT,
            timeout=120,
        )

        # Then
        assert result.returncode == 0, result.stderr
        assert result.stdout.strip().splitlines()[-1] == "False"
