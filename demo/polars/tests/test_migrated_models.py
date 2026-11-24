"""The polars models of the pipeline are the output of ``dynamicio migrate --engine polars`` on the pandas models of the demo."""

# pylint: disable=missing-class-docstring, missing-function-docstring
import os

import pytest

from dynamicio.schema.migrate.to_polars.migration import PolarsMigration

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
PANDAS_MODELS = os.path.join(ROOT, "demo", "pandas", "src", "resources", "schemas")
POLARS_MODELS = os.path.join(ROOT, "demo", "polars", "src", "resources", "schemas")


class TestMigratedModels:
    @pytest.mark.end_to_end
    def test_the_models_are_the_output_of_migrate(self, tmp_path):
        """A change of the pandas models, or of the migrate command, fails here: run the command again and commit its output."""
        # When
        result = PolarsMigration.migrate(PANDAS_MODELS, str(tmp_path / "schemas"), scan=os.path.join(ROOT, "demo", "pandas", "src", "resources"), root=ROOT)

        # Then
        modules = sorted(name for name in os.listdir(tmp_path / "schemas") if name.endswith(".py"))
        assert modules == sorted(name for name in os.listdir(POLARS_MODELS) if name.endswith(".py"))
        for name in modules:
            with open(tmp_path / "schemas" / name, encoding="utf-8") as generated, open(os.path.join(POLARS_MODELS, name), encoding="utf-8") as committed:
                assert generated.read() == committed.read(), name
        assert len(result["models"]) == 7 and result["todos"] == []

    @pytest.mark.end_to_end
    def test_the_report_lists_the_definitions_that_the_polars_pipeline_changed(self, tmp_path):
        # When
        result = PolarsMigration.migrate(PANDAS_MODELS, str(tmp_path / "schemas"), scan=os.path.join(ROOT, "demo", "pandas", "src", "resources"), root=ROOT)

        # Then
        assert [line.split(":")[1].strip() for line in result["refused"]] == ["FOO.DEV", "FOO.PROD", "FINAL_BAR.DEV", "FINAL_BAR.PROD"]
        assert result["csv_index"] == ["definitions/staging_input.yaml: FOO.LOCAL"]

    @pytest.mark.end_to_end
    def test_the_polars_definitions_have_no_refused_resource(self, tmp_path):
        # When
        result = PolarsMigration.migrate(PANDAS_MODELS, str(tmp_path / "schemas"), scan=os.path.join(ROOT, "demo", "polars", "src", "resources"), root=ROOT)

        # Then
        assert result["refused"] == []
