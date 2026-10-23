"""Tests for ``dynamicio migrate``: YAML schemas to Pandera models, parity check, and the call-site prompt."""

# pylint: disable=missing-function-docstring, redefined-outer-name
import pandera.pandas as pa
import pytest
import yaml
from pandera.typing import Series

from dynamicio import cli
from dynamicio.config.pydantic.table_schema import DataframeSchema
from dynamicio.schema.migrate import compile_module, migrate, render_model, schemas_equivalent
from dynamicio.schema.models import to_dataframe_schema
from tests.test_contracts.conftest import SCHEMA_SPEC


@pytest.mark.unit
def test_pandera_default_nullable_false_adds_a_not_null_validation_the_yaml_never_had():
    class Defaults(pa.DataFrameModel):  # pylint: disable=missing-class-docstring
        a: Series[int]

    assert [v.name for v in to_dataframe_schema(Defaults).columns["a"].validations] == ["has_no_null_values"]


@pytest.mark.unit
def test_yaml_schema_migrates_to_a_model_that_compiles_back_to_the_same_schema():
    original = DataframeSchema(**SCHEMA_SPEC)
    model = compile_module("import pandas as pd\nimport pandera.pandas as pa\nfrom pandera.typing import Series\n\n\n" + render_model(original))["ContractDataset"]

    assert schemas_equivalent(to_dataframe_schema(model), original)


@pytest.mark.unit
def test_schemas_equivalent_detects_a_changed_validation():
    original = DataframeSchema(**SCHEMA_SPEC)
    changed = DataframeSchema(**{**SCHEMA_SPEC, "columns": {**SCHEMA_SPEC["columns"], "b": {"type": "object", "validations": {}, "metrics": []}}})

    assert not schemas_equivalent(original, changed)


@pytest.fixture
def project(tmp_path, monkeypatch):
    schemas = tmp_path / "resources" / "schemas"
    schemas.mkdir(parents=True)
    (schemas / "contract.yaml").write_text(yaml.safe_dump(SCHEMA_SPEC, sort_keys=False))
    (schemas / "not_a_schema.yaml").write_text("foo: bar\n")
    (tmp_path / "io.py").write_text("from dynamicio import UnifiedIO\nfrom dynamicio.core import SCHEMA_FROM_FILE\n\n\nclass IO(UnifiedIO):\n    schema = SCHEMA_FROM_FILE\n")
    (tmp_path / "definitions.yaml").write_text("---\nX:\n  LOCAL:\n    type: local\n  schema:\n    file_path: schemas/contract.yaml\n")
    monkeypatch.chdir(tmp_path)
    return tmp_path


@pytest.mark.unit
def test_migrate_writes_models_and_prompt_listing_call_sites(project):
    report = migrate("resources/schemas", "schemas.py")

    assert report["models"] == ["Contract"]
    namespace = compile_module((project / "schemas.py").read_text())
    assert schemas_equivalent(to_dataframe_schema(namespace["Contract"]), DataframeSchema(**SCHEMA_SPEC))
    prompt = (project / "MIGRATION_PROMPT.md").read_text()
    assert "`Contract`" in prompt
    assert "io.py:2" in prompt and "io.py:6" in prompt
    assert "definitions.yaml" in prompt


@pytest.mark.unit
def test_migrate_fails_without_schemas(tmp_path):
    with pytest.raises(ValueError, match="No dynamicio YAML schemas"):
        migrate(str(tmp_path), str(tmp_path / "schemas.py"))


@pytest.mark.unit
def test_migrate_disambiguates_files_whose_schemas_share_a_name(project):
    (project / "resources" / "schemas" / "sub").mkdir()
    (project / "resources" / "schemas" / "sub" / "contract.yaml").write_text(yaml.safe_dump(SCHEMA_SPEC, sort_keys=False))

    report = migrate("resources/schemas", "schemas.py")

    assert sorted(report["models"]) == ["Contract", "SubContract"]
    namespace = compile_module((project / "schemas.py").read_text())
    assert namespace["Contract"].Config.name == namespace["SubContract"].Config.name == "contract_dataset"


@pytest.mark.unit
def test_cli_migrate_subcommand(project, monkeypatch, capsys):
    monkeypatch.setattr("sys.argv", ["dynamicio", "migrate", "resources/schemas", "-o", "schemas.py"])

    cli.run()

    assert "Wrote 1 models" in capsys.readouterr().out
    assert (project / "schemas.py").exists()


@pytest.mark.unit
def test_dynamic_variable_in_a_yaml_schema_becomes_a_name_to_import(project):
    spec = {"name": "dyn", "columns": {"x": {"type": "float64", "validations": {"is_lower_than": {"apply": True, "options": {"threshold": "[[ LIMIT ]]"}}}, "metrics": []}}}
    (project / "resources" / "schemas" / "dyn.yaml").write_text(yaml.safe_dump(spec, sort_keys=False))

    report = migrate("resources/schemas", "schemas.py")

    source = (project / "schemas.py").read_text()
    assert report["todos"] == ["LIMIT"]
    assert "lt=LIMIT" in source and "# TODO" in source
    assert "LIMIT" in (project / "MIGRATION_PROMPT.md").read_text()
