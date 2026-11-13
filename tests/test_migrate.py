"""Tests for ``dynamicio migrate``: YAML schemas to Pandera models, parity check, and the call-site prompt."""

# pylint: disable=missing-function-docstring, redefined-outer-name
import pandas as pd
import pandera.pandas as pa
import pytest
import yaml
from pandera.typing import Series

from dynamicio import cli
from dynamicio.config.pydantic.table_schema import DataframeSchema
from dynamicio.schema.migrate import compile_module, migrate, render_model, render_module, schemas_equivalent
from dynamicio.schema.models import to_dataframe_schema

# A v8 YAML schema exercising every validator and metric, with one non-applied validation.
SCHEMA_SPEC = {
    "name": "contract_dataset",
    "columns": {
        "a": {
            "type": "int64",
            "validations": {
                "has_unique_values": {"apply": True, "options": {}},
                "is_greater_than": {"apply": True, "options": {"threshold": 1}},
                "is_between": {"apply": True, "options": {"lower": 0, "upper": 5}},
            },
            "metrics": ["Min", "Counts", "UniqueCounts"],
        },
        "b": {
            "type": "object",
            "validations": {
                "has_no_null_values": {"apply": True, "options": {}},
                "is_in": {"apply": True, "options": {"categorical_values": ["x", "y"]}},
            },
            "metrics": ["CountsPerLabel"],
        },
        "c": {
            "type": "float64",
            "validations": {
                "has_no_null_values": {"apply": True, "options": {}},
                "has_acceptable_percentage_of_nulls": {"apply": True, "options": {"threshold": 0.3}},
                "is_lower_than_or_equal": {"apply": False, "options": {"threshold": 1}},
            },
            "metrics": ["Mean", "Max", "Std", "Variance"],
        },
    },
}
PRELUDE = (
    "import pandas as pd\nimport pandera.pandas as pa\nfrom pandera.typing import Series\n\n"
    "from dynamicio.schema import Metric, exactly_values, metrics, null_fraction_below, strictly_between\n\n\n"
)


@pytest.mark.unit
def test_pandera_default_nullable_false_adds_a_not_null_validation_the_yaml_never_had():
    class Defaults(pa.DataFrameModel):  # pylint: disable=missing-class-docstring
        a: Series[int]

    assert [v.name for v in to_dataframe_schema(Defaults).columns["a"].validations] == ["has_no_null_values"]


@pytest.mark.unit
def test_yaml_schema_migrates_to_a_model_that_compiles_back_to_the_same_schema():
    original = DataframeSchema(**SCHEMA_SPEC)
    model = compile_module(PRELUDE + render_model(original))["ContractDataset"]

    assert schemas_equivalent(to_dataframe_schema(model), original)


@pytest.mark.unit
def test_migrated_model_filters_extra_columns_like_dynamicio_does():
    model = compile_module(PRELUDE + render_model(DataframeSchema(**SCHEMA_SPEC)))["ContractDataset"]
    df = pd.DataFrame({"a": [2, 3, 4], "b": ["x", "y", "x"], "c": [1.0, 2.0, 3.0], "extra": [0, 0, 0]})

    out = model.validate(df)

    assert list(out.columns) == ["a", "b", "c"] and str(out["a"].dtype) == "int64"  # no coerce: Pandera's would make a nullable int `Int64`


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
    (tmp_path / "io.py").write_text(
        "from dynamicio import UnifiedIO\nfrom dynamicio.core import SCHEMA_FROM_FILE\n\n\nclass IO(UnifiedIO):\n    schema = SCHEMA_FROM_FILE\n"
    )  # a v8 call site
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
    assert namespace["Contract"].__dataset_name__ == namespace["SubContract"].__dataset_name__ == "contract_dataset"


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


@pytest.mark.unit
def test_is_in_with_mixed_type_categorical_values_migrates():
    original = DataframeSchema(
        name="mixed",
        columns={"c": {"type": "object", "validations": {"is_in": {"apply": True, "options": {"categorical_values": ["b", None, "a"]}}}, "metrics": []}},
    )
    model = compile_module(PRELUDE + render_model(original))["Mixed"]

    assert schemas_equivalent(to_dataframe_schema(model), original)


@pytest.mark.unit
def test_is_between_with_a_dynamic_variable_bound_migrates():
    original = DataframeSchema(
        name="dyn",
        columns={"y": {"type": "float64", "validations": {"is_between": {"apply": True, "options": {"lower": 1941, "upper": "[[ NEXT_YEAR ]]"}}}, "metrics": []}},
    )
    model = compile_module(PRELUDE + 'NEXT_YEAR = "[[ NEXT_YEAR ]]"\n\n\n' + render_model(original))["Dyn"]

    assert schemas_equivalent(to_dataframe_schema(model), original)


@pytest.mark.unit
def test_an_explicit_default_option_is_equivalent_to_omitting_it():
    def schema(options):
        return DataframeSchema(name="d", columns={"c": {"type": "object", "validations": {"is_in": {"apply": True, "options": options}}, "metrics": []}})

    assert schemas_equivalent(schema({"categorical_values": ["a"], "match_all": True}), schema({"categorical_values": ["a"]}))


@pytest.mark.unit
def test_dtype_aliases_migrate_to_their_pandas_names():
    original = DataframeSchema(name="al", columns={"a": {"type": "double", "validations": {}, "metrics": []}, "b": {"type": "int", "validations": {}, "metrics": []}})
    model = compile_module(PRELUDE + render_model(original))["Al"]

    assert schemas_equivalent(to_dataframe_schema(model), original)


@pytest.mark.unit
@pytest.mark.parametrize(
    "dtype, declared",
    [
        ("uint32", "np.uint32"),
        ("uint64", "np.uint64"),
        ("int32", "np.int32"),
        ("float32", "np.float32"),
        ("UInt32", "pd.UInt32Dtype"),
        ("Int64", "pd.Int64Dtype"),
        ("Float64", "pd.Float64Dtype"),
        ("boolean", "pd.BooleanDtype"),
    ],
)
def test_other_dtypes_migrate_to_types_not_calls(dtype, declared):
    original = DataframeSchema(name="d", columns={"a": {"type": dtype, "validations": {}, "metrics": []}})
    source = render_module({"D": original})
    model = compile_module(source)["D"]

    assert f"a: Series[{declared}]" in source and "pandas_dtype" not in source
    assert schemas_equivalent(to_dataframe_schema(model), original)


@pytest.mark.unit
def test_migrate_to_a_package_groups_models_by_source_directory(project):
    (project / "resources" / "schemas" / "stage").mkdir()
    (project / "resources" / "schemas" / "stage" / "staged.yaml").write_text(yaml.safe_dump({**SCHEMA_SPEC, "name": "staged"}, sort_keys=False))

    report = migrate("resources/schemas", "models")

    assert sorted(report["models"]) == ["Contract", "Staged"]
    assert sorted(path.name for path in (project / "models").glob("*.py")) == ["__init__.py", "common.py", "stage.py"]
    assert "class Staged" in (project / "models" / "stage.py").read_text()
    assert "class Contract" in (project / "models" / "common.py").read_text()
    assert "class Contract" not in (project / "models" / "stage.py").read_text()


@pytest.mark.unit
def test_a_yaml_object_column_migrates_to_object_and_keeps_list_values_valid():
    original = DataframeSchema(name="lists", columns={"tags": {"type": "object", "validations": {}, "metrics": []}, "label": {"type": "object", "validations": {}, "metrics": []}})
    source = render_module({"Lists": original})
    model = compile_module(source)["Lists"]

    assert "label: Series[str]" not in source and "tags: Series[object]" in source and "label: Series[object]" in source
    assert schemas_equivalent(to_dataframe_schema(model), original)
    model.validate(pd.DataFrame({"tags": [["a", "b"], []], "label": ["x", None]}))  # plain Pandera accepts lists in an `object` column


@pytest.mark.unit
def test_migrated_models_use_the_typed_helpers_and_name_their_dataset(project):
    migrate("resources/schemas", "schemas.py")

    source = (project / "schemas.py").read_text()
    assert "__dataset_name__ = 'contract_dataset'" in source
    assert "metadata=metrics(Metric.MIN, Metric.COUNTS, Metric.UNIQUE_COUNTS)" in source
    assert "metrics(Metric.MEAN, Metric.MAX, Metric.STD, Metric.VARIANCE) | null_fraction_below(0.3)" in source
    assert "from dynamicio.schema import Metric, metrics, null_fraction_below" in source
    assert "metadata={" not in source and "name = " not in source.split("class Config")[1]  # Config only keeps the column filter
    assert 'strict = "filter"' in source


@pytest.mark.unit
def test_a_model_is_given_a_table_name_only_when_a_resource_definition_binds_its_schema_to_postgres(project):
    (project / "resources" / "schemas" / "orders.yaml").write_text(yaml.safe_dump({**SCHEMA_SPEC, "name": "Orders_v2"}, sort_keys=False))
    (project / "definitions.yaml").write_text(
        "---\nORDERS:\n  LOCAL:\n    type: local\n  CLOUD:\n    type: postgres\n  schema:\n    file_path: schemas/orders.yaml\n"
        "X:\n  LOCAL:\n    type: local\n  schema:\n    file_path: schemas/contract.yaml\n"
    )

    migrate("resources/schemas", "schemas.py")

    namespace = compile_module((project / "schemas.py").read_text())
    assert to_dataframe_schema(namespace["Orders"]).table_name == "Orders_v2"  # the exact spelling of the old schema name
    assert to_dataframe_schema(namespace["Orders"]).name == "Orders_v2"
    assert to_dataframe_schema(namespace["Contract"]).table_name is None


@pytest.mark.unit
def test_scan_points_the_postgres_lookup_at_the_resource_definitions_elsewhere(tmp_path, project):
    definitions = tmp_path.parent / f"{tmp_path.name}_defs"
    definitions.mkdir()
    (definitions / "d.yaml").write_text("---\nX:\n  CLOUD:\n    type: postgres\n  schema:\n    file_path: s/contract.yaml\n")

    migrate("resources/schemas", "schemas.py", scan=str(definitions))

    assert to_dataframe_schema(compile_module((project / "schemas.py").read_text())["Contract"]).table_name == "contract_dataset"


@pytest.mark.unit
def test_postgres_models_can_be_read_by_a_postgres_io_class(project):
    (project / "definitions.yaml").write_text("---\nX:\n  CLOUD:\n    type: postgres\n  schema:\n    file_path: schemas/contract.yaml\n")

    migrate("resources/schemas", "schemas.py")

    assert to_dataframe_schema(compile_module((project / "schemas.py").read_text())["Contract"]).table_name


@pytest.mark.unit
def test_the_prompt_tells_how_to_name_datasets_and_tables(project):
    migrate("resources/schemas", "schemas.py")

    prompt = (project / "MIGRATION_PROMPT.md").read_text()
    assert "__dataset_name__" in prompt and "__table_name__" in prompt and "exactly_values" in prompt
