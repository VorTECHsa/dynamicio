# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, redefined-outer-name
import importlib
import importlib.util
import sys
import textwrap
import uuid

import pandas as pd
import pytest
import yaml
from pandera import errors as pandera_errors

# Application Imports
from dynamicio import cli
from dynamicio.engines import EngineSetting
from dynamicio.schema.migrate import SchemaComparison
from dynamicio.schema.models import to_dataframe_schema

pl = pytest.importorskip("polars")
PolarsMigration = pytest.importorskip("dynamicio.schema.migrate.to_polars.migration").PolarsMigration
PortedModel = pytest.importorskip("dynamicio.schema.migrate.to_polars.ported_model").PortedModel
PolarsModelRenderer = pytest.importorskip("dynamicio.schema.migrate.to_polars.renderer").PolarsModelRenderer
PolarsMigrationReport = pytest.importorskip("dynamicio.schema.migrate.to_polars.report").PolarsMigrationReport
ResourceReview = pytest.importorskip("dynamicio.schema.migrate.to_polars.resource_review").ResourceReview
DtypeMap = pytest.importorskip("dynamicio.schema.polars_dtypes").DtypeMap

FULL_MODELS = """
    from typing import Annotated

    import numpy as np
    import pandas as pd
    import pandera.pandas as pa
    from pandera.typing import Series

    from dynamicio.schema import DataModel, Field, Metric


    class Orders(DataModel, dataset="orders", table="sales.orders"):
        order_id: Series[int] = Field(nullable=False, unique=True, ge=0, metrics=[Metric.COUNTS])
        quantity: Series[pd.Int64Dtype] = Field(nullable=True, lt=100)
        price: Series[np.float32] = Field(nullable=True, gt=0.5, le=10.0, metrics=[Metric.MIN, Metric.MAX])
        status: Series[str] = Field(nullable=False, isin=["open", "closed"])
        region: Series[pd.StringDtype] = Field(nullable=True, exactly=["eu", "us"])
        paid: Series[bool] = Field(nullable=False)
        flag: Series[pd.BooleanDtype] = Field(nullable=True, max_null_fraction=0.25)
        created: Series[pd.Timestamp] = Field(nullable=False)
        created_utc: Series[Annotated[pd.DatetimeTZDtype, "ns", "UTC"]] = Field(nullable=True)
        wait: Series[pd.Timedelta] = Field(nullable=True)
        kind: Series[pd.CategoricalDtype] = Field(nullable=True)
        small: Series[np.uint8] = Field(nullable=True)
        unit_price: Series[float] = Field(nullable=True, alias="unit price")

        class Config:
            strict = "filter"
            coerce = True
            unique = ["order_id", "status"]


    class Plain(DataModel, dataset="plain"):
        a: Series[int] = Field(nullable=True)
"""


class Package:
    """A package of pandas models on disk, imported by its dotted name from the root of a test repo."""

    @staticmethod
    def write(root, modules):
        name = f"models_{uuid.uuid4().hex[:8]}"
        package = root / name
        package.mkdir()
        (package / "__init__.py").write_text("")
        for module, source in modules.items():
            (package / f"{module}.py").write_text(textwrap.dedent(source))
        return package


@pytest.fixture
def repo(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "path", [str(tmp_path), *sys.path])
    before = set(sys.modules)
    yield tmp_path
    for name in set(sys.modules) - before:
        if name.startswith("models_") or name.startswith("out_"):
            del sys.modules[name]
    EngineSetting.reset()


class Generated:
    @staticmethod
    def load(path):
        """Import a generated module from its file, under a new name."""
        spec = importlib.util.spec_from_file_location(f"out_{uuid.uuid4().hex[:8]}", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


class TestDtypes:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "pandas_dtype, annotation, pandas_dtype_argument",
        [
            ("int64", "pl.Int64", None),
            ("Int64", "pl.Int64", "Int64"),
            ("int8", "pl.Int8", None),
            ("UInt32", "pl.UInt32", "UInt32"),
            ("uint8", "pl.UInt8", None),
            ("float32", "pl.Float32", None),
            ("float64", "pl.Float64", None),
            ("object", "pl.String", None),
            ("string", "pl.String", "string"),
            ("string[pyarrow]", "pl.String", "string[pyarrow]"),
            ("bool", "pl.Boolean", None),
            ("boolean", "pl.Boolean", "boolean"),
            ("category", "pl.Categorical", None),
            ("datetime64[ns]", "Annotated[pl.Datetime, 'ns', None]", None),
            ("datetime64[us, Europe/London]", "Annotated[pl.Datetime, 'us', 'Europe/London']", None),
            ("timedelta64[ms]", "Annotated[pl.Duration, 'ms']", None),
        ],
    )
    def test_each_pandas_dtype_of_the_map_gives_its_polars_annotation_and_pandas_dtype(self, pandas_dtype, annotation, pandas_dtype_argument):
        assert PolarsModelRenderer.polars_dtype_source(pandas_dtype) == annotation
        assert PolarsModelRenderer.pandas_dtype_argument(pandas_dtype) == pandas_dtype_argument

    @pytest.mark.unit
    @pytest.mark.parametrize("pandas_dtype", ["datetime64[s]", "timedelta64[s]", "period[D]", "complex128"])
    def test_a_pandas_dtype_without_a_polars_pair_is_refused(self, pandas_dtype):
        with pytest.raises(ValueError, match="has no polars dtype"):
            PolarsModelRenderer.polars_dtype_source(pandas_dtype)


class TestModels:
    @pytest.mark.unit
    def test_each_field_option_is_ported_and_the_polars_model_has_parity(self, repo):
        # Given
        package = Package.write(repo, {"orders": FULL_MODELS})

        # When
        report = PolarsMigration.migrate(str(package), str(repo / "polars_models.py"))

        # Then
        assert report["models"] == [f"{package.name}.orders.Orders", f"{package.name}.orders.Plain"] and report["todos"] == []
        generated = Generated.load(repo / "polars_models.py")
        pandas_models = importlib.import_module(f"{package.name}.orders")
        for name in ("Orders", "Plain"):
            ported, source = to_dataframe_schema(getattr(generated, name)), to_dataframe_schema(getattr(pandas_models, name))
            assert SchemaComparison.differences(ported, source) == [] and ported.table_name == source.table_name
            assert {column: c.data_type for column, c in ported.columns.items()} == {column: c.data_type for column, c in source.columns.items()}
        schema = generated.Orders.to_schema()
        assert schema.strict == "filter" and schema.coerce and schema.unique == ["order_id", "status"]
        assert "unit price" in schema.columns and generated.Orders.__table_name__ == "sales.orders"

    @pytest.mark.unit
    def test_an_object_column_becomes_a_string_column_and_the_report_lists_it_for_review(self, repo):
        # Given
        package = Package.write(
            repo,
            {
                "tags": "from pandera.typing import Series\nfrom dynamicio.schema import DataModel, Field\n\n"
                "class Tags(DataModel, dataset='tags'):\n    tags: Series[object] = Field(nullable=True)\n    name: Series[str] = Field(nullable=True)\n"
            },
        )

        # When
        report = PolarsMigration.migrate(str(package), str(repo / "out.py"))

        # Then
        assert DtypeMap.of_model(Generated.load(repo / "out.py").Tags) == {"tags": pl.String, "name": pl.String}
        assert report["objects"] == ["Tags.tags"]
        assert "- Tags.tags" in (repo / "POLARS_MIGRATION.md").read_text()

    @pytest.mark.unit
    def test_the_generated_module_imports_only_what_it_uses(self, repo):
        # Given
        package = Package.write(
            repo,
            {
                "plain": "from pandera.typing import Series\nfrom dynamicio.schema import DataModel, Field\n\n"
                "class A(DataModel, dataset='a'):\n    a: Series[int] = Field(nullable=True)\n"
            },
        )

        # When
        PolarsMigration.migrate(str(package), str(repo / "out.py"))

        # Then
        source = (repo / "out.py").read_text()
        assert source.startswith("import polars as pl\n\nfrom dynamicio.schema import Field\nfrom dynamicio.schema.polars import DataModel\n\n\nclass A(")
        assert "Annotated" not in source and "Metric" not in source and "class Config" not in source

    @pytest.mark.unit
    def test_the_generated_model_validates_a_polars_frame(self, repo):
        # Given
        package = Package.write(repo, {"orders": FULL_MODELS})
        PolarsMigration.migrate(str(package), str(repo / "out.py"))
        plain = Generated.load(repo / "out.py").Plain

        # When
        validated = plain.validate(pl.DataFrame({"a": [1, None]}))

        # Then
        assert validated["a"].to_list() == [1, None]
        with pytest.raises(pandera_errors.SchemaError):
            plain.validate(pl.DataFrame({"a": ["x"]}))

    @pytest.mark.unit
    def test_checks_dataframe_checks_and_an_index_are_listed_to_port_and_left_out(self, repo):
        # Given
        package = Package.write(
            repo,
            {"checked": """
                    import pandera.pandas as pa
                    from pandera.typing import Index, Series

                    from dynamicio.schema import DataModel, Field


                    class Checked(DataModel, dataset="checked"):
                        idx: Index[int]
                        code: Series[str] = Field(nullable=False, str_matches=r"^[A-Z]+$")
                        value: Series[int] = Field(nullable=True, ge=0)

                        @pa.check("value")
                        def even(cls, series):
                            return series % 2 == 0

                        @pa.dataframe_check
                        def ordered(cls, df):
                            return df["value"] >= 0
                    """},
        )

        # When
        report = PolarsMigration.migrate(str(package), str(repo / "out.py"))

        # Then
        assert sorted(report["todos"]) == [
            "Checked.code: port the check 'str_matches' to polars",
            "Checked.value: port the check 'even' to polars",
            "Checked: a polars frame has no index. Write the index as a column (reset_index()) and declare it in the model",
            "Checked: port the dataframe checks ['ordered'] to polars",
        ]
        columns = to_dataframe_schema(Generated.load(repo / "out.py").Checked).columns
        assert [v.name for v in columns["value"].validations] == ["is_greater_than_or_equal"]
        assert [v.name for v in columns["code"].validations] == ["has_no_null_values"]
        assert "port the check 'even'" in (repo / "POLARS_MIGRATION.md").read_text()

    @pytest.mark.unit
    def test_a_column_of_a_dtype_without_a_polars_pair_fails_and_names_the_model(self, repo):
        # Given
        package = Package.write(
            repo,
            {"odd": """
                    from typing import Annotated
                    import pandas as pd
                    from pandera.typing import Series
                    from dynamicio.schema import DataModel, Field

                    class Odd(DataModel, dataset="odd"):
                        when: Series[Annotated[pd.DatetimeTZDtype, "s", "UTC"]] = Field(nullable=True)
                    """},
        )

        # When / Then
        with pytest.raises(ValueError, match=r"orders|odd\.Odd: the pandas dtype 'datetime64\[s, UTC\]' has no polars dtype"):
            PolarsMigration.migrate(str(package), str(repo / "out.py"))
        assert not (repo / "out.py").exists()

    @pytest.mark.unit
    def test_column_names_that_are_not_identifiers_or_shadow_the_module_get_an_alias(self, repo):
        # Given
        package = Package.write(
            repo,
            {"names": """
                    from pandera.typing import Series
                    from dynamicio.schema import DataModel, Field

                    class Names(DataModel, dataset="names"):
                        pl_col: Series[int] = Field(nullable=True, alias="pl")
                        klass: Series[int] = Field(nullable=True, alias="class")
                        dashed: Series[int] = Field(nullable=True, alias="a-b")
                    """},
        )

        # When
        PolarsMigration.migrate(str(package), str(repo / "out.py"))

        # Then
        source = (repo / "out.py").read_text()
        assert "column_pl: pl.Int64 = Field(nullable=True, alias='pl')" in source
        assert "column_class: pl.Int64 = Field(nullable=True, alias='class')" in source
        assert "column_a_b: pl.Int64 = Field(nullable=True, alias='a-b')" in source
        assert list(Generated.load(repo / "out.py").Names.to_schema().columns) == ["pl", "class", "a-b"]


class TestModules:
    @pytest.mark.unit
    def test_a_package_output_has_one_module_for_each_module_of_the_pandas_models(self, repo):
        # Given
        plain = "from pandera.typing import Series\nfrom dynamicio.schema import DataModel, Field\n\nclass {0}(DataModel, dataset='{0}'):\n    a: Series[int] = Field(nullable=True)\n"
        package = Package.write(repo, {"stage": plain.format("A"), "transform": plain.format("B"), "helpers": "X = 1\n"})
        out = repo / "polars_models"
        out.mkdir()
        (out / "__init__.py").write_text('"""Mine."""\n')

        # When
        report = PolarsMigration.migrate(str(package), str(out))

        # Then
        assert sorted(p.name for p in out.iterdir()) == ["POLARS_MIGRATION.md", "__init__.py", "stage.py", "transform.py"]
        assert (out / "__init__.py").read_text() == '"""Mine."""\n'
        assert report["report"] == str(out / "POLARS_MIGRATION.md")

    @pytest.mark.unit
    def test_two_models_of_one_name_in_one_output_module_are_refused(self, repo):
        # Given
        plain = "from pandera.typing import Series\nfrom dynamicio.schema import DataModel, Field\n\nclass A(DataModel, dataset='{0}'):\n    a: Series[int] = Field(nullable=True)\n"
        package = Package.write(repo, {"one": plain.format("one"), "two": plain.format("two")})

        # When / Then
        with pytest.raises(ValueError, match=r"one\.A and .*two\.A both map to the polars model A"):
            PolarsMigration.migrate(str(package), str(repo / "out.py"))

    @pytest.mark.unit
    def test_a_compatibility_alias_of_a_model_is_ported_once(self, repo):
        # Given
        source = (
            "from pandera.typing import Series\nfrom dynamicio.schema import DataModel, Field\n\n"
            "class Orders(DataModel, dataset='orders'):\n    a: Series[int] = Field(nullable=True)\n\nLegacyOrders = Orders\n"
        )
        package = Package.write(repo, {"orders": source})

        # When
        PolarsMigration.migrate(str(package), str(repo / "out.py"))

        # Then
        assert (repo / "out.py").read_text().count("class Orders(") == 1

    @pytest.mark.unit
    @pytest.mark.parametrize("name", ["Metric", "Field", "DataModel", "Annotated", "pl"])
    def test_a_model_named_like_an_import_of_the_generated_module_is_refused(self, repo, name):
        # Given
        source = (
            f"from pandera.typing import Series\nfrom dynamicio.schema import DataModel as Base, Field as F\n\nclass {name}(Base, dataset='x'):\n    a: Series[int] = F(nullable=True)\n"
        )
        package = Package.write(repo, {"named": source})

        # When / Then
        with pytest.raises(ValueError, match=rf"{name} shadows a name that the generated module imports"):
            PolarsMigration.migrate(str(package), str(repo / "out.py"))

    @pytest.mark.unit
    @pytest.mark.parametrize("value", ["datetime.datetime(2026, 1, 1)", "Timestamp('2026-01-01')"])
    def test_a_bound_without_a_literal_source_is_refused(self, value):
        # When / Then
        with pytest.raises(ValueError, match="no literal source"):
            PolarsModelRenderer.check_literal_arguments(f"Field(nullable=True, greater_than={value})", "when")

    @pytest.mark.unit
    def test_literal_bounds_and_metrics_are_accepted(self):
        # When / Then
        PolarsModelRenderer.check_literal_arguments("Field(nullable=False, greater_than=0, isin=['a', 'b'], metrics=[Metric.MIN])", "a")

    @pytest.mark.unit
    def test_imported_and_polars_models_are_not_ported(self, repo):
        # Given
        package = Package.write(
            repo,
            {
                "base": "from pandera.typing import Series\nfrom dynamicio.schema import DataModel, Field\n\n"
                "class A(DataModel, dataset='a'):\n    a: Series[int] = Field(nullable=True)\n",
                "uses": "import polars as pl\nfrom dynamicio.schema import Field\nfrom dynamicio.schema.polars import DataModel\nfrom .base import A\n\n"
                "class P(DataModel, dataset='p'):\n    a: pl.Int64 = Field(nullable=True)\n",
            },
        )

        # When
        report = PolarsMigration.migrate(str(package), str(repo / "out.py"))

        # Then
        assert report["models"] == [f"{package.name}.base.A"]

    @pytest.mark.unit
    def test_a_path_without_pandas_models_is_refused(self, repo):
        package = Package.write(repo, {"empty": "X = 1\n"})

        with pytest.raises(ValueError, match="No pandas models found"):
            PolarsMigration.migrate(str(package), str(repo / "out.py"))

    @pytest.mark.unit
    def test_a_path_outside_the_root_is_refused(self, repo, tmp_path_factory):
        outside = tmp_path_factory.mktemp("outside")

        with pytest.raises(ValueError, match="run dynamicio migrate from the root of the repo"):
            PolarsMigration.migrate(str(outside), str(repo / "out.py"))

    @pytest.mark.unit
    def test_one_module_file_can_be_the_path(self, repo):
        package = Package.write(repo, {"orders": FULL_MODELS})

        report = PolarsMigration.migrate(str(package / "orders.py"), str(repo / "out.py"))

        assert len(report["models"]) == 2

    @pytest.mark.unit
    def test_a_root_that_is_not_on_the_import_path_is_added(self, repo, monkeypatch):
        # Given
        package = Package.write(repo, {"orders": FULL_MODELS})
        monkeypatch.setattr(sys, "path", [path for path in sys.path if path != str(repo)])

        # When
        report = PolarsMigration.migrate(str(package), str(repo / "out.py"))

        # Then
        assert sys.path[0] == str(repo)
        assert len(report["models"]) == 2

    @pytest.mark.unit
    def test_a_generated_model_that_differs_from_its_pandas_model_is_refused(self, repo):
        # Given a polars model with another dtype than its pandas model
        pandas_source = (
            "from pandera.typing import Series\nfrom dynamicio.schema import DataModel, Field\n\n"
            "class Plain(DataModel, dataset='plain'):\n    a: Series[int] = Field(nullable=True)\n"
        )
        package = Package.write(repo, {"plain": pandas_source})
        pandas_model = importlib.import_module(f"{package.name}.plain").Plain
        source = (
            "import polars as pl\nfrom dynamicio.schema import Field\nfrom dynamicio.schema.polars import DataModel\n\n"
            "class Plain(DataModel, dataset='plain'):\n    a: pl.Float64 = Field(nullable=True)\n"
        )

        # When / Then
        with pytest.raises(ValueError, match=r"The polars model Plain is not equivalent to .*plain\.Plain"):
            PolarsMigration._verify_parity(source, [PortedModel(pandas_model)])  # pylint: disable=protected-access


class TestEngineContract:
    @pytest.mark.integration
    def test_a_pandas_write_reads_back_with_the_generated_polars_model(self, repo):
        # Given a pandas model, its polars model, and a parquet file that the pandas engine writes
        from tests.test_engines.test_polars_files import Files, FilesIO  # pylint: disable=import-outside-toplevel

        package = Package.write(
            repo,
            {"m": """
                    import pandas as pd
                    from pandera.typing import Series
                    from dynamicio.schema import DataModel, Field

                    class M(DataModel, dataset="m"):
                        n: Series[pd.Int64Dtype] = Field(nullable=True)
                        s: Series[str] = Field(nullable=True)
                        t: Series[pd.Timestamp] = Field(nullable=False)
                    """},
        )
        PolarsMigration.migrate(str(package), str(repo / "out.py"))
        pandas_model, polars_model = importlib.import_module(f"{package.name}.m").M, Generated.load(repo / "out.py").M
        frame = pd.DataFrame({"n": pd.array([1, None], dtype="Int64"), "s": ["x", None], "t": pd.to_datetime(["2026-01-01", "2026-01-02"])})
        EngineSetting.reset("pandas")
        FilesIO(source_config=Files.config(repo, "parquet"), schema=pandas_model).write(frame)

        # When
        EngineSetting.reset("polars")
        read = FilesIO(source_config=Files.config(repo, "parquet"), schema=polars_model).read()

        # Then
        assert read.schema == pl.Schema({"n": pl.Int64, "s": pl.String, "t": pl.Datetime("ns")})
        assert read.to_dicts() == [{"n": 1, "s": "x", "t": pd.Timestamp("2026-01-01").to_pydatetime()}, {"n": None, "s": None, "t": pd.Timestamp("2026-01-02").to_pydatetime()}]


class TestResourceReview:
    @staticmethod
    def review(tmp_path, resources):
        (tmp_path / "definitions.yaml").write_text(yaml.safe_dump(resources))
        return ResourceReview.review(str(tmp_path))

    @staticmethod
    def local(file_type, options=None, path="data"):
        environment = {"type": "local", "local": {"file_path": f"{path}.{file_type}", "file_type": file_type}}
        if options is not None:
            environment["options"] = options
        return {"LOCAL": environment}

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "environment, problem",
        [
            ({"type": "local", "local": {"file_path": "x.h5", "file_type": "hdf"}}, "not hdf files"),
            ({"type": "s3", "s3": {"bucket": "b", "file_path": "x.json", "file_type": "json"}, "options": {"orient": "index"}}, "orient"),
            ({"type": "local", "local": {"file_path": "x.parquet", "file_type": "parquet"}, "options": {"index": True}}, "has no index"),
            ({"type": "local", "local": {"file_path": "x.csv", "file_type": "csv"}, "options": {"index": False, "bogus": 10}}, "no csv option bogus"),
            ({"type": "kafka", "kafka": {"kafka_server": "s", "kafka_topic": "t"}}, "needs the option key_column"),
            ({"type": "postgres", "postgres": {}, "options": {"coerce_float": True}}, "no Postgres option coerce_float"),
        ],
    )
    def test_a_definition_that_the_polars_engine_refuses_is_listed(self, tmp_path, environment, problem):
        found = self.review(tmp_path, {"X": {"CLOUD": environment}})

        assert len(found["refused"]) == 1 and found["refused"][0].startswith("definitions.yaml: X.CLOUD: ") and problem in found["refused"][0]

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "environment",
        [
            {"type": "kafka", "kafka": {"kafka_server": "s", "kafka_topic": "t"}, "options": {"key_column": "id"}},
            {"type": "postgres", "postgres": {}},
            {"type": "athena", "athena": {}},
            {"type": "s3_path_prefix", "s3": {"bucket": "b", "path_prefix": "p/{day}/", "file_type": "parquet"}, "options": {"day": "x"}},
            {"type": "local_batch", "local": {"path_prefix": "p/", "file_type": "json"}, "options": {"orient": "records"}},
        ],
    )
    def test_a_definition_that_the_polars_engine_takes_is_not_listed(self, tmp_path, environment):
        found = self.review(tmp_path, {"X": {"CLOUD": environment}})

        assert found == {"refused": [], "csv_index": []}

    @pytest.mark.unit
    def test_csv_definitions_without_an_index_option_are_listed(self, tmp_path):
        found = self.review(tmp_path, {"A": self.local("csv"), "B": self.local("csv", {"index": False}), "C": self.local("csv", {"index": True}), "D": self.local("parquet")})

        assert found == {"refused": [], "csv_index": ["definitions.yaml: A.LOCAL"]}

    @pytest.mark.unit
    def test_the_fields_of_a_path_template_are_not_options(self, tmp_path):
        found = self.review(tmp_path, {"A": self.local("parquet", {"day": "monday"}, path="data/{day}")})

        assert found["refused"] == []

    @pytest.mark.unit
    def test_files_that_are_not_resource_definitions_are_skipped(self, tmp_path):
        # Given
        (tmp_path / "broken.yaml").write_text("a: [b\n")
        (tmp_path / "list.yml").write_text("- a\n- b\n")
        (tmp_path / "settings.yaml").write_text("name: x\ncolumns: {a: {type: int64}}\n")
        (tmp_path / "binary.yaml").write_bytes(b"\xff\xfe\x00")
        (tmp_path / "notes.txt").write_text("X:\n  LOCAL:\n    type: kafka\n")
        nested = tmp_path / "nested"
        nested.mkdir()
        (nested / "r.yaml").write_text(yaml.safe_dump({"K": {"CLOUD": {"type": "kafka", "kafka": {}}, "schema": {"file_path": "x"}}}))

        # When
        found = ResourceReview.review(str(tmp_path))

        # Then
        assert found["refused"] == ["nested/r.yaml: K.CLOUD: the polars engine needs the option key_column: a polars frame has no index to give the message keys."]

    @pytest.mark.unit
    def test_yaml_anchors_and_placeholders_are_read(self, tmp_path):
        # Given
        (tmp_path / "r.yaml").write_text(textwrap.dedent("""
                X:
                  DEV:
                    type: "s3"
                    s3: &x
                      bucket: "[[ BUCKET ]]"
                      file_path: "x.csv"
                      file_type: "csv"
                  PROD:
                    type: "s3"
                    s3:
                      <<: *x
                      bucket: "[[ PROD ]]"
                """))

        # When / Then
        assert ResourceReview.review(str(tmp_path))["csv_index"] == ["r.yaml: X.DEV", "r.yaml: X.PROD"]


class TestCli:
    @pytest.mark.unit
    def test_the_engine_option_runs_the_polars_migration_and_prints_a_summary(self, repo, capsys):
        # Given
        package = Package.write(repo, {"orders": FULL_MODELS})
        (repo / "definitions.yaml").write_text(yaml.safe_dump({"A": TestResourceReview.local("csv"), "K": {"CLOUD": {"type": "kafka", "kafka": {}}}}))

        # When
        cli.run_migrate([str(package), "--engine", "polars", "-o", str(repo / "out.py"), "--scan", str(repo)])

        # Then
        out = capsys.readouterr().out
        assert "Wrote 2 polars models to" in out
        assert "0 checks to port, 0 object columns to review, 1 refused resources, 1 CSV resources without index." in out
        report = (repo / "POLARS_MIGRATION.md").read_text()
        assert "- definitions.yaml: K.CLOUD: the polars engine needs the option key_column" in report and "- definitions.yaml: A.LOCAL" in report

    @pytest.mark.unit
    def test_an_unknown_engine_is_a_usage_error(self, capsys):
        with pytest.raises(SystemExit) as error:
            cli.run_migrate(["x", "--engine", "spark"])

        assert error.value.code == 2 and "invalid choice: 'spark'" in capsys.readouterr().err

    @pytest.mark.unit
    def test_the_report_says_none_for_an_empty_section(self):
        report = PolarsMigrationReport.render("out.py", ["a.B"], [], {"refused": [], "csv_index": []})

        assert report.count("None.") == 4 and "wrote 1 polars models to `out.py`" in report
