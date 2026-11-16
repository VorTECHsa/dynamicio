"""`DataModel` names the dataset and its table in class keywords; whether a read or write validates stays an argument of the IO call."""

# pylint: disable=missing-class-docstring, missing-function-docstring, redefined-outer-name
import pandas as pd
import pandera.pandas as pa
import pytest
import yaml
from pandera.typing import Series

from dynamicio import UnifiedIO
from dynamicio.config import IOConfig
from dynamicio.errors import DatasetNameNotFoundError, SchemaValidationError
from dynamicio.schema import DataModel, Field, to_dataframe_schema


class Plain(DataModel, dataset="plain"):
    size: Series[int] = Field(nullable=False, gt=0)


@pytest.fixture
def resource(tmp_path):
    definition = {"X": {"LOCAL": {"type": "local", "local": {"file_path": str(tmp_path / "data.csv"), "file_type": "csv", "options": {"index": False}}}}}
    (tmp_path / "definitions.yaml").write_text(yaml.safe_dump(definition))

    class _NoVars:
        pass

    pd.DataFrame({"size": [-1]}).to_csv(tmp_path / "data.csv", index=False)
    return IOConfig(str(tmp_path / "definitions.yaml"), env_identifier="LOCAL", dynamic_vars=_NoVars).get(source_key="X")


def switches(io):
    return (io.apply_schema_validations, io.log_schema_metrics, io.show_casting_warnings, io.strict_dtype_check)


class TestNames:
    @pytest.mark.unit
    def test_the_class_keywords_name_the_dataset_and_the_table(self):
        class Orders(DataModel, dataset="orders_label", table="Orders_v2"):
            id: Series[int] = Field(nullable=True)

        schema = to_dataframe_schema(Orders)

        assert (schema.name, schema.table_name) == ("orders_label", "Orders_v2")

    @pytest.mark.unit
    def test_a_model_without_a_dataset_is_refused_when_it_is_used(self):
        class Nameless(DataModel):
            id: Series[int] = Field(nullable=True)

        with pytest.raises(DatasetNameNotFoundError, match="Nameless"):
            to_dataframe_schema(Nameless)

    @pytest.mark.unit
    def test_a_base_model_can_share_columns_without_naming_a_dataset(self):
        class Base(DataModel):
            id: Series[int] = Field(nullable=True)

        class Orders(Base, dataset="orders"):
            pass

        assert list(to_dataframe_schema(Orders).columns) == ["id"]

    @pytest.mark.unit
    @pytest.mark.parametrize("keyword, bad", [("dataset", ""), ("dataset", 3), ("table", "  ")])
    def test_a_name_that_is_not_a_non_empty_string_is_refused_at_definition(self, keyword, bad):
        with pytest.raises(ValueError, match="non-empty string"):
            type("Bad", (DataModel,), {"__annotations__": {}}, **{keyword: bad})

    @pytest.mark.unit
    @pytest.mark.parametrize("keyword", ["validate", "log_metrics", "strict"])
    def test_any_other_class_keyword_is_refused_by_name(self, keyword):
        with pytest.raises(TypeError, match=keyword):
            type("Bad", (DataModel,), {"__annotations__": {}}, dataset="bad", **{keyword: True})

    @pytest.mark.unit
    def test_a_subclass_names_its_own_dataset(self):
        class Orders(DataModel, dataset="orders"):
            id: Series[int] = Field(nullable=True)

        class OpenOrders(Orders):
            pass

        with pytest.raises(DatasetNameNotFoundError, match="OpenOrders"):
            to_dataframe_schema(OpenOrders)

    @pytest.mark.unit
    def test_class_keywords_leave_pandera_options_working(self):
        class Strict(DataModel, dataset="strict"):
            id: Series[int] = Field(nullable=True)

            class Config:
                strict = "filter"

        out = Strict.validate(pd.DataFrame({"id": [1], "extra": [2]}))

        assert list(out.columns) == ["id"]

    @pytest.mark.unit
    def test_a_plain_pandera_model_declares_the_same_with_dunders(self):
        class Dunders(pa.DataFrameModel):
            __dataset_name__ = "dunders"
            __table_name__ = "Dunders"

            size: Series[int] = pa.Field(nullable=False, gt=0)

        schema = to_dataframe_schema(Dunders)

        assert (schema.name, schema.table_name) == ("dunders", "Dunders")


class TestSwitches:
    @pytest.mark.unit
    def test_the_model_never_switches_validation_on(self, resource):
        io = UnifiedIO(resource, schema=Plain)

        assert switches(io) == (False, False, False, False)
        assert io.read()["size"].tolist() == [-1]  # nothing validated

    @pytest.mark.unit
    def test_the_call_site_switches_validation_on(self, resource):
        with pytest.raises(SchemaValidationError):
            UnifiedIO(resource, schema=Plain, apply_schema_validations=True).read()
