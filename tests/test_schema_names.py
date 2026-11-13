"""Name resolution of a Pandera model: `__dataset_name__`, `Config.name`, the class name, and `__table_name__`."""

# pylint: disable=missing-class-docstring, missing-function-docstring
import pandera.pandas as pa
import pytest
from pandera.typing import Series

from dynamicio.schema import to_dataframe_schema


class TestDatasetName:
    @pytest.mark.unit
    def test_dataset_name_wins_over_config_name_and_class_name(self):
        class Orders(pa.DataFrameModel):
            id: Series[int] = pa.Field(nullable=True)

            __dataset_name__ = "explicit"

            class Config:
                name = "from_config"

        assert to_dataframe_schema(Orders).name == "explicit"

    @pytest.mark.unit
    def test_config_name_is_used_when_there_is_no_dataset_name(self):
        class Orders(pa.DataFrameModel):
            id: Series[int] = pa.Field(nullable=True)

            class Config:
                name = "from_config"

        assert to_dataframe_schema(Orders).name == "from_config"

    @pytest.mark.unit
    def test_config_name_equal_to_the_class_name_counts_as_unset(self):
        class FinalBar(pa.DataFrameModel):
            id: Series[int] = pa.Field(nullable=True)

            class Config:
                name = "FinalBar"

        assert to_dataframe_schema(FinalBar).name == "final_bar"

    @pytest.mark.unit
    @pytest.mark.parametrize("class_name, expected", [("FinalBar", "final_bar"), ("ThisNameABC", "this_name_abc"), ("Orders", "orders"), ("Pg2Rows", "pg2_rows")])
    def test_the_class_name_is_the_snake_case_fallback(self, class_name, expected):
        model = type(class_name, (pa.DataFrameModel,), {"__annotations__": {"id": Series[int]}, "id": pa.Field(nullable=True)})

        assert to_dataframe_schema(model).name == expected

    @pytest.mark.unit
    def test_dataset_name_is_not_inherited(self):
        class Parent(pa.DataFrameModel):
            id: Series[int] = pa.Field(nullable=True)

            __dataset_name__ = "parent_label"

        class Child(Parent):
            pass

        assert to_dataframe_schema(Parent).name == "parent_label"
        assert to_dataframe_schema(Child).name == "child"

    @pytest.mark.unit
    @pytest.mark.parametrize("bad", ["", "   ", 3, ["x"]])
    def test_an_invalid_dataset_name_is_refused(self, bad):
        class Bad(pa.DataFrameModel):
            id: Series[int] = pa.Field(nullable=True)

            __dataset_name__ = bad

        with pytest.raises(ValueError, match="__dataset_name__ must be a non-empty string"):
            to_dataframe_schema(Bad)


class TestTableName:
    @pytest.mark.unit
    def test_a_model_without_table_name_has_none(self):
        class NoTable(pa.DataFrameModel):
            id: Series[int] = pa.Field(nullable=True)

        assert to_dataframe_schema(NoTable).table_name is None

    @pytest.mark.unit
    def test_the_table_name_is_kept_exactly_and_is_independent_of_the_dataset_name(self):
        class Orders(pa.DataFrameModel):
            id: Series[int] = pa.Field(nullable=True)

            __dataset_name__ = "orders_label"
            __table_name__ = "Orders_v2"

        schema = to_dataframe_schema(Orders)

        assert (schema.name, schema.table_name) == ("orders_label", "Orders_v2")

    @pytest.mark.unit
    def test_the_table_name_is_not_derived_from_the_dataset_name_or_class(self):
        class Orders(pa.DataFrameModel):
            id: Series[int] = pa.Field(nullable=True)

            __dataset_name__ = "orders_label"

        assert to_dataframe_schema(Orders).table_name is None

    @pytest.mark.unit
    def test_table_name_is_not_inherited(self):
        class Parent(pa.DataFrameModel):
            id: Series[int] = pa.Field(nullable=True)

            __table_name__ = "parent_table"

        class Child(Parent):
            pass

        assert to_dataframe_schema(Parent).table_name == "parent_table"
        assert to_dataframe_schema(Child).table_name is None

    @pytest.mark.unit
    @pytest.mark.parametrize("bad", ["", "  ", 7])
    def test_an_invalid_table_name_is_refused(self, bad):
        class Bad(pa.DataFrameModel):
            id: Series[int] = pa.Field(nullable=True)

            __table_name__ = bad

        with pytest.raises(ValueError, match="__table_name__ must be a non-empty string"):
            to_dataframe_schema(Bad)
