# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring

import pytest

from dynamicio.schema import Field, Metric
from dynamicio.schema.models import ModelCompiler, dataframe_validator, to_dataframe_schema

pl = pytest.importorskip("polars")
pa_polars = pytest.importorskip("pandera.polars")
DataModel = pytest.importorskip("dynamicio.schema.polars").DataModel


class TestPolarsDataModel:
    @pytest.mark.unit
    def test_the_class_keywords_name_the_dataset_and_the_table(self):
        class Orders(DataModel, dataset="orders", table="sales.orders"):
            order_id: int

        assert (Orders.__dataset_name__, Orders.__table_name__) == ("orders", "sales.orders")
        assert (to_dataframe_schema(Orders).name, to_dataframe_schema(Orders).table_name) == ("orders", "sales.orders")

    @pytest.mark.unit
    @pytest.mark.parametrize("dataset", ["", "  ", 1])
    def test_an_empty_or_non_string_dataset_is_refused(self, dataset):
        with pytest.raises(ValueError, match="must be a non-empty string"):

            class Orders(DataModel, dataset=dataset):  # pylint: disable=unused-variable
                order_id: int

    @pytest.mark.unit
    def test_a_model_without_a_dataset_is_refused_at_compile(self):
        from dynamicio.errors import DatasetNameNotFoundError  # pylint: disable=import-outside-toplevel

        class Unnamed(DataModel):
            order_id: int

        with pytest.raises(DatasetNameNotFoundError):
            to_dataframe_schema(Unnamed)

    @pytest.mark.unit
    def test_dynamicio_field_metadata_compiles_on_a_polars_model(self):
        class Measured(DataModel, dataset="measured"):
            value: float = Field(nullable=True, ge=0.0, le=1.0, metrics=[Metric.MIN, Metric.MAX], max_null_fraction=0.2)

        column = to_dataframe_schema(Measured).columns["value"]

        assert column.data_type == "float64"
        assert [metric.value for metric in column.metrics] == ["Min", "Max"]
        assert {validation.name for validation in column.validations} == {"is_greater_than_or_equal", "is_lower_than_or_equal", "has_acceptable_percentage_of_nulls"}

    @pytest.mark.unit
    def test_a_polars_dataframe_check_compiles_to_a_polars_schema_without_an_index(self):
        class Unique(DataModel, dataset="unique"):
            a: int

            class Config:
                unique = ["a"]

        validator = dataframe_validator(Unique)

        assert isinstance(validator, pa_polars.DataFrameSchema)
        assert validator.unique == ["a"]
        validator.validate(pl.DataFrame({"a": [1, 2]}))

    @pytest.mark.unit
    def test_a_polars_model_without_dataframe_checks_has_no_dataframe_validator(self):
        class Plain(DataModel, dataset="plain"):
            a: int

        assert dataframe_validator(Plain) is None

    @pytest.mark.unit
    def test_pandera_of_names_the_module_of_the_backend(self):
        class Plain(DataModel, dataset="plain"):
            a: int

        assert ModelCompiler.pandera_of(Plain) is pa_polars

    @pytest.mark.unit
    def test_an_unsupported_option_on_a_polars_model_is_refused(self):
        class Refused(DataModel, dataset="refused"):
            a: int = pa_polars.Field(default=0)

        with pytest.raises(ValueError, match="default"):
            to_dataframe_schema(Refused)
