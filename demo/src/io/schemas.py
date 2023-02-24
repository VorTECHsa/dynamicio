__all__ = ["Foo", "Bar", "StagedFoo", "StagedBar", "FinalFoo", "FinalBar"]

from pandera import SchemaModel, String, Float, Field
from pandera.typing import Series


class Foo(SchemaModel):
    column_a: Series[String] = Field(unique=True, report_duplicates="all", logging={"metrics": ["Counts"], "dataset_name": "Foo", "column": "column_a"})
    column_b: Series[String] = Field(nullable=False, logging={"metrics": ["CountsPerLabel"], "dataset_name": "Foo", "column": "column_a"})
    column_c: Series[Float] = Field(gt=1000)
    column_d: Series[Float] = Field(lt=1000, logging={"metrics": ["Min", "Max", "Mean", "Std", "Variance"], "dataset_name": "Foo", "column": "column_a"})

class Bar(SchemaModel):
    column_a: Series[String] = Field(unique=True, report_duplicates="all", logging={"metrics": ["Counts"], "dataset_name": "Foo", "column": "column_a"})
    column_b: Series[String] = Field(nullable=False, logging={"metrics": ["CountsPerLabel"], "dataset_name": "Foo", "column": "column_a"})
    column_c: Series[Float] = Field(gt=1000)
    column_d: Series[Float] = Field(lt=1000, logging={"metrics": ["Min", "Max", "Mean", "Std", "Variance"], "dataset_name": "Foo", "column": "column_a"})

class StagedFoo(SchemaModel):
    column_a: Series[String] = Field(unique=True, report_duplicates="all", logging={"metrics": ["Counts"], "dataset_name": "Foo", "column": "column_a"})
    column_b: Series[String] = Field(nullable=False, logging={"metrics": ["CountsPerLabel"], "dataset_name": "Foo", "column": "column_a"})
    column_c: Series[Float] = Field(gt=1000)
    column_d: Series[Float] = Field(lt=1000, logging={"metrics": ["Min", "Max", "Mean", "Std", "Variance"], "dataset_name": "Foo", "column": "column_a"})

class StagedBar(SchemaModel):
    column_a: Series[String] = Field(unique=True, report_duplicates="all", logging={"metrics": ["Counts"], "dataset_name": "Foo", "column": "column_a"})
    column_b: Series[String] = Field(nullable=False, logging={"metrics": ["CountsPerLabel"], "dataset_name": "Foo", "column": "column_a"})
    column_c: Series[Float] = Field(gt=1000)
    column_d: Series[Float] = Field(lt=1000, logging={"metrics": ["Min", "Max", "Mean", "Std", "Variance"], "dataset_name": "Foo", "column": "column_a"})

class FinalFoo(SchemaModel):
    column_a: Series[String] = Field(unique=True, report_duplicates="all", logging={"metrics": ["Counts"], "dataset_name": "Foo", "column": "column_a"})
    column_b: Series[String] = Field(nullable=False, logging={"metrics": ["CountsPerLabel"], "dataset_name": "Foo", "column": "column_a"})
    column_c: Series[Float] = Field(gt=1000)
    column_d: Series[Float] = Field(lt=1000, logging={"metrics": ["Min", "Max", "Mean", "Std", "Variance"], "dataset_name": "Foo", "column": "column_a"})

class FinalBar(SchemaModel):
    column_a: Series[String] = Field(unique=True, report_duplicates="all", logging={"metrics": ["Counts"], "dataset_name": "Foo", "column": "column_a"})
    column_b: Series[String] = Field(nullable=False, logging={"metrics": ["CountsPerLabel"], "dataset_name": "Foo", "column": "column_a"})
    column_c: Series[Float] = Field(gt=1000)
    column_d: Series[Float] = Field(lt=1000, logging={"metrics": ["Min", "Max", "Mean", "Std", "Variance"], "dataset_name": "Foo", "column": "column_a"})