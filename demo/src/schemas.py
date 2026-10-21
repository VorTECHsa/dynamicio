"""Pandera models for the demo datasets (declaration only: dynamicio runs the validations and metrics)."""

import pandera.pandas as pa
from pandera.typing import Series

from demo.src.environment import LOWER_THAN_LIMIT


class Bar(pa.DataFrameModel):
    """Migrated from the dynamicio schema 'bar'."""

    column_a: Series[str] = pa.Field(nullable=True, unique=True, metadata={"dynamicio": {"metrics": ["Counts"]}})
    column_b: Series[str] = pa.Field(nullable=False, metadata={"dynamicio": {"metrics": ["CountsPerLabel"]}})
    column_c: Series[float] = pa.Field(nullable=True, gt=1000)
    column_d: Series[float] = pa.Field(nullable=True, lt=1000, metadata={"dynamicio": {"metrics": ["Min", "Max", "Mean", "Std", "Variance"]}})

    class Config:
        name = "bar"


class Events(pa.DataFrameModel):
    """Migrated from the dynamicio schema 'events'."""

    event_id: Series[int] = pa.Field(nullable=True, unique=True, metadata={"dynamicio": {"metrics": ["Counts"]}})
    vessel: Series[str] = pa.Field(nullable=False)
    model_action: Series[str] = pa.Field(nullable=True)

    class Config:
        name = "events"


class Foo(pa.DataFrameModel):
    """Migrated from the dynamicio schema 'foo'."""

    column_a: Series[str] = pa.Field(nullable=True, unique=True, metadata={"dynamicio": {"metrics": ["Counts"]}})
    column_b: Series[str] = pa.Field(nullable=False, metadata={"dynamicio": {"metrics": ["CountsPerLabel"]}})
    column_c: Series[float] = pa.Field(nullable=True, gt=1000)
    column_d: Series[float] = pa.Field(nullable=True, lt=1000, metadata={"dynamicio": {"metrics": ["Min", "Max", "Mean", "Std", "Variance"]}})

    class Config:
        name = "foo"


class FinalBar(pa.DataFrameModel):
    """Migrated from the dynamicio schema 'final_bar'."""

    column_a: Series[str] = pa.Field(nullable=True, unique=True, metadata={"dynamicio": {"metrics": ["Counts"]}})
    column_b: Series[str] = pa.Field(nullable=False, metadata={"dynamicio": {"metrics": ["CountsPerLabel"]}})
    column_c: Series[float] = pa.Field(nullable=True, gt=1000)
    column_d: Series[float] = pa.Field(nullable=True, lt=LOWER_THAN_LIMIT, metadata={"dynamicio": {"metrics": ["Min", "Max", "Mean", "Std", "Variance"]}})

    class Config:
        name = "final_bar"


class FinalFoo(pa.DataFrameModel):
    """Migrated from the dynamicio schema 'final_foo'."""

    column_a: Series[str] = pa.Field(nullable=True, unique=True, metadata={"dynamicio": {"metrics": ["Counts"]}})
    column_b: Series[str] = pa.Field(nullable=False, metadata={"dynamicio": {"metrics": ["CountsPerLabel"]}})
    column_c: Series[float] = pa.Field(nullable=True, gt=1000)
    column_d: Series[float] = pa.Field(nullable=True, lt=1000, metadata={"dynamicio": {"metrics": ["Min", "Max", "Mean", "Std", "Variance"]}})

    class Config:
        name = "final_foo"
