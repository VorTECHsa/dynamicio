"""Pandera constructs without a dynamicio validator: they run, or they are refused. Nothing is dropped silently."""

# pylint: disable=missing-class-docstring, missing-function-docstring, redefined-outer-name
import pandas as pd
import pandera.pandas as pa
import pytest
import yaml
from pandera.typing import Series

from dynamicio import UnifiedIO
from dynamicio.config import IOConfig
from dynamicio.errors import SchemaValidationError
from dynamicio.schema.models import to_dataframe_schema


@pytest.fixture
def make_io(tmp_path):
    csv_file = tmp_path / "data.csv"
    definition = {"X": {"LOCAL": {"type": "local", "local": {"file_path": str(csv_file), "file_type": "csv", "options": {"index": False}}}}}
    (tmp_path / "definitions.yaml").write_text(yaml.safe_dump(definition))

    class _NoVars:
        pass

    def _make(model, df: pd.DataFrame):
        df.to_csv(csv_file, index=False)
        resource = IOConfig(str(tmp_path / "definitions.yaml"), env_identifier="LOCAL", dynamic_vars=_NoVars).get(source_key="X")

        class IO(UnifiedIO):
            schema = model

        return IO(source_config=resource, apply_schema_validations=True)

    return _make


class Checked(pa.DataFrameModel):
    __dataset_name__ = "checked"

    code: Series[str] = pa.Field(str_matches=r"^[A-Z]{3}$")
    size: Series[int] = pa.Field()
    other: Series[int] = pa.Field()

    @pa.check("size")
    @classmethod
    def size_is_even(cls, series: Series[int]) -> Series[bool]:
        return series % 2 == 0

    @pa.dataframe_check
    @classmethod
    def size_not_above_other(cls, df: pd.DataFrame) -> Series[bool]:
        return df["size"] <= df["other"]

    class Config:
        unique = ["code", "size"]
        strict = "filter"


GOOD = pd.DataFrame({"code": ["ABC", "DEF"], "size": [2, 4], "other": [3, 5]})


@pytest.mark.unit
def test_valid_data_passes_every_construct(make_io):
    assert make_io(Checked, GOOD).read().equals(GOOD)


@pytest.mark.unit
@pytest.mark.parametrize(
    "bad, failing",
    [
        (GOOD.assign(code=["ABC", "de"]), "str_matches"),
        (GOOD.assign(size=[2, 3], other=[3, 5]), "size_is_even"),
        (GOOD.assign(other=[1, 5]), "size_not_above_other"),
        (pd.DataFrame({"code": ["ABC", "ABC"], "size": [2, 2], "other": [3, 5]}), "multiple_fields_uniqueness"),
    ],
)
def test_each_construct_fails_the_read(make_io, bad, failing):
    with pytest.raises(SchemaValidationError) as error:
        make_io(Checked, bad).read()

    assert any(failing in str(key) for key in error.value.message), error.value.message


@pytest.mark.unit
def test_an_ordered_model_fails_when_columns_are_out_of_order(make_io):
    class Ordered(pa.DataFrameModel):
        __dataset_name__ = "ordered"

        a: Series[int]
        b: Series[int]

        class Config:
            ordered = True

    with pytest.raises(SchemaValidationError):
        make_io(Ordered, pd.DataFrame({"a": [1], "b": [2]})).validate_from_schema(pd.DataFrame({"b": [1], "a": [2]}))
    make_io(Ordered, pd.DataFrame({"a": [1], "b": [2]})).validate_from_schema(pd.DataFrame({"a": [1], "b": [2]}))


@pytest.mark.unit
def test_an_index_declared_on_the_model_is_checked(make_io):
    class Indexed(pa.DataFrameModel):
        __dataset_name__ = "indexed"

        a: Series[int]
        idx: pa.typing.Index[int] = pa.Field(ge=0)

    io = make_io(Indexed, pd.DataFrame({"a": [1, 2]}))
    with pytest.raises(SchemaValidationError):
        io.validate_from_schema(pd.DataFrame({"a": [1, 2]}, index=pd.Index([0, -1], name="idx")))


@pytest.mark.unit
@pytest.mark.parametrize(
    "config, message",
    [
        ({"strict": True}, "strict"),
        ({"add_missing_columns": True}, "add_missing_columns"),
        ({"unique_column_names": True}, "unique_column_names"),
        ({"drop_invalid_rows": True}, "drop_invalid_rows"),
    ],
)
def test_options_dynamicio_cannot_honour_are_refused(config, message):
    model = type("Refused", (pa.DataFrameModel,), {"__annotations__": {"a": Series[int]}, "Config": type("Config", (), config)})

    with pytest.raises(ValueError, match=message):
        to_dataframe_schema(model)


@pytest.mark.unit
def test_a_column_option_dynamicio_cannot_honour_is_refused():
    class Refused(pa.DataFrameModel):
        __dataset_name__ = "refused"

        a: Series[int] = pa.Field(default=0)

    with pytest.raises(ValueError, match="default"):
        to_dataframe_schema(Refused)


@pytest.mark.unit
def test_a_polars_model_is_refused():
    pl = pytest.importorskip("pandera.polars")

    class Frame(pl.DataFrameModel):
        __dataset_name__ = "frame"

        a: int

    with pytest.raises(ValueError, match="pandas"):
        to_dataframe_schema(Frame)


@pytest.mark.unit
def test_str_follows_the_pandas_default_string_dtype():
    class Strings(pa.DataFrameModel):
        __dataset_name__ = "strings"

        a: Series[str]

    assert to_dataframe_schema(Strings).columns["a"].data_type == str(pd.Series([""]).dtype)


@pytest.mark.unit
def test_a_declared_coerce_gives_the_nullable_dtype():
    class Coerced(pa.DataFrameModel):
        __dataset_name__ = "coerced"

        a: Series[int] = pa.Field(nullable=True, coerce=True)

    assert to_dataframe_schema(Coerced).columns["a"].data_type == "Int64"
