"""`__io__ = Behaviour(...)` on a model sets the defaults of the IO arguments; an argument of the call overrides it."""

# pylint: disable=missing-class-docstring, missing-function-docstring, redefined-outer-name
import pandas as pd
import pandera.pandas as pa
import pytest
import yaml
from pandera.typing import Series

from dynamicio import UnifiedIO
from dynamicio.config import IOConfig
from dynamicio.errors import SchemaValidationError
from dynamicio.schema import Behaviour


class Plain(pa.DataFrameModel):
    size: Series[int] = pa.Field(gt=0)


class Validated(pa.DataFrameModel):
    __io__ = Behaviour(validate=True, log_metrics=True, show_casting_warnings=True, strict_dtype_check=True)

    size: Series[int] = pa.Field(gt=0)


class ValidatedChild(Validated):
    """Inherits the declaration of its base."""


@pytest.fixture
def resource(tmp_path):
    definition = {"X": {"LOCAL": {"type": "local", "local": {"file_path": str(tmp_path / "data.csv"), "file_type": "csv", "options": {"index": False}}}}}
    (tmp_path / "definitions.yaml").write_text(yaml.safe_dump(definition))

    class _NoVars:
        pass

    pd.DataFrame({"size": [-1]}).to_csv(tmp_path / "data.csv", index=False)
    return IOConfig(str(tmp_path / "definitions.yaml"), env_identifier="LOCAL", dynamic_vars=_NoVars).get(source_key="X")


class TestBehaviour:
    @pytest.mark.unit
    def test_a_model_without_a_declaration_has_every_switch_off(self, resource):
        io = UnifiedIO(resource, schema=Plain)

        assert (io.apply_schema_validations, io.log_schema_metrics, io.show_casting_warnings, io.strict_dtype_check) == (False, False, False, False)
        assert io.read()["size"].tolist() == [-1]  # nothing validated

    @pytest.mark.unit
    @pytest.mark.parametrize("model", [Validated, ValidatedChild])
    def test_the_declaration_of_the_model_is_the_default_of_every_argument(self, resource, model):
        io = UnifiedIO(resource, schema=model)

        assert (io.apply_schema_validations, io.log_schema_metrics, io.show_casting_warnings, io.strict_dtype_check) == (True, True, True, True)
        with pytest.raises(SchemaValidationError):
            io.read()

    @pytest.mark.unit
    def test_an_argument_of_the_call_overrides_the_model_in_either_direction(self, resource):
        off = UnifiedIO(resource, schema=Validated, apply_schema_validations=False, log_schema_metrics=False)
        on = UnifiedIO(resource, schema=Plain, apply_schema_validations=True)

        assert (off.apply_schema_validations, off.log_schema_metrics, off.show_casting_warnings) == (False, False, True)
        assert off.read()["size"].tolist() == [-1]
        with pytest.raises(SchemaValidationError):
            on.read()

    @pytest.mark.unit
    def test_a_declaration_that_is_not_a_behaviour_is_refused(self, resource):
        class Wrong(pa.DataFrameModel):
            __io__ = {"validate": True}

            size: Series[int] = pa.Field()

        with pytest.raises(TypeError, match="Behaviour"):
            UnifiedIO(resource, schema=Wrong)

    @pytest.mark.unit
    def test_the_class_attribute_schema_of_an_io_class_is_honoured_too(self, resource):
        class IO(UnifiedIO):
            schema = Validated

        assert IO(resource).apply_schema_validations is True
