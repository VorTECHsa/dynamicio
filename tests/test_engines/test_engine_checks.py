# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, redefined-outer-name
import subprocess
import sys
from pathlib import Path

import pandas as pd
import pytest

import dynamicio
from dynamicio import UnifiedIO
from dynamicio.engines import EngineSetting, PandasEngine, PolarsEngine
from dynamicio.errors import ModelEngineMismatchError, WrongFrameError
from dynamicio.schema.models import ModelCompiler
from tests.test_engines.ios import EngineCheckIO, Positive


@pytest.fixture
def polars_model():
    pl = pytest.importorskip("pandera.polars")

    class PolarsPositive(pl.DataFrameModel):
        __dataset_name__ = "polars_positive"

        a: int = pl.Field(gt=0)

    return PolarsPositive


class TestModelCheck:
    @pytest.mark.unit
    def test_a_polars_model_on_the_pandas_engine_is_refused_at_init(self, tmp_path, polars_model):
        with pytest.raises(ModelEngineMismatchError, match="pandera.polars model, and the engine is pandas"):
            EngineCheckIO.make(tmp_path, polars_model)

    @pytest.mark.unit
    def test_a_pandas_model_on_the_polars_engine_is_refused_at_init(self, tmp_path):
        pytest.importorskip("polars")

        # Given
        dynamicio.configure(engine="polars")

        # When / Then
        with pytest.raises(ModelEngineMismatchError, match="pandera.pandas model, and the engine is polars"):
            EngineCheckIO.make(tmp_path, Positive)

    @pytest.mark.unit
    def test_a_polars_model_as_class_attribute_is_accepted_at_class_definition(self, polars_model):
        # When
        class PolarsIO(UnifiedIO):
            schema = polars_model

        # Then
        assert PolarsIO.schema is polars_model

    @pytest.mark.unit
    def test_a_read_after_the_engine_changed_since_init_is_refused(self, tmp_path):
        pytest.importorskip("polars")

        # Given
        pd.DataFrame({"a": [1]}).to_csv(tmp_path / "data.csv", index=False)
        io = EngineCheckIO.make(tmp_path)
        dynamicio.configure(engine="polars")

        # When / Then
        with pytest.raises(ModelEngineMismatchError):
            io.read()

    @pytest.mark.unit
    def test_a_write_after_the_engine_changed_since_init_is_refused(self, tmp_path):
        pytest.importorskip("polars")

        # Given
        io = EngineCheckIO.make(tmp_path)
        dynamicio.configure(engine="polars")

        # When / Then
        with pytest.raises(ModelEngineMismatchError):
            io.write(pd.DataFrame({"a": [1]}))
        assert not (tmp_path / "data.csv").exists()

    @pytest.mark.unit
    def test_a_refused_read_does_not_freeze_the_engine(self, tmp_path):
        pytest.importorskip("polars")

        # Given
        pd.DataFrame({"a": [1]}).to_csv(tmp_path / "data.csv", index=False)
        io = EngineCheckIO.make(tmp_path)
        dynamicio.configure(engine="polars")
        with pytest.raises(ModelEngineMismatchError):
            io.read()

        # When
        dynamicio.configure(engine="pandas")

        # Then
        pd.testing.assert_frame_equal(io.read(), pd.DataFrame({"a": [1]}))
        assert EngineSetting._frozen  # pylint: disable=protected-access

    @pytest.mark.unit
    def test_a_model_engine_mismatch_is_also_a_value_error_and_a_type_error(self, tmp_path, polars_model):
        with pytest.raises(ValueError):
            EngineCheckIO.make(tmp_path, polars_model)
        with pytest.raises(TypeError):
            EngineCheckIO.make(tmp_path, polars_model)

    @pytest.mark.unit
    def test_the_polars_engine_takes_a_polars_model_and_has_no_reads_yet(self, tmp_path, polars_model):
        # Given
        dynamicio.configure(engine="polars")
        io = EngineCheckIO.make(tmp_path, polars_model)

        # When / Then
        with pytest.raises(NotImplementedError, match="the polars engine does not read yet"):
            io.read()

    @pytest.mark.unit
    def test_a_non_model_schema_argument_is_still_a_type_error(self, tmp_path):
        with pytest.raises(TypeError, match="pandera DataFrameModel subclass"):
            EngineCheckIO.make(tmp_path, dict)

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "engine, model_fixture, expected", [(PandasEngine, None, True), (PolarsEngine, None, False), (PandasEngine, "polars_model", False), (PolarsEngine, "polars_model", True)]
    )
    def test_is_model_matches_the_backend_of_the_model(self, request, engine, model_fixture, expected):
        model = request.getfixturevalue(model_fixture) if model_fixture else Positive
        assert engine().is_model(model) is expected

    @pytest.mark.unit
    def test_the_backend_of_a_dynamicio_data_model_subclass_is_found_through_its_bases(self):
        pytest.importorskip("polars")
        from dynamicio.schema import DataModel  # pylint: disable=import-outside-toplevel
        from dynamicio.schema.polars import DataModel as PolarsDataModel  # pylint: disable=import-outside-toplevel

        class Base(PolarsDataModel):
            a: int

        class Child(Base, dataset="child"):
            b: int

        class PandasChild(DataModel, dataset="pandas_child"):
            a: int

        assert (ModelCompiler.backend_of(Child), ModelCompiler.backend_of(PandasChild)) == ("polars", "pandas")
        assert (Child.__dataset_name__, vars(Base).get("__dataset_name__")) == ("child", None)


class TestFrameCheck:
    @pytest.mark.unit
    @pytest.mark.parametrize("make_frame", [lambda pl: pl.DataFrame({"a": [1]}), lambda pl: pl.LazyFrame({"a": [1]}), lambda pl: pl.Series("a", [1])])
    def test_a_polars_frame_on_the_pandas_engine_is_refused_before_any_write(self, tmp_path, make_frame):
        pl = pytest.importorskip("polars")

        # Given
        io = EngineCheckIO.make(tmp_path)

        # When / Then
        with pytest.raises(WrongFrameError, match="on the pandas engine"):
            io.write(make_frame(pl))
        assert not (tmp_path / "data.csv").exists()

    @pytest.mark.unit
    def test_a_refused_frame_does_not_freeze_the_engine(self, tmp_path):
        pl = pytest.importorskip("polars")

        # Given
        io = EngineCheckIO.make(tmp_path)
        with pytest.raises(WrongFrameError):
            io.write(pl.DataFrame({"a": [1]}))

        # When
        dynamicio.configure(engine="polars")

        # Then
        assert isinstance(EngineSetting.current(), PolarsEngine)
        assert not EngineSetting._frozen  # pylint: disable=protected-access

    @pytest.mark.unit
    def test_a_pandas_frame_on_the_polars_engine_is_refused(self):
        with pytest.raises(WrongFrameError, match="pandas DataFrame on the polars engine"):
            PolarsEngine().check_frame(pd.DataFrame({"a": [1]}))

    @pytest.mark.unit
    @pytest.mark.parametrize("frame", [pd.DataFrame({"a": [1]}), pd.Series([1]), {"a": [1]}, None, 1])
    def test_anything_but_a_polars_object_passes_the_pandas_frame_check(self, frame):
        PandasEngine().check_frame(frame)

    @pytest.mark.unit
    def test_wrong_frame_error_is_a_type_error(self):
        assert issubclass(WrongFrameError, TypeError) and issubclass(ModelEngineMismatchError, TypeError)

    @pytest.mark.unit
    def test_is_frame_names_the_library_of_a_frame(self):
        pl = pytest.importorskip("polars")
        assert PandasEngine().is_frame(pd.DataFrame()) and not PandasEngine().is_frame(pl.DataFrame())
        assert PolarsEngine().is_frame(pl.LazyFrame()) and not PolarsEngine().is_frame(pd.DataFrame())

    @pytest.mark.unit
    def test_a_subclass_of_a_polars_frame_on_the_pandas_engine_is_refused_before_any_write(self, tmp_path):
        pl = pytest.importorskip("polars")

        # Given
        class AppFrame(pl.DataFrame):
            pass

        AppFrame.__module__ = "app.frames"
        io = EngineCheckIO.make(tmp_path)

        # When / Then
        with pytest.raises(WrongFrameError, match="got a polars AppFrame on the pandas engine"):
            io.write(AppFrame({"a": [1]}))
        assert not (tmp_path / "data.csv").exists()
        assert not EngineSetting._frozen  # pylint: disable=protected-access

    @pytest.mark.unit
    def test_a_subclass_of_a_pandas_frame_is_a_pandas_frame(self):
        # Given
        class AppFrame(pd.DataFrame):
            pass

        AppFrame.__module__ = "app.frames"

        # When / Then
        assert PandasEngine().is_frame(AppFrame({"a": [1]}))
        assert not PolarsEngine().is_frame(AppFrame({"a": [1]}))
        PandasEngine().check_frame(AppFrame({"a": [1]}))
        with pytest.raises(WrongFrameError, match="got a pandas AppFrame on the polars engine"):
            PolarsEngine().check_frame(AppFrame({"a": [1]}))

    @pytest.mark.unit
    def test_a_write_of_a_non_frame_fails_as_before(self, tmp_path):
        with pytest.raises(AttributeError):
            EngineCheckIO.make(tmp_path).write({"a": [1]})

    @pytest.mark.unit
    def test_a_pandas_process_does_not_import_polars(self, tmp_path):
        # Given a process that reads and writes on the pandas engine
        script = (
            "import sys, pandas as pd\n"
            "from tests.test_engines.ios import EngineCheckIO\n"
            "from pathlib import Path\n"
            f"io = EngineCheckIO.make(Path({str(tmp_path)!r}), apply_schema_validations=True, log_schema_metrics=True)\n"
            "io.write(pd.DataFrame({'a': [1, 2]}))\n"
            "io.read()\n"
            "print('polars' in sys.modules)\n"
        )

        # When
        result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, check=True, cwd=Path(__file__).parents[2])

        # Then
        assert result.stdout.strip().splitlines()[-1] == "False"
