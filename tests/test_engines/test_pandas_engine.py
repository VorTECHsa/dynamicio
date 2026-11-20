# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import asyncio
import json
from unittest.mock import patch

import pandas as pd
import pytest
import yaml

from dynamicio.config import IOConfig
from dynamicio.core import DynamicDataIO
from dynamicio.engines import Engine, EngineSetting, PandasEngine, PolarsEngine, Registry
from dynamicio.errors import ColumnsDataTypeError, SchemaValidationError
from dynamicio.schema import metrics, validations
from tests.test_engines.ios import EngineCheckIO


class TestRegistry:
    @pytest.mark.unit
    def test_the_pandas_registry_holds_the_live_registries_of_dynamicio_schema(self):
        registry = PandasEngine().registry
        assert registry.validators is validations.ALL_VALIDATORS
        assert registry.metrics is metrics.__metrics__

    @pytest.mark.unit
    def test_a_validator_registered_after_the_engine_was_made_is_found(self):
        # Given
        engine = EngineSetting.current()

        # When
        def is_engine_registry_probe(dataset, df, column):  # pylint: disable=unused-argument
            return validations.ValidationResult(valid=True, message="", value=0)

        validations.validator(is_engine_registry_probe)
        try:
            # Then
            assert engine.registry.validator("is_engine_registry_probe") is is_engine_registry_probe
        finally:
            del validations.ALL_VALIDATORS["is_engine_registry_probe"]

    @pytest.mark.unit
    def test_an_unknown_name_is_a_key_error(self):
        with pytest.raises(KeyError):
            Registry({}, {}).validator("missing")
        with pytest.raises(KeyError):
            Registry({}, {}).metric("missing")

    @pytest.mark.unit
    def test_the_polars_registry_has_the_names_of_the_pandas_registry(self):
        pytest.importorskip("polars")

        # Given
        pandas_registry, polars_registry = PandasEngine().registry, PolarsEngine().registry

        # Then
        assert set(polars_registry.metrics) == set(pandas_registry.metrics)
        assert set(polars_registry.validators) == {name for name in pandas_registry.validators if not name.startswith("_")}


class TestEngineInterface:
    @pytest.mark.unit
    def test_an_engine_without_all_members_cannot_be_made(self):
        class Partial(Engine):  # pylint: disable=abstract-method
            name = "partial"

        with pytest.raises(TypeError, match="abstract"):
            Partial()  # pylint: disable=abstract-class-instantiated

    @pytest.mark.unit
    @pytest.mark.parametrize("engine", [PandasEngine, PolarsEngine])
    def test_both_engines_have_every_member(self, engine):
        assert not engine.__abstractmethods__


class TestLazyOnPandas:
    @pytest.mark.unit
    def test_scan_names_the_polars_engine(self, tmp_path):
        with pytest.raises(NotImplementedError, match=r'scan\(\) needs the polars engine: call dynamicio.configure\(engine="polars"\)'):
            EngineCheckIO.make(tmp_path).scan()

    @pytest.mark.unit
    def test_sink_names_the_polars_engine(self, tmp_path):
        with pytest.raises(NotImplementedError, match=r"sink\(\) needs the polars engine"):
            EngineCheckIO.make(tmp_path).sink(pd.DataFrame({"a": [1]}))

    @pytest.mark.unit
    @pytest.mark.parametrize("operation", ["scan", "sink"])
    def test_the_lazy_members_of_the_engine_name_the_polars_engine(self, tmp_path, operation):
        # Given
        io, engine = EngineCheckIO.make(tmp_path), PandasEngine()
        arguments = (io,) if operation == "scan" else (io, pd.DataFrame({"a": [1]}))

        # When / Then
        with pytest.raises(NotImplementedError, match=rf"{operation}\(\) needs the polars engine"):
            getattr(engine, operation)(*arguments)

    @pytest.mark.unit
    @pytest.mark.parametrize("operation", [None, "read", "write"])
    def test_an_eager_operation_passes_the_check(self, tmp_path, operation):
        # When / Then
        assert PandasEngine().check_operation(EngineCheckIO.make(tmp_path), operation) is None

    @pytest.mark.unit
    def test_sink_of_a_polars_lazy_frame_on_pandas_is_a_wrong_frame_error(self, tmp_path):
        pl = pytest.importorskip("polars")
        from dynamicio.errors import WrongFrameError  # pylint: disable=import-outside-toplevel

        with pytest.raises(WrongFrameError):
            EngineCheckIO.make(tmp_path).sink(pl.LazyFrame({"a": [1]}))


class TestPandasLifecycle:
    @pytest.mark.unit
    def test_select_columns_drops_undeclared_columns_and_keeps_the_caller_frame(self, tmp_path):
        # Given
        io = EngineCheckIO.make(tmp_path)
        frame = pd.DataFrame({"a": ["1"], "extra": [2]})

        # When
        selected = PandasEngine().select_columns(io, frame)
        PandasEngine().cast(io, selected)

        # Then
        assert list(selected.columns) == ["a"]
        assert frame["a"].tolist() == ["1"] and list(frame.columns) == ["a", "extra"]

    @pytest.mark.unit
    def test_a_write_casts_a_copy_and_not_the_caller_frame(self, tmp_path):
        # Given
        frame = pd.DataFrame({"a": ["1", "2"]})

        # When
        EngineCheckIO.make(tmp_path).write(frame)

        # Then
        assert frame["a"].tolist() == ["1", "2"]
        assert pd.read_csv(tmp_path / "data.csv")["a"].tolist() == [1, 2]

    @pytest.mark.unit
    def test_a_column_that_does_not_cast_is_a_columns_data_type_error(self, tmp_path):
        with pytest.raises(ColumnsDataTypeError):
            PandasEngine().cast(EngineCheckIO.make(tmp_path), pd.DataFrame({"a": ["x"]}))

    @pytest.mark.unit
    @pytest.mark.parametrize("records", [[{"b": 1}], []], ids=["without-the-column", "without-records"])
    def test_a_json_read_without_a_schema_column_is_a_columns_data_type_error(self, tmp_path, records):
        # Given
        (tmp_path / "data.json").write_text(json.dumps(records))
        definition = {"X": {"LOCAL": {"type": "local", "local": {"file_path": str(tmp_path / "data.json"), "file_type": "json", "options": {"orient": "records"}}}}}
        (tmp_path / "definitions.yaml").write_text(yaml.safe_dump(definition))
        source_config = IOConfig(str(tmp_path / "definitions.yaml"), env_identifier="LOCAL", dynamic_vars=EngineCheckIO).get(source_key="X")

        # When / Then
        with pytest.raises(ColumnsDataTypeError):
            EngineCheckIO.make(tmp_path, source_config=source_config).read()

    @pytest.mark.unit
    def test_validate_raises_for_a_failing_validation(self, tmp_path):
        with pytest.raises(SchemaValidationError) as error:
            PandasEngine().validate(EngineCheckIO.make(tmp_path), pd.DataFrame({"a": [0, 1]}))
        assert "is_greater_than" in error.value.message

    @pytest.mark.unit
    def test_validate_passes_a_valid_frame(self, tmp_path):
        PandasEngine().validate(EngineCheckIO.make(tmp_path), pd.DataFrame({"a": [1, 2]}))

    @pytest.mark.unit
    def test_log_metrics_logs_the_declared_metrics(self, tmp_path, caplog):
        with caplog.at_level("INFO"):
            PandasEngine().log_metrics(EngineCheckIO.make(tmp_path), pd.DataFrame({"a": [3, 2]}))
        assert any('"metric": "Min"' in record.getMessage() and '"value": 2.0' in record.getMessage() for record in caplog.records)

    @pytest.mark.unit
    def test_io_methods_delegate_to_the_engine_and_return_self(self, tmp_path):
        io = EngineCheckIO.make(tmp_path)
        frame = pd.DataFrame({"a": [1]})
        assert io.validate_from_schema(frame) is io and io.log_metrics_from_schema(frame) is io
        assert io._apply_schema(frame) is frame  # pylint: disable=protected-access


class TestLifecycleOrder:
    @pytest.mark.unit
    def test_a_read_casts_validates_and_logs_metrics_through_the_io_methods(self, tmp_path):
        # Given
        pd.DataFrame({"a": [1, 2]}).to_csv(tmp_path / "data.csv", index=False)
        io = EngineCheckIO.make(tmp_path, apply_schema_validations=True, log_schema_metrics=True)

        # When
        with (
            patch.object(DynamicDataIO, "_apply_schema", side_effect=lambda df: df) as cast,
            patch.object(DynamicDataIO, "validate_from_schema", return_value=io) as validate,
            patch.object(DynamicDataIO, "log_metrics_from_schema", return_value=io) as log_metrics,
        ):
            io.read()

        # Then
        cast.assert_called_once()
        validate.assert_called_once()
        log_metrics.assert_called_once()

    @pytest.mark.unit
    def test_a_write_casts_validates_and_logs_metrics_through_the_io_methods(self, tmp_path):
        # Given
        io = EngineCheckIO.make(tmp_path, apply_schema_validations=True, log_schema_metrics=True)
        calls = []

        # When
        with (
            patch.object(DynamicDataIO, "_apply_schema", side_effect=lambda df: calls.append("cast") or df),
            patch.object(DynamicDataIO, "validate_from_schema", side_effect=lambda df: calls.append("validate") or io),
            patch.object(DynamicDataIO, "log_metrics_from_schema", side_effect=lambda df: calls.append("metrics") or io),
        ):
            io.write(pd.DataFrame({"a": [1, 2]}))

        # Then
        assert calls == ["cast", "validate", "metrics"]
        assert (tmp_path / "data.csv").exists()

    @pytest.mark.unit
    def test_the_validations_and_the_metrics_of_a_write_see_the_cast_frame(self, tmp_path):
        # Given
        io = EngineCheckIO.make(tmp_path, apply_schema_validations=True, log_schema_metrics=True)
        seen = []

        # When
        with (
            patch.object(DynamicDataIO, "validate_from_schema", side_effect=lambda df: seen.append(("validate", df.columns.to_list(), str(df["a"].dtype))) or io),
            patch.object(DynamicDataIO, "log_metrics_from_schema", side_effect=lambda df: seen.append(("metrics", df.columns.to_list(), str(df["a"].dtype))) or io),
        ):
            io.write(pd.DataFrame({"extra": ["x", "y"], "a": [1.0, 2.0]}))

        # Then
        assert seen == [("validate", ["a"], "int64"), ("metrics", ["a"], "int64")]

    @pytest.mark.unit
    def test_a_write_that_fails_its_validations_writes_no_file(self, tmp_path):
        # Given
        io = EngineCheckIO.make(tmp_path, apply_schema_validations=True)

        # When / Then
        with pytest.raises(SchemaValidationError):
            io.write(pd.DataFrame({"a": [1, -1]}))
        assert not (tmp_path / "data.csv").exists()

    @pytest.mark.unit
    def test_the_cast_messages_go_to_the_core_logger(self, tmp_path):
        # Given
        pd.DataFrame({"a": [1.0, 2.0]}).to_csv(tmp_path / "data.csv", index=False)
        io = EngineCheckIO.make(tmp_path, show_casting_warnings=True)

        # When
        with patch("dynamicio.core.logger") as logger:
            io.read()

        # Then
        assert any("Expected: 'int64' dtype for" in str(call) and "found 'float64'" in str(call) for call in logger.info.call_args_list)


class TestAsyncPaths:
    @pytest.mark.unit
    def test_async_write_and_read_run_on_the_engine_and_freeze_it(self, tmp_path):
        # Given
        io = EngineCheckIO.make(tmp_path, apply_schema_validations=True)

        async def round_trip():
            await io.async_write(pd.DataFrame({"a": [1, 2]}))
            return await io.async_read()

        # When
        result = asyncio.run(round_trip())

        # Then
        pd.testing.assert_frame_equal(result, pd.DataFrame({"a": [1, 2]}))
        assert EngineSetting._frozen  # pylint: disable=protected-access
