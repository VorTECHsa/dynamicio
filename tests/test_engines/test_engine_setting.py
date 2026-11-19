# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import importlib.metadata
import importlib.util
import logging
import threading
import tomllib
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

import dynamicio
from dynamicio.engines import EngineSetting, PandasEngine, PolarsEngine
from dynamicio.errors import EngineConfigurationError
from tests.test_engines.ios import EngineCheckIO


class TestConfigure:
    @pytest.mark.unit
    def test_the_default_engine_is_pandas(self):
        # When
        engine = EngineSetting.current()

        # Then
        assert isinstance(engine, PandasEngine)

    @pytest.mark.unit
    def test_configure_sets_the_engine_before_the_first_use(self):
        pytest.importorskip("polars")

        # When
        dynamicio.configure(engine="polars")

        # Then
        assert isinstance(EngineSetting.use(), PolarsEngine)

    @pytest.mark.unit
    def test_a_later_configure_replaces_an_earlier_one_before_the_first_use(self):
        pytest.importorskip("polars")

        # Given
        dynamicio.configure(engine="polars")

        # When
        dynamicio.configure(engine="pandas")

        # Then
        assert isinstance(EngineSetting.use(), PandasEngine)

    @pytest.mark.unit
    def test_an_unknown_engine_is_refused(self):
        with pytest.raises(EngineConfigurationError, match="no engine 'arrow'"):
            dynamicio.configure(engine="arrow")

    @pytest.mark.unit
    def test_the_polars_engine_without_polars_raises_an_import_error(self):
        with patch("dynamicio.engines.engine_setting.importlib.util.find_spec", return_value=None):
            with pytest.raises(ImportError, match=r'needs polars 2.1 or later: pip install "dynamicio\[polars\]"'):
                dynamicio.configure(engine="polars")

        assert isinstance(EngineSetting.current(), PandasEngine)

    @pytest.mark.unit
    @pytest.mark.parametrize("version", ["1.31.0", "2.0.5", "0.20.31", "2.0.99rc1"])
    def test_a_polars_older_than_2_1_raises_an_import_error(self, version):
        with (
            patch("dynamicio.engines.engine_setting.importlib.util.find_spec", return_value=object()),
            patch("dynamicio.engines.engine_setting.importlib.metadata.version", return_value=version),
        ):
            with pytest.raises(ImportError, match=f"polars {version} is installed"):
                dynamicio.configure(engine="polars")

        assert isinstance(EngineSetting.current(), PandasEngine)

    @pytest.mark.unit
    @pytest.mark.skipif(importlib.util.find_spec("polars") is None, reason="the polars engine needs the polars extra")
    @pytest.mark.parametrize("version", ["2.1.0", "2.1.0rc1", "2.10.0", "3.0.0", "10.0"])
    def test_a_polars_of_2_1_or_later_is_accepted(self, version):
        with (
            patch("dynamicio.engines.engine_setting.importlib.util.find_spec", return_value=object()),
            patch("dynamicio.engines.engine_setting.importlib.metadata.version", return_value=version),
        ):
            dynamicio.configure(engine="polars")

        assert isinstance(EngineSetting.current(), PolarsEngine)

    @pytest.mark.unit
    @pytest.mark.skipif(importlib.util.find_spec("polars") is None, reason="the polars engine needs the polars extra")
    def test_a_polars_build_of_another_distribution_name_is_accepted(self):
        with (
            patch("dynamicio.engines.engine_setting.importlib.util.find_spec", return_value=object()),
            patch("dynamicio.engines.engine_setting.importlib.metadata.version", side_effect=importlib.metadata.PackageNotFoundError("polars")),
        ):
            dynamicio.configure(engine="polars")

        assert isinstance(EngineSetting.current(), PolarsEngine)

    @pytest.mark.unit
    def test_the_minimum_is_the_minimum_of_the_polars_extra(self):
        with open(Path(__file__).parents[2] / "pyproject.toml", "rb") as file:
            extra = tomllib.load(file)["project"]["optional-dependencies"]["polars"]

        assert extra == [f"polars>={'.'.join(str(part) for part in EngineSetting.POLARS_MINIMUM)}"]

    @pytest.mark.unit
    def test_a_switch_to_polars_after_the_first_use_without_polars_is_an_engine_configuration_error(self):
        # Given
        EngineSetting.use()

        # When / Then
        with patch("dynamicio.engines.engine_setting.importlib.util.find_spec", return_value=None) as find_spec:
            with pytest.raises(EngineConfigurationError, match="fixed by the first read or write"):
                dynamicio.configure(engine="polars")
        find_spec.assert_not_called()


class TestFreeze:
    @pytest.mark.unit
    def test_another_engine_after_the_first_use_is_refused(self):
        pytest.importorskip("polars")

        # Given
        EngineSetting.use()

        # When / Then
        with pytest.raises(EngineConfigurationError, match="fixed by the first read or write"):
            dynamicio.configure(engine="polars")
        assert isinstance(EngineSetting.current(), PandasEngine)

    @pytest.mark.unit
    def test_the_same_engine_after_the_first_use_is_accepted(self):
        # Given
        EngineSetting.use()

        # When
        dynamicio.configure(engine="pandas")

        # Then
        assert isinstance(EngineSetting.current(), PandasEngine)

    @pytest.mark.unit
    def test_the_first_use_logs_the_engine_once(self, caplog):
        # When
        with caplog.at_level(logging.INFO, logger="dynamicio.engines.engine_setting"):
            EngineSetting.use()
            EngineSetting.use()

        # Then
        assert [record.getMessage() for record in caplog.records if record.name == "dynamicio.engines.engine_setting"] == ["dynamicio engine: pandas"]

    @pytest.mark.unit
    def test_a_read_freezes_the_engine(self, tmp_path):
        pytest.importorskip("polars")

        # Given
        pd.DataFrame({"a": [1]}).to_csv(tmp_path / "data.csv", index=False)
        EngineCheckIO.make(tmp_path).read()

        # When / Then
        with pytest.raises(EngineConfigurationError):
            dynamicio.configure(engine="polars")

    @pytest.mark.unit
    def test_a_write_freezes_the_engine(self, tmp_path):
        pytest.importorskip("polars")

        # Given
        EngineCheckIO.make(tmp_path).write(pd.DataFrame({"a": [1]}))

        # When / Then
        with pytest.raises(EngineConfigurationError):
            dynamicio.configure(engine="polars")

    @pytest.mark.unit
    @pytest.mark.parametrize("operation", ["scan", "sink"])
    def test_a_refused_lazy_operation_on_pandas_does_not_freeze_the_engine(self, tmp_path, operation):
        pytest.importorskip("polars")

        # Given
        io = EngineCheckIO.make(tmp_path)

        # When
        with pytest.raises(NotImplementedError, match=rf'{operation}\(\) needs the polars engine: call dynamicio.configure\(engine="polars"\)'):
            if operation == "scan":
                io.scan()
            else:
                io.sink(pd.DataFrame({"a": [1]}))
        dynamicio.configure(engine="polars")

        # Then
        assert isinstance(EngineSetting.current(), PolarsEngine)

    @pytest.mark.unit
    @pytest.mark.parametrize("operation, source", [("scan", "postgres"), ("sink", "kafka")])
    def test_a_lazy_operation_that_polars_does_not_have_does_not_freeze_the_engine(self, monkeypatch, operation, source):
        pytest.importorskip("polars")

        # Given
        EngineSetting.reset("polars")
        monkeypatch.setattr(PolarsEngine, "check_resource", lambda self, io: None)
        monkeypatch.setattr(PolarsEngine, "source_of", staticmethod(lambda io: source))
        monkeypatch.setattr(PolarsEngine, "check_model", lambda self, model: None)

        # When
        with pytest.raises(NotImplementedError, match=f"the polars engine does not {operation} {source} resources yet"):
            EngineSetting.use(resource=object(), operation=operation)
        dynamicio.configure(engine="pandas")

        # Then
        assert isinstance(EngineSetting.current(), PandasEngine)

    @pytest.mark.unit
    def test_creating_an_io_object_does_not_freeze_the_engine(self, tmp_path):
        pytest.importorskip("polars")

        # Given
        EngineCheckIO.make(tmp_path)

        # When
        dynamicio.configure(engine="polars")

        # Then
        assert isinstance(EngineSetting.current(), PolarsEngine)

    @pytest.mark.unit
    def test_concurrent_first_uses_freeze_and_log_once(self, caplog):
        # Given
        EngineSetting._instances.clear()  # pylint: disable=protected-access
        barrier = threading.Barrier(8)
        engines = []

        def first_use():
            barrier.wait()
            engines.append(EngineSetting.use())

        # When
        with caplog.at_level(logging.INFO, logger="dynamicio.engines.engine_setting"):
            threads = [threading.Thread(target=first_use) for _ in range(8)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join()

        # Then
        assert len({id(engine) for engine in engines}) == 1
        assert len([record for record in caplog.records if record.name == "dynamicio.engines.engine_setting"]) == 1

    @pytest.mark.unit
    def test_an_engine_is_made_once(self):
        assert EngineSetting.current() is EngineSetting.use() is EngineSetting.instance("pandas")
