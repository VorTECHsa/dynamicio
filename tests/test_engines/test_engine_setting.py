# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring
import logging
import threading
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
            with pytest.raises(ImportError, match="polars>=2.1"):
                dynamicio.configure(engine="polars")

        assert isinstance(EngineSetting.current(), PandasEngine)

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
