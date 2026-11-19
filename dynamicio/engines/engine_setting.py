"""The engine of the process: ``dynamicio.configure(engine=...)`` sets it, and the first read or write fixes it."""

import importlib.util
import logging
import threading
from typing import Any, Dict, Optional, Tuple

from dynamicio.engines.engine import Engine
from dynamicio.engines.pandas_engine import PandasEngine
from dynamicio.engines.polars_engine import PolarsEngine
from dynamicio.errors import EngineConfigurationError

logger = logging.getLogger(__name__)


class EngineSetting:
    """The one engine of the process.

    The default is pandas. ``configure`` changes it until the first read or write, which freezes it, so that all resources of a process take and give
    frames of one library. After that, ``configure`` with the same engine does nothing, and with another engine raises ``EngineConfigurationError``.
    """

    ENGINES = {"pandas": PandasEngine, "polars": PolarsEngine}
    NAMES: Tuple[str, ...] = tuple(ENGINES)
    DEFAULT = "pandas"

    _name: str = DEFAULT
    _frozen: bool = False
    _lock = threading.RLock()
    _instances: Dict[str, Engine] = {}

    @classmethod
    def configure(cls, engine: str) -> None:
        """Set the engine of the process. Call it once, at the start of the process, before the first read or write.

        Args:
            engine: ``"pandas"`` (the default) or ``"polars"``.

        Raises:
            EngineConfigurationError: If dynamicio has no engine of that name, or a read or write has already fixed another engine.
            ImportError: If the engine is polars and polars is not installed.
        """
        if engine not in cls.ENGINES:
            raise EngineConfigurationError(f"dynamicio has no engine {engine!r}: use one of {', '.join(cls.NAMES)}")
        if engine == "polars" and importlib.util.find_spec("polars") is None:
            raise ImportError('The polars engine needs polars: pip install "polars>=2.1"')
        with cls._lock:
            if cls._frozen and engine != cls._name:
                raise EngineConfigurationError(f"The engine is {cls._name}, fixed by the first read or write. Call dynamicio.configure(engine={engine!r}) before it.")
            cls._name = engine

    @classmethod
    def instance(cls, name: str) -> Engine:
        """The engine object of a name, made once."""
        with cls._lock:
            if name not in cls._instances:
                cls._instances[name] = cls.ENGINES[name]()
            return cls._instances[name]

    @classmethod
    def current(cls) -> Engine:
        """The engine of the process, without a freeze: for checks before the first read or write."""
        return cls.instance(cls._name)

    @classmethod
    def use(cls, model: Optional[type] = None, frames: Tuple[Any, ...] = ()) -> Engine:
        """The engine of a read or write. The first call that passes the checks freezes the engine and logs its name.

        Args:
            model: The Pandera model of the IO object, checked against the engine.
            frames: The frames that the IO object writes, checked against the engine.

        Raises:
            ModelEngineMismatchError: If the model is not for the engine. The engine stays unfrozen, so ``configure()`` can still correct it.
            WrongFrameError: If a frame is of the other engine. The engine stays unfrozen.
        """
        with cls._lock:
            engine = cls.current()
            if model is not None:
                engine.check_model(model)
            for frame in frames:
                engine.check_frame(frame)
            if not cls._frozen:
                cls._frozen = True
                logger.info(f"dynamicio engine: {cls._name}")
            return engine

    @classmethod
    def reset(cls, engine: Optional[str] = None) -> None:
        """Unfreeze and set the engine, the default if no engine is given. Only for tests: a process has one engine."""
        with cls._lock:
            cls._name = engine or cls.DEFAULT
            cls._frozen = False
