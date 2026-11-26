"""The engine of the process: ``dynamicio.configure(engine=...)`` sets it, and the first read or write fixes it."""

import importlib.metadata
import importlib.util
import logging
import os
import re
import sys
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

    A process that ``fork`` makes from a process with the polars engine and polars imported cannot use the polars engine (change 24 of ADR 0010). The
    threads of polars do not go into the fork, so a lock that one of them held stays locked, and the next polars call can wait for ever.
    """

    ENGINES = {"pandas": PandasEngine, "polars": PolarsEngine}
    NAMES: Tuple[str, ...] = tuple(ENGINES)
    DEFAULT = "pandas"
    POLARS_MINIMUM = (2, 1)  # the minimum of the dynamicio[polars] extra

    _name: str = DEFAULT
    _frozen: bool = False
    _lock = threading.RLock()
    _instances: Dict[str, Engine] = {}
    _forked: bool = False
    FORK_MESSAGE = (
        "This process is a fork of a process that imported polars, and polars can deadlock in a fork. "
        'Start the processes with "spawn" or "forkserver", for example multiprocessing.get_context("spawn")'
    )

    @classmethod
    def configure(cls, engine: str) -> None:
        """Set the engine of the process. Call it once, at the start of the process, before the first read or write.

        Args:
            engine: ``"pandas"`` (the default) or ``"polars"``.

        Raises:
            EngineConfigurationError: If dynamicio has no engine of that name, or a read or write has already fixed another engine.
            ImportError: If the engine is polars and polars is not installed.
            EngineConfigurationError: If the engine is polars and the process is a fork of a process that imported polars.

        Examples:
            Call it in the package ``__init__.py`` of the repo, so that a spawned process also gets it:

            >>> import dynamicio
            >>> dynamicio.configure(engine="polars")
        """
        if engine not in cls.ENGINES:
            raise EngineConfigurationError(f"dynamicio has no engine {engine!r}: use one of {', '.join(cls.NAMES)}")
        with cls._lock:
            if cls._frozen and engine != cls._name:  # before the import check: installing polars cannot make this switch legal
                raise EngineConfigurationError(f"The engine is {cls._name}, fixed by the first read or write. Call dynamicio.configure(engine={engine!r}) before it.")
            cls._refuse_a_polars_fork(engine)
            if engine == "polars":
                cls.require_polars()
            cls._name = engine

    @classmethod
    def require_polars(cls) -> None:
        """Raise ``ImportError`` if polars is not installed or is older than 2.1. It reads the version of the distribution, and does not import polars.

        polars reads ``POLARS_MAX_THREADS`` when it is first imported, so ``configure()`` must not import it. A polars build of another distribution
        name, for example ``polars-lts-cpu``, has no ``polars`` distribution, and the check accepts it.
        """
        minimum = ".".join(str(part) for part in cls.POLARS_MINIMUM)
        if importlib.util.find_spec("polars") is None:
            raise ImportError(f'The polars engine needs polars {minimum} or later: pip install "dynamicio[polars]"')
        try:
            version = importlib.metadata.version("polars")
        except importlib.metadata.PackageNotFoundError:
            return
        if tuple(int(part) for part in re.findall(r"\d+", version)[: len(cls.POLARS_MINIMUM)]) < cls.POLARS_MINIMUM:
            raise ImportError(f'The polars engine needs polars {minimum} or later, and polars {version} is installed: pip install "dynamicio[polars]"')

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
    def use(cls, model: Optional[type] = None, frames: Tuple[Any, ...] = (), resource: Any = None, operation: Optional[str] = None) -> Engine:
        """The engine of a read or write. The first call that passes the checks freezes the engine and logs its name.

        Args:
            model: The Pandera model of the IO object, checked against the engine.
            frames: The frames that the IO object writes, checked against the engine.
            resource: The IO object, whose file type and options are checked against the engine.
            operation: ``"scan"`` or ``"sink"`` for a lazy operation on ``resource``, which the engine must have.

        Raises:
            ModelEngineMismatchError: If the model is not for the engine. The engine stays unfrozen, so ``configure()`` can still correct it.
            WrongFrameError: If a frame is of the other engine. The engine stays unfrozen.
            EngineResourceError: If the engine cannot read or write the resource. The engine stays unfrozen.
            NotImplementedError: If the engine has no ``operation`` for the resource. The engine stays unfrozen.
            EngineConfigurationError: If the engine is polars and the process is a fork of a process that imported polars.
        """
        with cls._lock:
            cls._refuse_a_polars_fork(cls._name)
            engine = cls.current()
            if model is not None:
                engine.check_model(model)
            for frame in frames:
                engine.check_frame(frame)
            if resource is not None:
                engine.check_resource(resource)
                if operation is not None:
                    engine.check_operation(resource, operation)
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
            cls._forked = False

    @classmethod
    def _refuse_a_polars_fork(cls, engine: str) -> None:
        if engine == "polars" and cls._forked:
            raise EngineConfigurationError(cls.FORK_MESSAGE)

    @classmethod
    def after_fork_in_child(cls) -> None:
        """Mark a fork of a process that imported polars, with any engine: the fork has the polars thread pool of the parent, so it cannot use polars.

        The fork can still use the pandas engine. The fork gets a new lock, because a thread of the parent can hold the old one.
        """
        cls._lock = threading.RLock()
        if "polars" in sys.modules:
            cls._forked = True


if hasattr(os, "register_at_fork"):  # Windows has no fork
    os.register_at_fork(after_in_child=EngineSetting.after_fork_in_child)
