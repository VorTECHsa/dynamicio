"""The polars engine. It checks frames and models, and has no I/O, cast, validations or metrics yet."""

from typing import TYPE_CHECKING, Any, NoReturn

from dynamicio.engines.engine import Engine
from dynamicio.engines.registry import Registry

if TYPE_CHECKING:  # pragma: no cover
    from dynamicio.core import DynamicDataIO


class PolarsEngine(Engine):
    """The engine of polars frames. This module does not import polars, so dynamicio imports without it."""

    name = "polars"
    FOREIGN_FRAME_MODULES = ("pandas",)

    def __init__(self):
        """Start with an empty registry: the polars validators and metrics come with the polars cast."""
        self._registry = Registry({}, {})

    @property
    def registry(self) -> Registry:
        """The polars validators and metrics, none yet."""
        return self._registry

    def not_yet(self, operation: str) -> NoReturn:
        """Refuse an operation that the polars engine does not have yet."""
        raise NotImplementedError(f"the polars engine does not {operation} yet")

    def read(self, io: "DynamicDataIO") -> Any:
        """Refuse: no polars reads yet."""
        self.not_yet("read")

    def write(self, io: "DynamicDataIO", frame: Any) -> None:
        """Refuse: no polars writes yet."""
        self.not_yet("write")

    def scan(self, io: "DynamicDataIO") -> Any:
        """Refuse: no polars scans yet."""
        self.not_yet("scan")

    def sink(self, io: "DynamicDataIO", frame: Any) -> None:
        """Refuse: no polars sinks yet."""
        self.not_yet("sink")

    def select_columns(self, io: "DynamicDataIO", frame: Any) -> Any:
        """Refuse: no polars column selection yet."""
        self.not_yet("select columns")

    def cast(self, io: "DynamicDataIO", frame: Any) -> Any:
        """Refuse: no polars cast yet."""
        self.not_yet("cast")

    def validate(self, io: "DynamicDataIO", frame: Any) -> None:
        """Refuse: no polars validations yet."""
        self.not_yet("validate")

    def log_metrics(self, io: "DynamicDataIO", frame: Any) -> None:
        """Refuse: no polars metrics yet."""
        self.not_yet("log metrics")
