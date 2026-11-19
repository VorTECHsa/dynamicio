"""The interface between ``DynamicDataIO`` and the dataframe library of the process."""

import abc
from typing import TYPE_CHECKING, Any, Tuple

from dynamicio.engines.registry import Registry
from dynamicio.errors import ModelEngineMismatchError, WrongFrameError
from dynamicio.schema.models import ModelCompiler

if TYPE_CHECKING:  # pragma: no cover
    from dynamicio.core import DynamicDataIO


class Engine(abc.ABC):
    """What ``DynamicDataIO`` asks of a dataframe library: read and write, scan and sink, cast, validate and log metrics.

    An engine has no state of its own. It takes the IO object as an argument, so the I/O code stays in the mixins and the cast in ``DynamicDataIO``.
    """

    name: str = ""
    # The top-level modules of the frames of the other engine. ``check_frame`` compares the module of a frame by name and imports nothing.
    FOREIGN_FRAME_MODULES: Tuple[str, ...] = ()

    @property
    @abc.abstractmethod
    def registry(self) -> Registry:
        """The validators and metrics of the engine."""

    def is_frame(self, frame: Any) -> bool:
        """Whether a frame is of the library of this engine."""
        return type(frame).__module__.split(".")[0] == self.name

    def check_frame(self, frame: Any) -> None:
        """Refuse a frame of the other engine, before ``write()`` selects its columns."""
        library = type(frame).__module__.split(".")[0]
        if library in self.FOREIGN_FRAME_MODULES:
            raise WrongFrameError(f"write() got a {library} {type(frame).__name__} on the {self.name} engine. Convert it, or configure the {library} engine.")

    def is_model(self, model: type) -> bool:
        """Whether a Pandera model is for the library of this engine."""
        return ModelCompiler.backend_of(model) == self.name

    def check_model(self, model: type) -> None:
        """Refuse a Pandera model of the other engine."""
        backend = ModelCompiler.backend_of(model)
        if backend != self.name:
            raise ModelEngineMismatchError(
                f"{model.__name__} is a pandera.{backend} model, and the engine is {self.name}. Use a pandera.{self.name} model, or configure the {backend} engine."
            )

    @staticmethod
    def source_of(io: "DynamicDataIO") -> str:
        """The name of the source of an IO object, as its mixin methods spell it (``_read_from_s3``)."""
        return io.sources_config.data_backend_type.value

    def read(self, io: "DynamicDataIO") -> Any:
        """Read the source of an IO object with the method of its mixin."""
        return getattr(io, f"_read_from_{self.source_of(io)}")()

    def write(self, io: "DynamicDataIO", frame: Any) -> None:
        """Write a frame to the source of an IO object with the method of its mixin."""
        getattr(io, f"_write_to_{self.source_of(io)}")(frame)

    @abc.abstractmethod
    def scan(self, io: "DynamicDataIO") -> Any:
        """Read the source of an IO object lazily."""

    @abc.abstractmethod
    def sink(self, io: "DynamicDataIO", frame: Any) -> None:
        """Write a lazy frame to the source of an IO object."""

    @abc.abstractmethod
    def select_columns(self, io: "DynamicDataIO", frame: Any) -> Any:
        """The columns of a frame that the schema of an IO object declares, on a frame that ``cast`` can change without a change to the caller's frame."""

    @abc.abstractmethod
    def cast(self, io: "DynamicDataIO", frame: Any) -> Any:
        """A frame with the dtypes of the schema of an IO object."""

    @abc.abstractmethod
    def validate(self, io: "DynamicDataIO", frame: Any) -> None:
        """Run the validations of the schema of an IO object, and raise ``SchemaValidationError`` for the validations that fail."""

    @abc.abstractmethod
    def log_metrics(self, io: "DynamicDataIO", frame: Any) -> None:
        """Log the metrics of the schema of an IO object."""
