"""The validators and metrics of one engine, by name."""

from typing import Any, Callable, Mapping


class Registry:
    """The validators and metrics of one engine, by the names a schema uses.

    The pandas engine holds the live dicts of ``dynamicio.schema.validations`` and ``dynamicio.schema.metrics``, so a validator or metric that a
    consumer registers with ``@validator`` or a ``Metric`` subclass is in its registry too.
    """

    def __init__(self, validators: Mapping[str, Callable[..., Any]], metrics: Mapping[str, type]):
        """Hold the validators and metrics of an engine.

        Args:
            validators: Validator name -> validator function.
            metrics: Metric name -> ``Metric`` class.
        """
        self.validators = validators
        self.metrics = metrics

    def validator(self, name: str) -> Callable[..., Any]:
        """The validator of a name, a ``KeyError`` for a name the engine has no validator for."""
        return self.validators[name]

    def metric(self, name: str) -> type:
        """The metric of a name, a ``KeyError`` for a name the engine has no metric for."""
        return self.metrics[name]
