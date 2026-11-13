"""What a dataset's model says about how its reads and writes are treated: validated, measured, warned about, type-checked.

Declared once on the model, so the call sites do not repeat it::

    class Orders(pa.DataFrameModel):
        __io__ = Behaviour(validate=True, log_metrics=True)

An argument of the IO call overrides the model for that call (``IO(config, schema=Orders, apply_schema_validations=False)``). Where neither says, the flag is off.
"""

from dataclasses import dataclass
from typing import Optional

ATTRIBUTE = "__io__"


@dataclass(frozen=True)
class Behaviour:
    """The switches of a dataset, each the default for the IO argument named beside it."""

    validate: bool = False  # apply_schema_validations: run the validations of the model on every read and write
    log_metrics: bool = False  # log_schema_metrics: log the metrics of the model as METRIC lines
    show_casting_warnings: bool = False  # show_casting_warnings: log a column whose dtype had to be cast
    strict_dtype_check: bool = False  # strict_dtype_check: inspect every row for mixed Python types in a column being cast (expensive)

    @staticmethod
    def of(model: type) -> "Behaviour":
        """The behaviour declared by ``model`` (a base model's declaration is inherited), or the all-off default."""
        declared = getattr(model, ATTRIBUTE, None)
        if declared is None:
            return Behaviour()
        if not isinstance(declared, Behaviour):
            raise TypeError(f"{model.__name__}.{ATTRIBUTE} must be a dynamicio.schema.Behaviour, got {declared!r}")
        return declared

    @staticmethod
    def resolve(argument: Optional[bool], declared: bool) -> bool:
        """The argument of the call if it was given, else what the model declares."""
        return declared if argument is None else argument
