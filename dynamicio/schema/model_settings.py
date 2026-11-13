"""What a model declares about its dataset next to its columns: the names of the dataset and of its table, and how reads and writes are treated.

The settings live on the model class as dunder attributes, because Pandera reserves public attributes for columns. ``DataModel`` sets them from
class keywords (``class Orders(DataModel, dataset="orders", validate=True)``); a plain Pandera model sets the dunders by hand.
"""

from typing import Any, Dict, Optional

from dynamicio.errors import DatasetNameNotFoundError


class ModelSettings:
    """Reads, validates and writes the dynamicio settings of a Pandera model."""

    NAMES = {"dataset": "__dataset_name__", "table": "__table_name__"}
    FLAGS = {
        "validate": "__validate__",  # apply_schema_validations: run the validations of the model on every read and write
        "log_metrics": "__log_metrics__",  # log_schema_metrics: log the metrics of the model as METRIC lines
        "show_casting_warnings": "__show_casting_warnings__",  # show_casting_warnings: log a column whose dtype had to be cast
        "strict_dtype_check": "__strict_dtype_check__",  # strict_dtype_check: inspect every row for mixed Python types in a column being cast (expensive)
    }

    @classmethod
    def declare(cls, model: type, **settings: Any) -> None:
        """Set the settings given (those left as None are not declared) on ``model``."""
        for key, value in settings.items():
            if value is None:
                continue
            if key in cls.NAMES:
                cls._check_name(model, cls.NAMES[key], value)
                setattr(model, cls.NAMES[key], value)
            else:
                if not isinstance(value, bool):
                    raise TypeError(f"{model.__name__}: {key} must be True or False, got {value!r}")
                setattr(model, cls.FLAGS[key], value)

    @staticmethod
    def _check_name(model: type, attribute: str, value: Any) -> None:
        if not (isinstance(value, str) and value.strip()):
            raise ValueError(f"{model.__name__}.{attribute} must be a non-empty string, got {value!r}")

    @classmethod
    def dataset_name(cls, model: type) -> str:
        """The label of the dataset in logs and ``METRIC`` lines, as the model declares it. It is not inherited and never guessed from the class name."""
        declared = vars(model).get(cls.NAMES["dataset"])
        if declared is None:
            raise DatasetNameNotFoundError(model.__name__)
        cls._check_name(model, cls.NAMES["dataset"], declared)
        return declared

    @classmethod
    def table_name(cls, model: type) -> Optional[str]:
        """The Postgres table of the model, exactly as the database spells it; None for a model without one."""
        declared = vars(model).get(cls.NAMES["table"])
        if declared is not None:
            cls._check_name(model, cls.NAMES["table"], declared)
        return declared

    @classmethod
    def flags(cls, model: type) -> Dict[str, bool]:
        """The switches the model declares (a base model's declaration is inherited); a switch nobody declared is off."""
        flags = {key: getattr(model, attribute, False) for key, attribute in cls.FLAGS.items()}
        for key, value in flags.items():
            if not isinstance(value, bool):
                raise TypeError(f"{model.__name__}.{cls.FLAGS[key]} must be True or False, got {value!r}")
        return flags

    @staticmethod
    def resolve(argument: Optional[bool], declared: bool) -> bool:
        """The argument of the IO call if it was given, else what the model declares."""
        return declared if argument is None else argument
