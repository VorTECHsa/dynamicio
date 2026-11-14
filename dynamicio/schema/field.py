"""The column declaration of a dynamicio model: ``pa.Field`` with dynamicio's metrics and validations as named, documented arguments."""

from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandera.pandas as pa

from dynamicio.schema.metadata import METADATA_KEY, DynamicioMetadata, Metric, ValidationSpec


class FieldDeclaration:
    """Turns the dynamicio arguments of ``Field`` into the ``metadata`` of a ``pa.Field``."""

    @staticmethod
    def metrics(names: Iterable[Metric]) -> Tuple[Metric, ...]:
        """The metrics to log, in declaration order and without repeats; anything but a ``Metric`` member is refused."""
        names = tuple(names)
        for name in names:
            if not isinstance(name, Metric):
                raise TypeError(f"Field(metrics=...) takes members of dynamicio.schema.Metric (for example Metric.MIN), got {name!r}")
        return tuple(dict.fromkeys(names))

    @staticmethod
    def null_fraction_below(threshold: Any) -> ValidationSpec:
        """Validate that the fraction of nulls in the column is below ``threshold`` (0.2 allows up to, not including, 20% nulls)."""
        if not 0 < threshold < 1:
            raise ValueError(f"Field(max_null_fraction=...) takes a fraction between 0 and 1, exclusive, got {threshold!r}")
        return ValidationSpec("has_acceptable_percentage_of_nulls", {"threshold": threshold})

    @staticmethod
    def exactly(expected: Iterable[Any]) -> ValidationSpec:
        """Validate that the column holds exactly these values: none other, and none missing (``isin`` only forbids others)."""
        return ValidationSpec("is_in", {"categorical_values": set(expected), "match_all": False})

    @staticmethod
    def between(lower: Any, upper: Any, include_lower: bool, include_upper: bool) -> ValidationSpec:
        """Validate that the column lies between two bounds, which are not compared on definition (``pa.Field(in_range=...)`` does)."""
        return ValidationSpec("is_between", {"lower": lower, "upper": upper, "include_left": include_lower, "include_right": include_upper})

    @classmethod
    def metadata(  # pylint: disable=too-many-positional-arguments
        cls,
        metric_names: Iterable[Metric],
        max_null_fraction: Optional[float],
        exactly: Optional[Iterable[Any]],
        between: Optional[Tuple[Any, Any]],
        include_lower: bool,
        include_upper: bool,
    ) -> Optional[Dict[str, DynamicioMetadata]]:
        """The ``metadata`` of the field, or None when the column declares nothing for dynamicio."""
        validations: List[ValidationSpec] = []
        if max_null_fraction is not None:
            validations.append(cls.null_fraction_below(max_null_fraction))
        if exactly is not None:
            validations.append(cls.exactly(exactly))
        if between is not None:
            validations.append(cls.between(*between, include_lower, include_upper))
        declared = DynamicioMetadata(metrics=cls.metrics(metric_names), validations=tuple(validations))
        return {METADATA_KEY: declared} if declared.metrics or declared.validations else None


def Field(  # pylint: disable=invalid-name,too-many-arguments,too-many-locals
    *,
    nullable: bool,
    unique: bool = False,
    gt: Any = None,
    ge: Any = None,
    lt: Any = None,
    le: Any = None,
    isin: Optional[Iterable[Any]] = None,
    exactly: Optional[Iterable[Any]] = None,
    between: Optional[Tuple[Any, Any]] = None,
    include_lower: bool = False,
    include_upper: bool = False,
    max_null_fraction: Optional[float] = None,
    metrics: Iterable[Metric] = (),
    **pandera_options: Any,
) -> Any:
    """Declare a column: what its values must satisfy, and which metrics to log for it.

    Args:
        nullable: Whether the column may hold nulls. Required, so that it is always a decision (Pandera's own default is False).
        unique: The values must be unique.
        gt: Every value must be greater than this.
        ge: Every value must be greater than or equal to this.
        lt: Every value must be lower than this.
        le: Every value must be lower than or equal to this.
        isin: Every value must be one of these (it may use only some of them).
        exactly: The values must be exactly these: none other, and none missing.
        between: ``(lower, upper)``: every value must lie between them. The bounds are not compared on definition, so they may come from settings.
        include_lower: Whether a value equal to the lower bound of ``between`` is valid. Default: no.
        include_upper: Whether a value equal to the upper bound of ``between`` is valid. Default: no.
        max_null_fraction: A fraction between 0 and 1: the validation fails when this share or more of the column is null (0.2 allows up to, not including, 20%).
        metrics: ``Metric`` members to log for the column on reads and writes that log metrics: ``Metric.MIN``, ``MAX``, ``MEAN``, ``STD``, ``VARIANCE``,
            ``COUNTS``, ``UNIQUE_COUNTS``, ``COUNTS_PER_LABEL``.
        pandera_options: Any other ``pa.Field`` argument (``alias``, ``coerce``, ``str_matches``, ``description`` ...).
    """
    if "metadata" in pandera_options:
        raise TypeError("Field() declares dynamicio's metadata itself: use metrics=, exactly=, between= and max_null_fraction= instead of metadata=")
    if exactly is not None and isin is not None:
        raise TypeError("Field() takes either isin= (values may be missing) or exactly= (none may be missing), not both")
    metadata = FieldDeclaration.metadata(metrics, max_null_fraction, exactly, between, include_lower, include_upper)
    if metadata is not None:
        pandera_options["metadata"] = metadata
    named = {"gt": gt, "ge": ge, "lt": lt, "le": le, "isin": None if isin is None else list(isin)}
    given = {key: value for key, value in named.items() if value is not None}  # `gt=0` is a bound, so compare with None and not by truth
    return pa.Field(nullable=nullable, unique=unique, **given, **pandera_options)
