"""The dtype map between pandas and polars (ADR 0010, decision 14).

The map is many to one: ``int64`` and ``Int64`` both become ``Int64``, and ``object`` and ``string`` both become ``String``. A ``List`` of a scalar dtype
of the map pairs with ``object``, the pandas dtype of a column of lists, but ``object`` alone gives ``String``. A polars model keeps the pandas
dtype of a column in the ``pandas_dtype`` key of its field metadata (``Field(nullable=False, pandas_dtype="Int64")``). A column without it takes the dtype of
the fixed reverse map. The module needs polars, so only the polars paths of dynamicio import it.
"""

import re
from typing import Any, Dict, Optional

import polars as pl

from dynamicio.schema.metadata import PANDAS_DTYPE_KEY


class DtypeMap:
    """The pairs of pandas and polars dtypes, in both directions. A dtype without a pair raises ``ValueError``."""

    METADATA_KEY = PANDAS_DTYPE_KEY

    INTEGERS = {
        "int8": pl.Int8,
        "int16": pl.Int16,
        "int32": pl.Int32,
        "int64": pl.Int64,
        "uint8": pl.UInt8,
        "uint16": pl.UInt16,
        "uint32": pl.UInt32,
        "uint64": pl.UInt64,
    }
    FLOATS = {"float32": pl.Float32, "float64": pl.Float64}
    STRINGS = ("object", "str", "string", "string[python]", "string[pyarrow]")
    SIMPLE = {"bool": pl.Boolean, "boolean": pl.Boolean, "category": pl.Categorical, **{name: pl.String for name in STRINGS}}
    # polars has no second unit: a pandas `datetime64[s]` or `timedelta64[s]` has no pair
    TIME_UNITS = ("ms", "us", "ns")
    TEMPORAL = re.compile(r"^(?P<kind>datetime64|timedelta64)\[(?P<unit>\w+)(?:,\s*(?P<tz>[^\]]+))?\]$")
    # A pandas column of lists has the dtype `object`, as `pd.read_parquet` gives a `list<...>` column
    LIST = "object"

    @classmethod
    def polars_of(cls, pandas_dtype: str) -> pl.DataType:
        """The polars dtype of a pandas dtype name.

        Args:
            pandas_dtype: A pandas dtype name, for example ``"Int64"``, ``"object"`` or ``"datetime64[ns, UTC]"``.

        Returns:
            The polars dtype. A ``category`` gives ``Categorical``: the values of an ``Enum`` come from the polars model only.

        Raises:
            ValueError: The pandas dtype has no polars pair.
        """
        name = pandas_dtype.strip()
        # `int64` and `Int64`: the nullable integer is the same polars dtype
        simple = cls.INTEGERS.get(name.lower()) or cls.FLOATS.get(name) or cls.SIMPLE.get(name)
        if simple is not None:
            return simple()
        match = cls.TEMPORAL.match(name)
        if match is not None and match["unit"] in cls.TIME_UNITS and not (match["kind"] == "timedelta64" and match["tz"]):
            return pl.Datetime(match["unit"], match["tz"]) if match["kind"] == "datetime64" else pl.Duration(match["unit"])
        raise ValueError(f"the pandas dtype {pandas_dtype!r} has no polars dtype in the dtype map of dynamicio")

    @classmethod
    def pandas_of(cls, polars_dtype: pl.DataType, has_nulls: bool = False) -> str:
        """The pandas dtype name of a polars dtype, by the fixed reverse map.

        Args:
            polars_dtype: A polars dtype.
            has_nulls: Whether the column holds a null. An integer or a boolean column with a null takes the nullable pandas dtype
                (``Int64``, ``boolean``), because ``int64`` and ``bool`` cannot hold one.

        Returns:
            The pandas dtype name: ``String`` gives ``object``, ``Int64`` gives ``int64`` (``Int64`` with nulls), ``Enum`` gives ``category``.

        Raises:
            ValueError: The polars dtype has no pandas pair, for example ``Date``, ``Decimal`` or a ``List`` of ``Categorical``.
        """
        name = cls.reverse_name(polars_dtype)
        if name is None:
            raise ValueError(f"the polars dtype {polars_dtype} has no pandas dtype in the dtype map of dynamicio")
        return cls.holding_nulls(name) if has_nulls else name

    @classmethod
    def holding_nulls(cls, pandas_dtype: str) -> str:
        """The pandas dtype of a column with a null: the nullable dtype of a numpy integer or ``bool`` (``Int64``, ``UInt8``, ``boolean``), else the dtype.

        ``int64`` and ``bool`` cannot hold a null. Their pandas metadata over a null makes a pandas reader give ``float64``, which loses the digits of an
        integer above 2**53, or ``object``.
        """
        if pandas_dtype in cls.INTEGERS:
            return f"UInt{pandas_dtype[4:]}" if pandas_dtype.startswith("uint") else f"Int{pandas_dtype[3:]}"
        return "boolean" if pandas_dtype == "bool" else pandas_dtype

    @classmethod
    def reverse_name(cls, polars_dtype: pl.DataType) -> Optional[str]:
        """The pandas dtype name of a polars dtype for a column without nulls, or None when the dtype has no pair.

        A ``List`` pairs with ``object`` when its items have a dtype of the map other than ``Categorical`` and ``Enum``. A ``List`` of lists has no pair.
        """
        if isinstance(polars_dtype, pl.List):
            return cls.LIST if cls.is_list_item(polars_dtype.inner) else None
        for name, dtype in {**cls.INTEGERS, **cls.FLOATS, "bool": pl.Boolean, "object": pl.String}.items():
            if polars_dtype == dtype:
                return name
        if isinstance(polars_dtype, (pl.Categorical, pl.Enum)):
            return "category"
        if isinstance(polars_dtype, pl.Datetime):
            zone = f", {polars_dtype.time_zone}" if polars_dtype.time_zone else ""
            return f"datetime64[{polars_dtype.time_unit}{zone}]"
        if isinstance(polars_dtype, pl.Duration):
            return f"timedelta64[{polars_dtype.time_unit}]"
        return None

    @classmethod
    def is_list_item(cls, polars_dtype: pl.DataType) -> bool:
        """Whether a polars dtype can be the item dtype of a ``List`` column: a scalar dtype of the map other than ``Categorical`` and ``Enum``."""
        return not isinstance(polars_dtype, (pl.List, pl.Categorical, pl.Enum)) and cls.reverse_name(polars_dtype) is not None

    @classmethod
    def pairs(cls, pandas_dtype: str, polars_dtype: pl.DataType) -> bool:
        """Whether a pandas dtype name is a pair of a polars dtype. Any ``category`` pairs with any ``Categorical`` or ``Enum``, and ``object`` with a ``List``."""
        if isinstance(polars_dtype, pl.List):
            return pandas_dtype.strip() == cls.LIST and cls.reverse_name(polars_dtype) is not None
        try:
            mapped = cls.polars_of(pandas_dtype)
        except ValueError:
            return False
        if isinstance(mapped, pl.Categorical):
            return isinstance(polars_dtype, (pl.Categorical, pl.Enum))
        return bool(mapped == polars_dtype) and getattr(mapped, "time_zone", None) == getattr(polars_dtype, "time_zone", None)

    @classmethod
    def pandas_dtype_of_column(cls, model: type, column: Any) -> str:
        """The pandas dtype of a column of a polars model: its ``pandas_dtype`` metadata, or the reverse map for a column without nulls.

        Args:
            model: The polars model, for the error message.
            column: A ``pandera.polars`` column of the model.

        Returns:
            A pandas dtype name.

        Raises:
            ValueError: The polars dtype has no pandas pair, or the ``pandas_dtype`` metadata is not a pair of it.
        """
        polars_dtype = column.dtype.type
        declared: Optional[str] = (column.metadata or {}).get(cls.METADATA_KEY)
        if declared is None:
            try:
                return cls.pandas_of(polars_dtype)
            except ValueError as error:
                raise ValueError(
                    f"{model.__name__}.{column.name}: {error}. Declare a dtype of the map: an integer, a float, Boolean, String, Categorical, Enum, Datetime, Duration or a List of one of these "
                    "other than Categorical and Enum"
                ) from None
        if not isinstance(declared, str) or not cls.pairs(declared, polars_dtype):
            raise ValueError(f"{model.__name__}.{column.name}: pandas_dtype={declared!r} is not a pair of the polars dtype {polars_dtype}")
        return declared

    @staticmethod
    def of_model(model: type) -> Dict[str, pl.DataType]:
        """The polars dtype of each column of a polars model, by column name."""
        return {name: column.dtype.type for name, column in model.to_schema().columns.items()}
