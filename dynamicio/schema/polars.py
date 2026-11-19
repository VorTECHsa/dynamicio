"""The base class of a dynamicio model for the polars engine: a ``pandera.polars`` ``DataFrameModel`` that also names its dataset and its table.

Import it as ``from dynamicio.schema.polars import DataModel``. The module needs polars and ``pandera[polars]``, so ``dynamicio.schema`` does not import it.
"""

from typing import Optional

import pandera.polars as pa

from dynamicio.schema.models import ModelCompiler


class DataModel(pa.DataFrameModel):
    """A ``pandera.polars`` ``DataFrameModel`` whose class keywords name the dataset, the same as ``dynamicio.schema.DataModel`` for pandas::

        class Orders(DataModel, dataset="orders", table="orders_v2"):
            order_id: int = Field(nullable=False, unique=True)

    An IO object takes this model only when the engine of the process is polars (``dynamicio.configure(engine="polars")``).
    """

    def __init_subclass__(cls, dataset: Optional[str] = None, table: Optional[str] = None):
        """Name the dataset of the model.

        Args:
            dataset: The label of the dataset in logs and ``METRIC`` lines. Sets ``__dataset_name__``.
            table: The exact name of the table in the database. Only a model used with a Postgres resource needs it. Sets ``__table_name__``.
        """
        super().__init_subclass__()
        ModelCompiler.name_model(cls, dataset, table)
