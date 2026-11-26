"""The base class of a dynamicio model: a Pandera ``DataFrameModel`` that also names its dataset and its table."""

from typing import Optional

import pandera.pandas as pa

from dynamicio.schema.models import ModelCompiler


class DataModel(pa.DataFrameModel):
    """A Pandera ``DataFrameModel`` whose class keywords name the dataset, so the class body holds only the columns::

        class Orders(DataModel, dataset="orders", table="orders_v2"):
            order_id: Series[int] = Field(nullable=False, unique=True)

    Every model used for I/O must name its ``dataset``; a model for a Postgres resource must also name its ``table``. A subclass does not inherit the
    names, because it declares another dataset; a base class that only shares columns names none. Whether a read or write validates or logs metrics is
    an argument of the IO call (``apply_schema_validations=True``), not part of the model.
    """

    def __init_subclass__(cls, dataset: Optional[str] = None, table: Optional[str] = None):
        """Name the dataset of the model.

        Args:
            dataset: The label of the dataset in logs and ``METRIC`` lines, for example ``"orders"`` (logged as ``ORDERS``). Sets ``__dataset_name__``.
            table: The exact name of the table in the database, ``schema.table`` for one outside the default schema. Only a model used with a Postgres
                resource needs it. Sets ``__table_name__``.
        """
        super().__init_subclass__()
        ModelCompiler.name_model(cls, dataset, table)
