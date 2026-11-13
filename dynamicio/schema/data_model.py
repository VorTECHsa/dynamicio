"""The base class of a dynamicio model: a Pandera ``DataFrameModel`` that also says what its dataset is called and how it is treated."""

from typing import Any, Optional

import pandera.pandas as pa

from dynamicio.schema.model_settings import ModelSettings


class DataModel(pa.DataFrameModel):
    """A Pandera ``DataFrameModel`` whose class keywords declare the dataset, so the class body holds only the columns::

        class Orders(DataModel, dataset="orders", validate=True, log_metrics=True):
            order_id: Series[int] = Field(nullable=False, unique=True)

    Every model must name its ``dataset``; a model for a Postgres resource must also name its ``table``. The switches are the defaults of the IO
    arguments of the same purpose, and an argument of the IO call overrides them. A subclass inherits the switches of its base, never the names.
    """

    def __init_subclass__(  # pylint: disable=too-many-positional-arguments
        cls,
        dataset: Optional[str] = None,
        table: Optional[str] = None,
        validate: Optional[bool] = None,
        log_metrics: Optional[bool] = None,
        show_casting_warnings: Optional[bool] = None,
        strict_dtype_check: Optional[bool] = None,
        **options: Any,
    ):
        """Declare the dataset of the model.

        Args:
            dataset: The label of the dataset in logs and ``METRIC`` lines, for example ``"orders"`` (logged as ``ORDERS``).
            table: The exact name of the table in the database. Only a model used with a Postgres resource needs it.
            validate: Run the validations of the model on every read and write (the default of ``apply_schema_validations``). Default: off.
            log_metrics: Log the metrics of the model on every read and write (the default of ``log_schema_metrics``). Default: off.
            show_casting_warnings: Log each column whose dtype had to be cast (the default of ``show_casting_warnings``). Default: off.
            strict_dtype_check: Inspect every row of a column being cast for mixed Python types, which is expensive (the default of ``strict_dtype_check``). Default: off.
            options: Anything else Pandera's ``DataFrameModel`` takes.
        """
        super().__init_subclass__(**options)
        ModelSettings.declare(
            cls, dataset=dataset, table=table, validate=validate, log_metrics=log_metrics, show_casting_warnings=show_casting_warnings, strict_dtype_check=strict_dtype_check
        )
