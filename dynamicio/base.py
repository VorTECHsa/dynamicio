# pylint: disable=no-name-in-module disable=invalid-name

"""BaseResource class for creating various resources types."""
from abc import ABC, abstractmethod
from typing import Optional, Type, TypeVar

import pandas as pd
import pandera as pa
from pandera import SchemaModel
from pydantic import BaseModel

SchemaType = TypeVar("SchemaType", bound=pa.SchemaModel)  # Todo utilise this


class BaseResource(BaseModel, ABC):
    """BaseClass for resource classes."""

    pa_schema: Optional[pa.SchemaModel]
    validate_default: bool = True
    log_metrics_default: bool = True
    allow_no_schema: bool = False

    def read(
        self,
        validate: Optional[bool] = None,
        log_metrics: Optional[bool] = None,
        pa_schema: Optional[Type[SchemaModel]] = None,
    ) -> pd.DataFrame:
        """Read from resource. Read, then process."""
        df = self._resource_read()
        return self._process(df, validate, log_metrics, pa_schema)

    def write(
        self,
        df: pd.DataFrame,
        validate: Optional[bool] = None,
        log_metrics: Optional[bool] = None,
        pa_schema: Optional[Type[SchemaModel]] = None,
    ) -> None:
        """Write to resource. Process, then write."""
        df = self._process(df, validate, log_metrics, pa_schema)
        return self._resource_write(df)

    def _process(
        self,
        df: pd.DataFrame,
        validate: Optional[bool],
        log_metrics: Optional[bool],
        pa_schema: Optional[Type[SchemaModel]],
    ) -> pd.DataFrame:
        """Process data."""
        self._check_injections()

        # Use defaults if not specified during read/write
        if (validate is None and self.validate_default) or validate:
            df = self._validate(df, pa_schema)
        if (log_metrics is None and self.log_metrics_default) or log_metrics:
            self._log_metrics(df)

        return df

    def _log_metrics(self, df: pd.DataFrame) -> None:
        """Log metrics."""
        # TODO: implement this - tied to schema?

    def _validate(self, df: pd.DataFrame, pa_schema: Optional[Type[SchemaModel]] = None) -> pd.DataFrame:
        """Validate dataframe."""
        if pa_schema is not None:
            return pa_schema.validate(df)  # type: ignore
        if self.pa_schema is not None:
            return self.pa_schema.validate(df)  # type: ignore
        if not self.allow_no_schema:
            raise ValueError("No schema provided and allow_no_schema is False")
        return df

    def _check_injections(self) -> None:
        """Check that there are no missing injections. Implement in subclass if relevant."""

    def inject(self, **kwargs) -> None:
        """Inject kwargs into resource paths/wherever relevant. Implement in subclass if needed."""

    @abstractmethod
    def _resource_read(self) -> pd.DataFrame:
        """Read from resource."""
        raise NotImplementedError()

    @abstractmethod
    def _resource_write(self, df) -> None:
        """Write to resource."""
        raise NotImplementedError()

    class Config:  # pylint: disable=missing-class-docstring
        """Pydantic config."""

        validate_assignment = True
        allow_arbitrary_types = True
