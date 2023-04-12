# pylint: disable=no-name-in-module disable=invalid-name

"""BaseResource class for creating various resources types."""
from abc import ABC, abstractmethod
from copy import deepcopy
from typing import Optional, Type

import pandas as pd
import pandera as pa
from pandera import SchemaModel
from pydantic import BaseModel


class BaseResource(BaseModel, ABC):
    """BaseClass for resource classes."""

    pa_schema: Optional[pa.SchemaModel]
    disable_validation: bool = False
    log_metrics_default: bool = True
    allow_no_schema: bool = False

    def read(
        self,
        validate: Optional[bool] = None,
        log_metrics: Optional[bool] = None,
        pa_schema: Optional[Type[SchemaModel]] = None,
    ) -> pd.DataFrame:
        """Read from resource.

        Read, then process.

        Args:
            validate: Whether to validate the dataframe before writing. If not given, will validate if a schema is
                available.
            log_metrics: Whether to log metrics for the dataframe before writing. If not given, will log metrics if a
                schema is available.
            pa_schema: Schema to validate against. If not given, will use the schema defined to the resource.
                If given, will override the resource schema.

        Returns:
            Processed dataframe.
        """
        self._check_injections()
        df = self._resource_read()
        return self._process(df, validate, log_metrics, pa_schema)

    def write(
        self,
        df: pd.DataFrame,
        validate: Optional[bool] = None,
        log_metrics: Optional[bool] = None,
        pa_schema: Optional[Type[SchemaModel]] = None,
    ) -> None:
        """Write to resource.

        Process, then write.

        Args:
            df: Dataframe to write.
            validate: Whether to validate the dataframe before writing. If not given, will validate if a schema is
                available.
            log_metrics: Whether to log metrics for the dataframe before writing. If not given, will log metrics if a
                schema is available.
            pa_schema: Schema to validate against. If not given, will use the schema defined to the resource.
                If given, will override the resource schema.

        Returns:
            None
        """
        self._check_injections()
        df = self._process(df, validate, log_metrics, pa_schema)
        return self._resource_write(df)

    def inject(self, **_) -> "BaseResource":
        """Inject kwargs into relevant resource attributes. Immutable."""
        return deepcopy(self)

    def _process(
        self,
        df: pd.DataFrame,
        validate: Optional[bool],
        log_metrics: Optional[bool],
        pa_schema: Optional[Type[SchemaModel]],
    ) -> pd.DataFrame:
        """Process data."""
        # Use defaults if not specified during read/write
        if (validate is None and not self.disable_validation) or validate:
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
        arbitrary_types_allowed = True
