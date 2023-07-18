# pylint: disable=protected-access
"""Csv config and resource."""
from __future__ import annotations

from typing import ClassVar
from abc import ABC, abstractmethod
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional, Type

import pandas as pd
from pandera import SchemaModel
from pydantic import BaseModel  # type: ignore
from uhura.serde import Serde
from uhura import Readable, Writable

from dynamicio.inject import check_injections, inject


class BaseResource(ABC, BaseModel, Readable[pd.DataFrame], Writable[pd.DataFrame]):
    """Abstract Base Resource."""
    serde: ClassVar[Optional[Serde]] = None  # this should be replaced in the concreted implementation

    read_kwargs: Dict[str, Any] = {}
    write_kwargs: Dict[str, Any] = {"index": False}
    pa_schema: Optional[Type[SchemaModel]] = None


    def check_injections(self) -> None:
        """Check that all injections have been completed."""
        check_injections(self.path)

    def read(self) -> pd.DataFrame:
        """Read a file."""
        self.check_injections()
        df = self._concrete_resource_read()
        df = self.validate(df)
        return df

    @abstractmethod
    def _concrete_resource_read(self) -> pd.DataFrame:
        raise NotImplementedError

    def write(self, df: pd.DataFrame) -> None:
        """Write the CSV file."""
        self.check_injections()
        df = self.validate(df)
        self._concrete_resource_write(df)

    @abstractmethod
    def _concrete_resource_write(self, df: pd.DataFrame) -> None:
        raise NotImplementedError

    def validate(self, df: pd.DataFrame) -> pd.DataFrame:
        if schema := self.pa_schema:
            df = schema.validate(df)
        return df


    def get_serde(self):
        if self.serde is None:
            raise NotImplementedError("A valid Serde should be specified for the Resource")
        else:
            # TODO update uhura serde interface so that it aligns with Dynamicio needs
            return self.serde(self.validate, self.read_kwargs, self.write_kwargs)
