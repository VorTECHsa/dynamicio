from abc import abstractmethod
from copy import deepcopy
from pathlib import Path
from typing import Callable, List, Optional, Type

import pandas as pd
from pandera import SchemaModel
from pydantic import BaseModel
from uhura import Readable, Writable

from dynamicio.inject import InjectionError, inject
from dynamicio.io.serde import BaseSerde, PickleSerde


def create_schema_validator(schema) -> Callable[[pd.DataFrame], pd.DataFrame]:
    def validate_schema(df: pd.DataFrame):
        return schema.validate(df)

    return validate_schema


class BaseResource(BaseModel, Readable[pd.DataFrame], Writable[pd.DataFrame]):  # Cacheable kaputt, ABC kaputt
    """Base class for all resources.

    :injectables: List of attributes that can be injected with format strings.
    :test_path (optional): Path to the test data. If set, the resource will be read from and written to this path.
    :pa_schema (optional): Pandera schema to validate the resource. If set, the resource will be validated before writing and after reading.
    """

    injectables: List[str]
    test_path: Optional[Path] = None
    pa_schema: Optional[Type[SchemaModel]] = None

    def inject(self, **kwargs) -> "BaseResource":
        # copy object
        clone = deepcopy(self)
        for injectable in self.injectables:
            # inject attributes
            value = getattr(clone, injectable)
            if isinstance(value, str) or isinstance(value, Path) or value is None:
                formatted_str = inject(value, **kwargs)
                setattr(clone, injectable, formatted_str)

            else:
                raise InjectionError(f"Cannot inject {injectable} of type {type(value)}")

            # inject test path
            if self.test_path is not None:
                clone.test_path = inject(self.test_path, **kwargs)
        return clone

    @abstractmethod
    def _read(self) -> pd.DataFrame:
        """Internal read method. Should not be called directly. Use read() instead.

        Overwrite this method to implement custom read logic.
        The main read() method is replaced when in uhura testing mode."""
        ...

    @abstractmethod
    def _write(self, df: pd.DataFrame) -> None:
        """Internal write method. Should not be called directly. Use write() instead.

        Overwrite this method to implement custom write logic.
        The main write() method is replaced when in uhura testing mode."""
        ...

    def read(self) -> pd.DataFrame:
        """Read the resource."""
        df = self._read()
        df = self.get_serde().validate(df)
        return df

    def write(self, df: pd.DataFrame) -> None:
        """Write the resource."""
        df = self.get_serde().validate(df)
        self._write(df)

    def cache_key(self):
        """Return the test path."""
        if self.test_path is None:
            raise ValueError("No test path set. Please implement fixture_path property or set test_path.")
        return str(self.test_path)

    @property
    def serde_class(self) -> Type[BaseSerde]:
        """Return the serde class. Default is PickleSerde."""
        return PickleSerde

    def get_serde(self) -> BaseSerde:
        """Return the serde instance, with baked-in validation."""
        validations = []
        if self.pa_schema is not None:
            # validations.append(create_schema_validator(self.pa_schema))
            validations.append(self.pa_schema.validate)

        return self.serde_class(validations=validations)
