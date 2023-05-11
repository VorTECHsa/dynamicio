# pylint: disable=W0707
"""KeyedResource class for reading and writing to different resources based on a key."""
from __future__ import annotations

from copy import deepcopy
from typing import Protocol, Type

import pandas as pd
from pandera import SchemaModel


class IOResource(Protocol):
    """IOResource Protocol."""

    def inject(self, **kwargs) -> "IOResource":
        """Inject variables. Immutable."""
        return deepcopy(self)

    def check_injections(self) -> None:
        """Check that all injections have been completed. Raise InjectionError if not."""

    def read(self) -> pd.DataFrame:
        """Read."""

    def write(self, df: pd.DataFrame) -> None:
        """Write."""


class KeyedResource:
    """KeyedResource class for reading and writing based on a key and given configs and io."""

    def __init__(
        self,
        keyed_resources: dict[str, IOResource],
        pa_schema: Type[SchemaModel] | None = None,
        default_key: str | None = None,
    ):
        """Initialize the KeyedResource."""
        if len(keyed_resources) == 0:
            raise ValueError("KeyedResource must have at least one resource.")
        self.keyed_build_configs = keyed_resources
        self.pa_schema = pa_schema
        self.key = default_key or list(keyed_resources.keys())[0]

    def inject(self, **kwargs) -> "KeyedResource":
        """Inject variables into all configs. Immutable."""
        new = deepcopy(self)
        for key, resource in new.keyed_build_configs.items():
            new.keyed_build_configs[key] = resource.inject(**kwargs)
        return new

    def read(self) -> pd.DataFrame:
        """Read from the active key resource."""
        resource = self.keyed_build_configs[self.key]
        return resource.read()

    def write(self, df: pd.DataFrame) -> None:
        """Write to the active key resource."""
        resource = self.keyed_build_configs[self.key]
        return resource.write(df)
