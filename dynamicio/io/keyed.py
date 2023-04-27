# pylint: disable=W0707
"""KeyedResource class for reading and writing to different resources based on a key."""
from __future__ import annotations

from copy import deepcopy
from typing import Protocol, Tuple, Type

import pandas as pd
from pandera import SchemaModel


class IOConfig(Protocol):
    """IOConfig Protocol."""

    def inject(self, **kwargs) -> "IOConfig":
        """Inject variables. Immutable."""
        return deepcopy(self)

    def check_injections(self) -> None:
        """Check that all injections have been completed. Raise InjectionError if not."""


class IOResource(Protocol):
    """IOResource Protocol."""

    def __init__(self, config: IOConfig, pa_schema: Type[SchemaModel] | None = None):
        """Initialize the IO Resource."""

    def read(self) -> pd.DataFrame:
        """Read."""

    def write(self, df: pd.DataFrame) -> None:
        """Write."""


BuildConfig = Tuple[Type[IOResource], IOConfig]


class KeyedResource:
    """KeyedResource class for reading and writing based on a key and given configs and io."""

    def __init__(
        self,
        keyed_build_configs: dict[str, BuildConfig],
        pa_schema: Type[SchemaModel] | None = None,
        default_key: str | None = None,
    ):
        """Initialize the KeyedResource."""
        if len(keyed_build_configs) == 0:
            raise ValueError("KeyedResource must have at least one build_config.")
        self.keyed_build_configs = keyed_build_configs
        self.pa_schema = pa_schema
        self.key = default_key or list(keyed_build_configs.keys())[0]

    def inject(self, **kwargs) -> "KeyedResource":
        """Inject variables into all configs. Immutable."""
        new = deepcopy(self)
        for key, (resource, config) in new.keyed_build_configs.items():
            new.keyed_build_configs[key] = (resource, config.inject(**kwargs))
        return new

    def read(self) -> pd.DataFrame:
        """Read from the active key resource."""
        resource, config = self.keyed_build_configs[self.key]
        return resource(config, self.pa_schema).read()

    def write(self, df: pd.DataFrame) -> None:
        """Write to the active key resource."""
        resource, config = self.keyed_build_configs[self.key]
        return resource(config, self.pa_schema).write(df)
