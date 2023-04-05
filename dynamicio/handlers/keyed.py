# pylint: disable=W0707
"""KeyedResource class for reading and writing to different resources based on a key."""

import os
from copy import deepcopy
from typing import Dict, Optional

import pandas as pd

from dynamicio.base import BaseResource


class KeyedResource(BaseResource):
    """A resource that can be read from and written to based on a key.

    define keyed_resources as a dict of resources keyed by a string.

    Warning:
    key_env_var_name is case-insensitive and expects env vars to be uppercase.
    """

    keyed_resources: Dict[str, BaseResource]
    default_key: str = "default"
    selected_key: Optional[str] = None

    def __getitem__(self, key: str) -> BaseResource:
        """Get resource by key."""
        return self.keyed_resources[key]

    def set_key_from_env(self, env_var_name: str = "DYNAMICIO_RESOURCE_KEY") -> "KeyedResource":
        """Set key from environment variable. env_var_name defaults to self.key_env_var_name. Immutable."""
        new = deepcopy(self)
        new.selected_key = os.environ.get(env_var_name.upper())
        return new

    def set_key(self, key: str) -> "KeyedResource":
        """Set key explicitly. Immutable."""
        new = deepcopy(self)
        new.selected_key = key
        return new

    def inject(self, **kwargs) -> "KeyedResource":
        """Inject kwargs into selected resource. Warning, correct resource needs to be selected first. Immutable."""
        new = deepcopy(self)
        for key, resource in new.keyed_resources.items():
            new.keyed_resources[key] = resource.inject(**kwargs)
        return new

    def _resource_read(self) -> pd.DataFrame:
        key = self._get_key()
        try:
            resource = self.keyed_resources[key]
        except KeyError:
            raise KeyError(f"Resource key {key} not found in keyed_resources.")
        return resource.read(validate=False, log_metrics=False)

    def _resource_write(self, df) -> None:
        key = self._get_key()
        self.keyed_resources[key].write(df, validate=False, log_metrics=False)

    def _get_key(self) -> str:
        if self.selected_key:
            return self.selected_key
        return self.default_key
