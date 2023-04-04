# pylint: disable=W0707
"""KeyedResource class for reading and writing to different resources based on a key."""

import os
from typing import Dict, Optional

import pandas as pd

from dynamicio.base import BaseResource


class KeyedResource(BaseResource):
    """A resource that can be read from and written to based on a key.

    define keyed_resources as a dict of resources keyed by a string.

    Warning:
    key_env_var_name is case-insensitive and expects env vars to be uppercase.
    """

    default_key: str = "default"
    keyed_resources: Dict[str, BaseResource]
    load_key_from_env: bool = False
    selected_key: Optional[str] = None
    key_env_var_name: str = "DYNAMICIO_RESOURCE_KEY"

    def set_key_from_env(self, env_var_name: Optional[str] = None) -> None:
        """Set key from environment variable. env_var_name defaults to self.key_env_var_name."""
        if env_var_name:
            self.selected_key = os.environ.get(env_var_name.upper())
        else:
            self.selected_key = os.environ.get(self.key_env_var_name.upper())

    def set_key(self, key: str) -> None:
        """Set key explicitly."""
        self.selected_key = key

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
