# pylint: disable=protected-access
"""Csv config and resource."""
from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
from pydantic import BaseModel  # type: ignore
from uhura import Readable, Writable

from dynamicio.inject import inject
from dynamicio.io.base import BaseResource


class BaseFileResource(BaseResource, BaseModel, Readable[pd.DataFrame], Writable[pd.DataFrame]):
    """Abstract Base File Resource."""

    path: Path
    test_path: Optional[Path] = None

    def inject(self, **kwargs) -> "BaseFileResource":
        """Inject variables into path. Immutable."""
        clone = deepcopy(self)
        clone.path = inject(clone.path, **kwargs)
        clone.test_path = inject(clone.test_path, **kwargs)
        return clone

    def cache_key(self):
        if self.test_path:
            return str(self.test_path)
        return f"file/{self.path}"
