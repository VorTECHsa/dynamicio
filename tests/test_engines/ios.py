"""The IO class and models of the engine tests: a local CSV resource whose model is an argument."""

# pylint: disable=missing-class-docstring
from pathlib import Path
from typing import Any, Optional

import pandera.pandas as pa
import yaml
from pandera.typing import Series

from dynamicio import UnifiedIO
from dynamicio.config import IOConfig
from dynamicio.config.pydantic import IOEnvironment
from dynamicio.core import SCHEMA_FROM_ARGUMENT
from dynamicio.schema import Field, Metric


class Positive(pa.DataFrameModel):
    __dataset_name__ = "positive"

    a: Series[int] = Field(nullable=False, gt=0, metrics=[Metric.MIN])


class EngineCheckIO(UnifiedIO):
    schema = SCHEMA_FROM_ARGUMENT

    @staticmethod
    def make(tmp_path: Path, model: Any = Positive, source_config: Optional[IOEnvironment] = None, **kwargs: Any) -> "EngineCheckIO":
        """An IO object with `model` as its schema, for `source_config`, or for `tmp_path/data.csv` when no `source_config` is given."""
        if source_config is None:
            definition = {"X": {"LOCAL": {"type": "local", "local": {"file_path": str(tmp_path / "data.csv"), "file_type": "csv", "options": {"index": False}}}}}
            (tmp_path / "definitions.yaml").write_text(yaml.safe_dump(definition))
            source_config = IOConfig(str(tmp_path / "definitions.yaml"), env_identifier="LOCAL", dynamic_vars=EngineCheckIO).get(source_key="X")
        return EngineCheckIO(source_config=source_config, schema=model, **kwargs)
