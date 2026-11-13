# Basic I/O

These examples run locally and are executed by the test suite. They share one empty working directory, in order.

## One definition, several formats

The same schema and IO class serve any format; only `file_type` and its options change in the definition.

<!-- doctest -->
```python
import pathlib

import pandas as pd
from pandera.typing import Series

from dynamicio import UnifiedIO
from dynamicio.config import IOConfig
from dynamicio.schema import DataModel, Field

pathlib.Path("events.yaml").write_text(
    """
EVENTS_CSV:
  LOCAL:
    type: local
    local:
      file_path: "[[ DATA_DIR ]]/events.csv"
      file_type: csv

EVENTS_JSON:
  LOCAL:
    type: local
    local:
      file_path: "[[ DATA_DIR ]]/events.json"
      file_type: json
"""
)


class Settings:
    DATA_DIR = "."


class Events(DataModel, dataset="events"):
    event_id: Series[int] = Field(nullable=False, unique=True)
    label: Series[str] = Field(nullable=False)


class EventsIO(UnifiedIO):
    schema = Events


config = IOConfig("events.yaml", env_identifier="LOCAL", dynamic_vars=Settings)
frame = pd.DataFrame({"event_id": [1, 2], "label": ["start", "stop"]})

for key in ("EVENTS_CSV", "EVENTS_JSON"):
    io = EventsIO(source_config=config.get(source_key=key), apply_schema_validations=True)
    io.write(frame)
    assert io.read()["label"].tolist() == ["start", "stop"]
```

## An optional source

A source read with `missing_ok=True` reads as `None` when it does not exist, for example the state of a first run.

<!-- doctest -->
```python
pathlib.Path("state.yaml").write_text(
    """
PREVIOUS_STATE:
  LOCAL:
    type: local
    local:
      file_path: "[[ DATA_DIR ]]/previous_state.csv"
      file_type: csv
"""
)

state_config = IOConfig("state.yaml", env_identifier="LOCAL", dynamic_vars=Settings)
state = EventsIO(source_config=state_config.get(source_key="PREVIOUS_STATE"), missing_ok=True).read()
assert state is None
```

## Selecting the environment at run time

<!-- doctest -->
```python
import os

environment = os.getenv("ENVIRONMENT", "LOCAL")  # "DEV" and "PROD" need entries in the definitions
config = IOConfig("events.yaml", env_identifier=environment, dynamic_vars=Settings)
```
