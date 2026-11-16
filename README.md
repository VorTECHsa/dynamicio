[![Build and Publish](https://github.com/VorTECHsa/dynamicio/actions/workflows/build_and_publish.yml/badge.svg)](https://github.com/VorTECHsa/dynamicio/actions/workflows/build_and_publish.yml)
[![codecov](https://codecov.io/gh/VorTECHsa/dynamicio/graph/badge.svg?token=McZFwD2QPh)](https://codecov.io/gh/VorTECHsa/dynamicio)
[<img src="https://img.shields.io/badge/slack-@vortexa/dynamicio_public-purple.svg?logo=slack">](https://join.slack.com/share/enQtMzg2Nzk3ODY3MzEzNi0yNTU1ZmIyN2JkMGFhZjhhZWVjNzA2OWUzNWIyMjMyYmYzZmE4MzBjYWQ3YjdhNjU1MGU2NjFkNzMyZDllMzE2?raw=True)

<img src="https://raw.githubusercontent.com/VorTECHsa/dynamicio/master/docs/images/logo-transparent.png" alt="dynamicio — Wrap your I/O" width="500"> <img src="https://raw.githubusercontent.com/VorTECHsa/dynamicio/master/docs/images/wrapped-panda.png" alt="The dynamicio panda mascot" width="100">

**Keep your business logic independent of I/O.** dynamicio is the seam between your `pandas` code and the places your
data lives: describe each dataset once in YAML, and your code just calls `read()` and `write()`.

The same code reads a parquet file on your laptop, a CSV prefix on S3, a PostgreSQL table or an Athena query. You pick
which one by changing a single identifier, so developers stop worrying about connection strings, file-format keyword
arguments and which bucket a task reads this week.

**Documentation:** [dynamicio.readthedocs.io](https://dynamicio.readthedocs.io/en/latest/): guides, examples and API reference
· **Changelog:** [CHANGELOG.md](https://github.com/VorTECHsa/dynamicio/blob/master/CHANGELOG.md)

## Why dynamicio

- **Logic that does not know about storage.** Resource definitions are YAML, per environment (`LOCAL`, `DEV`, `PROD`, or your own).
  Moving a pipeline between environments never touches Python.
- **Fast, safe feedback loops.** Run the real pipeline against sample files on your machine, with no cloud credentials, no deploy and
  no mocks of external services. Compare the outputs with expected files using the built-in test harness (`dynamicio.testharness`).
- **Made for the LLM era.** A coding agent that changes a pipeline needs somewhere safe to run it and a check that says whether the
  change was right. A `LOCAL` environment and expected outputs give it both, and production credentials never enter the loop.
- **Contracts for your dataframes.** A [Pandera](https://pandera.readthedocs.io) model declares columns, dtypes and validations. Bad data
  fails at the boundary with every violation listed, not three steps later, and the same model can emit metrics.
- **Pandas behaviour, not a new dialect.** Options in the YAML go to `pandas` / `awswrangler`, so what you know still applies.
- **Sync and async.** Read and write from plain code or from `asyncio` services.

## Supported sources

| Source | Read | Write | Formats |
|---|---|---|---|
| Local files | yes | yes | `parquet`, `hdf`, `json`, `csv` |
| S3 (single object or prefix) | yes | yes | `parquet`, `hdf`, `json`, `csv` |
| PostgreSQL | yes | yes | tables |
| AWS Athena | yes | no | SQL query results |
| Kafka | no | yes | messages |

Dataframe execution currently supports pandas only; Polars is not yet supported.

## Install

```shell
pip install dynamicio
# or, in a uv project
uv add dynamicio
```

Python 3.10 to 3.14.

## See it work

Describe the resource in `orders.yaml`:

```yaml
ORDERS:
  LOCAL:
    type: local
    local:
      file_path: "[[ DATA_DIR ]]/orders.parquet"
      file_type: parquet
```

Bind it to a schema, then read and write:

```python
import pandas as pd
from pandera.typing import Series

from dynamicio import UnifiedIO
from dynamicio.config import IOConfig
from dynamicio.schema import DataModel, Field


class Settings:
    DATA_DIR = "."


class Orders(DataModel, dataset="orders"):
    order_id: Series[int] = Field(nullable=False, unique=True)
    amount: Series[float] = Field(nullable=False, ge=0)


class OrdersIO(UnifiedIO):
    schema = Orders


config = IOConfig("orders.yaml", env_identifier="LOCAL", dynamic_vars=Settings)
orders = OrdersIO(source_config=config.get(source_key="ORDERS"), apply_schema_validations=True)

orders.write(pd.DataFrame({"order_id": [1, 2, 3], "amount": [9.5, 20.0, 3.25]}))
df = orders.read()
```

To run the same code against S3, add a `DEV` block to the YAML and set `env_identifier="DEV"`. The Python does not change.

## Where next

1. **The idea:** [why dynamicio](https://dynamicio.readthedocs.io/en/latest/getting-started/why-dynamicio/).
2. **New here?** The [quickstart](https://dynamicio.readthedocs.io/en/latest/getting-started/quickstart/) explains each step above,
   and its code is run by the test suite.
3. **Core ideas:** [concepts](https://dynamicio.readthedocs.io/en/latest/getting-started/concepts/): resource definitions, IO classes, schemas.
4. **Task guides:** [reading](https://dynamicio.readthedocs.io/en/latest/guides/reading-data/),
   [writing](https://dynamicio.readthedocs.io/en/latest/guides/writing-data/),
   [testing pipelines](https://dynamicio.readthedocs.io/en/latest/guides/testing-pipelines/).
5. **Reference:** [API reference](https://dynamicio.readthedocs.io/en/latest/reference/).

## Community and contributing

- Questions and bugs: [GitHub issues](https://github.com/VorTECHsa/dynamicio/issues)
- Contributions are welcome: [CONTRIBUTING.md](https://github.com/VorTECHsa/dynamicio/blob/master/CONTRIBUTING.md)
- Source: <https://github.com/VorTECHsa/dynamicio> · PyPI: <https://pypi.org/project/dynamicio/>
- License: Apache-2.0 ([LICENSE](https://github.com/VorTECHsa/dynamicio/blob/master/LICENSE))

Original logo illustrated by [Nick Loucas](https://www.linkedin.com/in/nickloucas/); the refreshed logo retains its circular I/O motif.
