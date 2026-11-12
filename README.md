[![Build and Publish](https://github.com/VorTECHsa/dynamicio/actions/workflows/build_and_publish.yml/badge.svg)](https://github.com/VorTECHsa/dynamicio/actions/workflows/build_and_publish.yml)
[![codecov](https://codecov.io/gh/VorTECHsa/dynamicio/graph/badge.svg?token=McZFwD2QPh)](https://codecov.io/gh/VorTECHsa/dynamicio)
[<img src="https://img.shields.io/badge/slack-@vortexa/dynamicio_public-purple.svg?logo=slack">](https://join.slack.com/share/enQtMzg2Nzk3ODY3MzEzNi0yNTU1ZmIyN2JkMGFhZjhhZWVjNzA2OWUzNWIyMjMyYmYzZmE4MzBjYWQ3YjdhNjU1MGU2NjFkNzMyZDllMzE2?raw=True)

<img src="https://raw.githubusercontent.com/VorTECHsa/dynamicio/master/docs/images/logo-transparent.png" alt="dynamicio — Wrap your I/O" width="500"> <img src="https://raw.githubusercontent.com/VorTECHsa/dynamicio/master/docs/images/wrapped-panda.png" alt="The dynamicio panda mascot" width="100">

**Stop rewriting the same pandas I/O code. Describe your data once in YAML, and read and write it anywhere.**

dynamicio wraps the I/O of a `pandas` pipeline in configuration. The same code reads a parquet file on your laptop, a CSV
prefix on S3, a PostgreSQL table or an Athena query, and you pick which one by changing a single identifier.

**Documentation:** [guides, examples and API reference](https://github.com/VorTECHsa/dynamicio/tree/master/docs)
<!-- Replace the link above with the Read the Docs URL once the project is imported (docs/development/readthedocs-setup.md) -->
· **Changelog:** [CHANGELOG.md](https://github.com/VorTECHsa/dynamicio/blob/master/CHANGELOG.md)

## Why dynamicio

- **One place for "where is the data".** Resource definitions are YAML, per environment (`LOCAL`, `DEV`, `PROD`, or your own).
  Moving a pipeline between environments never touches Python.
- **Contracts for your dataframes.** A [Pandera](https://pandera.readthedocs.io) model declares columns, dtypes and validations. Bad data
  fails at the boundary, not three steps later, and the same model can emit metrics.
- **Local-first testing.** Run the real pipeline against sample files, then compare the outputs with expected files
  using the built-in test harness (`dynamicio.testharness`).
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
import pandera.pandas as pa
from pandera.typing import Series

from dynamicio import UnifiedIO
from dynamicio.config import IOConfig


class Settings:
    DATA_DIR = "."


class Orders(pa.DataFrameModel):
    order_id: Series[int] = pa.Field(nullable=False, unique=True)
    amount: Series[float] = pa.Field(nullable=False, ge=0)


class OrdersIO(UnifiedIO):
    schema = Orders


config = IOConfig("orders.yaml", env_identifier="LOCAL", dynamic_vars=Settings)
orders = OrdersIO(source_config=config.get(source_key="ORDERS"), apply_schema_validations=True)

orders.write(pd.DataFrame({"order_id": [1, 2, 3], "amount": [9.5, 20.0, 3.25]}))
df = orders.read()
```

To run the same code against S3, add a `DEV` block to the YAML and set `env_identifier="DEV"`. The Python does not change.

## Where next

1. **New here?** The [quickstart](https://github.com/VorTECHsa/dynamicio/blob/master/docs/getting-started/quickstart.md) explains each step above,
   and its code is run by the test suite.
2. **Core ideas:** [concepts](https://github.com/VorTECHsa/dynamicio/blob/master/docs/getting-started/concepts.md): resource definitions, IO classes, schemas.
3. **Task guides:** [reading](https://github.com/VorTECHsa/dynamicio/blob/master/docs/guides/reading-data.md),
   [writing](https://github.com/VorTECHsa/dynamicio/blob/master/docs/guides/writing-data.md),
   [testing pipelines](https://github.com/VorTECHsa/dynamicio/blob/master/docs/guides/testing-pipelines.md).
4. **Reference:** [API reference](https://github.com/VorTECHsa/dynamicio/blob/master/docs/reference/index.md).

## Community and contributing

- Questions and bugs: [GitHub issues](https://github.com/VorTECHsa/dynamicio/issues)
- Contributions are welcome: [CONTRIBUTING.md](https://github.com/VorTECHsa/dynamicio/blob/master/CONTRIBUTING.md)
- Source: <https://github.com/VorTECHsa/dynamicio> · PyPI: <https://pypi.org/project/dynamicio/>
- License: Apache-2.0 ([LICENSE](https://github.com/VorTECHsa/dynamicio/blob/master/LICENSE))

Original logo illustrated by [Nick Loucas](https://www.linkedin.com/in/nickloucas/); the refreshed logo retains its circular I/O motif.
