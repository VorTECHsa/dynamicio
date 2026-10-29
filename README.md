[![Build and Publish](https://github.com/VorTECHsa/dynamicio/actions/workflows/build_and_publish.yml/badge.svg)](https://github.com/VorTECHsa/dynamicio/actions/workflows/build_and_publish.yml)
[![codecov](https://codecov.io/gh/VorTECHsa/dynamicio/graph/badge.svg?token=McZFwD2QPh)](https://codecov.io/gh/VorTECHsa/dynamicio)
[<img src="https://img.shields.io/badge/slack-@vortexa/dynamicio_public-purple.svg?logo=slack">](https://join.slack.com/share/enQtMzg2Nzk3ODY3MzEzNi0yNTU1ZmIyN2JkMGFhZjhhZWVjNzA2OWUzNWIyMjMyYmYzZmE4MzBjYWQ3YjdhNjU1MGU2NjFkNzMyZDllMzE2?raw=True)

<img src="https://github.com/VorTECHsa/dynamicio/blob/master/docs/images/logo-transparent.png?raw=True" width="500"> <img src="https://github.com/VorTECHsa/dynamicio/blob/master/docs/images/wrapped-panda.png?raw=True" width="100">

A repository for hosting the `dynamicio` library, used as a wrapper for `pandas` i/o operations.

-- Logo illustrated by [Nick Loucas](https://www.linkedin.com/in/nickloucas/)

## Table of Contents
- [Why wrap your i/o?](https://github.com/VorTECHsa/dynamicio/tree/master#why-wrap-your-i-o)
  * [Managing various Resources](https://github.com/VorTECHsa/dynamicio/tree/master#managing-various-resources)
  * [Managing Various Data Types](https://github.com/VorTECHsa/dynamicio/tree/master#managing-various-data-types)
  * [Validations & Metrics Generation](https://github.com/VorTECHsa/dynamicio/tree/master#validations---metrics-generation)
  * [Testing (Running Local Regression Tests)](https://github.com/VorTECHsa/dynamicio/tree/master#testing--running-local-regression-tests)
  * [So, what do we do about these?](https://github.com/VorTECHsa/dynamicio/tree/master#so--what-do-we-do-about-these)
  * [The Solution](https://github.com/VorTECHsa/dynamicio/tree/master#the-solution)
  * [Main features](https://github.com/VorTECHsa/dynamicio/tree/master#main-features)
- [Supported sources and data formats:](https://github.com/VorTECHsa/dynamicio/tree/master#supported-sources-and-data-formats)
  * [S3 I/O](https://github.com/VorTECHsa/dynamicio/tree/master#s3-i-o)
  * [Athena I/O](https://github.com/VorTECHsa/dynamicio/tree/master#athena-i-o)
  * [Coming soon](https://github.com/VorTECHsa/dynamicio/tree/master#coming-soon)
- [Installation](https://github.com/VorTECHsa/dynamicio/tree/master#installation)
- [API Documentation](https://github.com/VorTECHsa/dynamicio/tree/master#api-documentation)
- [How to use](https://github.com/VorTECHsa/dynamicio/tree/master#how-to-use)
  * [Keywords](https://github.com/VorTECHsa/dynamicio/tree/master#keywords)
  * [Let's start](https://github.com/VorTECHsa/dynamicio/tree/master#let-s-start)
    + [Step 1: Resource Definitions](https://github.com/VorTECHsa/dynamicio/tree/master#step-1--resource-definitions)
    + [Step 2: Defining your environment variables](https://github.com/VorTECHsa/dynamicio/tree/master#step-2--defining-your-environment-variables)
    + [Step 3: Read in your resource definitions](https://github.com/VorTECHsa/dynamicio/tree/master#step-3--read-in-your-resource-definitions)
    + [Step 4: Loading the data resources](https://github.com/VorTECHsa/dynamicio/tree/master#step-4--loading-the-data-resources)
      - [Step 4.1. Pandera models](https://github.com/VorTECHsa/dynamicio/tree/master#step-41-pandera-models)
      - [Step 4.1b. `SCHEMA_FROM_FILE`](https://github.com/VorTECHsa/dynamicio/tree/master#step-41b--schema-from-file)
      - [Step 4.2. Use the dynamicio cli](https://github.com/VorTECHsa/dynamicio/tree/master#step-42-use-the-dynamicio-cli)
      - [Step 4.3a: Loading from `S3`](https://github.com/VorTECHsa/dynamicio/tree/master#step-43a-loading-from-s3)
      - [Step 4.3b: Loading from `Postgres`](https://github.com/VorTECHsa/dynamicio/tree/master#step-43b-loading-from-postgres)
      - [Step 4.3c: Loading from `Athena`](https://github.com/VorTECHsa/dynamicio/tree/master#step-43c-loading-from-athena)
    + [Step 5: Writing out](https://github.com/VorTECHsa/dynamicio/tree/master#step-5--writing-out)
    + [Step 6: Full Code](https://github.com/VorTECHsa/dynamicio/tree/master#step-6--full-code)
  * [Utilising `asyncio`](https://github.com/VorTECHsa/dynamicio/tree/master#utilising--asyncio)
- [Testing Locally](https://github.com/VorTECHsa/dynamicio/tree/master#testing-locally)
- [Last notes](https://github.com/VorTECHsa/dynamicio/tree/master#last-notes)

<small><i><a href='http://ecotrust-canada.github.io/markdown-toc/'>Table of contents generated with markdown-toc</a></i></small>

## Why wrap your i/o?

Working with `pandas` dataframes has opened up a new world of potential in Data Science. However,
if you are using `pandas` to support production pipelines, whether ML or ETL, you end up having
a big part of your code be concerned with I/O operations. 

### Managing various Resources
First, it's the various type of resources you need to interact with; object storage (S3 or GCS)
databases (Athena, Big Query, Postgres), Kafka and many more. For each of these, you have 
dependencies on various libraries such as `fsspec`, `gcfs`, `boto3`, `aws-wrangler`,
`sql-alchemy`, `tables`, `kafka-python` and many more. 

### Managing Various Data Types
![data-types](https://github.com/VorTECHsa/dynamicio/blob/master/docs/images/data-types.png?raw=True)

Then it's the various data types you need handle, `parquet`, `hdfs`, `csv`, `json` and many
others, each of which come with their own set of configuration `kwargs`, things like the 
orientation of the dataset (`json`) or the parquet engine you want 
to use behind the scenes (`pyarrow` or `fastparquet`).

### Validations & Metrics Generation
Then, it's the need to validate your expectations on the datasets; things like unique or
null values being allowed in a column, allowing only a specific set of categorical values, or
numerical values within a specified range. And what about metrics generation? The ability to
monitor data distributions and how various metrics change with every run, is a significant
aspect of monitoring the quality of your solution. 

### Testing (Running Local Regression Tests)
Finally, what about testing your code in different environments? Take, for instance, a traditional
setup where you have the following 4 environments to work against:

- Local;
- Develop;
- Staging, and;
- Production.

Configuring your code to work against the last 3 `Develop, Staging and Production` can easily be done through
environment variables, but what about testing locally? What if you want to run your pipelines locally? 
Well, you can, but usually that entails a big deal of mocking calls to external services. Instead, wouldn't
it be great if you could seamlessly direct your I/O operations to local sample data.

### So, what do we do about these? 
This proliferation of I/O operations leads to the emergence of glue code, which can be very difficult to manage. The problem is highlighted 
as an **ML-System Anti-Pattern** in [Hidden Technical Debt in Machine Learning Systems](https://papers.nips.cc/paper/2015/file/86df7dcfd896fcaf2674f757a2463eba-Paper.pdf)

> ...ML researchers tend to develop general purpose solutions as self-contained packages. A wide variety of these are
> available as open-source packages at places like `ml-oss.org`, or from in-house code, proprietary packages, and cloud-based platforms.
>
> Using generic packages often results in a **glue code** system design pattern, in which a massive amount of supporting
> code is written to get data into and out of general-purpose packages. **Glue code** is costly in the long
> term because it tends to freeze a system to the peculiarities of a specific package; testing alternatives
> may become prohibitively expensive. In this way, using a generic package can inhibit improvements, because it
> makes it harder to take advantage of domain-specific properties or to tweak the objective function to
> achieve a domain-specific goal. Because a mature system might end up being (at most) 5% machine learning
> code and (at least) 95% glue code, it may be less costly to create a clean native solution rather
> than re-use a generic package.

### The Solution

Quoting from the same paper:

> An important strategy for combating glue-code is to wrap black-box packages into common API's. This allows supporting
> infrastructure to be more reusable and reduces the cost of changing packages.

Dynamicio (or dynamic(i/o)) serves exactly that; it serves as a convenient wrapper around `pandas` I/O operations. It's a manifestation of 
the dependency inversion principle--a layer of indirection if you want--which keeps your code DRY and increases re-usability, effectively 
decoupling business logic from the I/O layer.

### Main features
`dynamic(i/o)` supports:
* seamless transition between environments; 
* abstracting away from resource and data types through `resource definitions`; 
* honouring your expectations on data through `schema definitions`;
* metrics auto-generation (logging) for monitoring purposes.

## Supported sources and data formats

<img src="https://github.com/VorTECHsa/dynamicio/blob/master/docs/images/supported_sources.png?raw=True" width="600">

- **S3** (or local) Input & Output, for single files (`type: s3`) and for prefixes holding many files (`type: s3_path_prefix`):
  - `parquet`
  - `h5`
  - `json`
  - `csv`
- **Postgres** Input & Output
- **Athena** Input only (`type: athena`)
- **Kafka** Output

### S3 I/O

S3 is accessed directly through `boto3`, with one client shared per process (safe across forked workers):

* **Single files** (`type: s3`): parquet is read with one `GetObject` and parsed in memory, and written with one
  `PutObject` (multipart above 16 MiB), using the same `pandas`/`pyarrow` options as local I/O. `awswrangler` is only
  used when you pass an option that only it supports, and for `csv`/`json`.
* **Prefixes** (`type: s3_path_prefix`): objects are listed and fetched by a pool of 32 threads, one request per object.
  Parquet objects are parsed in memory as they arrive, so parsing overlaps the download. `no_disk_space: true` is still
  accepted.
* **No `awscli` or `s3fs`**: they are no longer dependencies, so installing `dynamicio` no longer puts an `aws` command
  in your environment. Credentials come from the usual `boto3` chain (environment, profile, IAM role).

Compared with the previous implementation, single-file reads are about 2.4x faster and writes about 1.3x faster, and
prefix reads/writes are on par or better, using less CPU per call
([benchmarks](https://github.com/VorTECHsa/dynamicio/blob/master/docs/perf-s3-io.md),
[design decision](https://github.com/VorTECHsa/dynamicio/blob/master/docs/adr/0001-direct-boto3-s3-io.md)).

#### Parquet prefixes that changed schema over time

Prefixes accumulate objects over the years, so older objects may lack columns that were added to the schema later.
When reading a parquet `s3_path_prefix`, such objects are read with the columns they do have and the missing columns
are filled with nulls; a warning lists each column and how many objects lacked it:

```text
[s3] Some objects under s3://bucket/live/state/ lack columns that are filled with nulls: {'model_action': 2} (objects lacking each column)
```

This tolerance is deliberate and bounded: a column that is missing from *every* object raises a `ValueError`, and a
schema column typed as a non-nullable dtype (e.g. `int64`) still fails, because nulls cannot be represented. Single-file
reads and `local_batch` are strict. Watch the warning if the missing data would indicate a real upstream problem rather
than an old schema ([design decision](https://github.com/VorTECHsa/dynamicio/blob/master/docs/adr/0002-tolerate-missing-columns-in-parquet-prefix-reads.md)).

### Athena I/O

`type: athena` resources run a SQL query on AWS Athena and return the result as a dataframe, through
[AWS SDK for pandas](https://aws-sdk-pandas.readthedocs.io/) (`awswrangler`). Mix `WithAthena` into your IO class, as with the other sources.
Athena is read-only: writing raises `NotImplementedError`.

```yaml
EVENTS_ATHENA:
  CLOUD:
    type: athena
    athena:
      s3_output: s3://my-athena-results-bucket/dynamicio/   # required: where Athena writes query results
      database: analytics        # optional, default "default"
      workgroup: primary         # optional
      region_name: eu-west-1     # optional: builds a dedicated boto3 session for that region
      query: SELECT id, ts FROM events WHERE day = '2024-01-01'   # optional here, or pass `query=` when instantiating the IO class
```

Notes:

* The query comes from the resource definition or from `query=...` at instantiation; the latter wins, as do any other
  options, which are passed to `awswrangler.athena.read_sql_query` (unknown ones are dropped). A read without a query raises `ValueError`.
* Credentials and region come from the usual `boto3` chain and the process-wide shared session, unless `region_name` is set.
* `ctas_approach` defaults to `False`: the query runs as is and column names keep the case used in the `SELECT`. Pass
  `ctas_approach=True` to use awswrangler's CTAS path (faster for large results, but it creates a temporary Glue table
  and lower-cases column names).
* awswrangler returns nullable pandas dtypes (`Int64`, `string`, `boolean`) and Python `date` objects for `date`
  columns. When the resource declares a schema, dynamicio casts to the declared dtypes, so declare them.
* **Breaking (9.0):** the `s3_staging_dir` key from the PyAthena era was removed. Rename it to `s3_output` (same
  value); a definition that still has it fails to load with an error saying so.

### Coming soon
- **Delta Tables** (pending)
- **GCS** (pending)
- **BigQuery** (pending)

## Installation

```shell
>> pip install dynamicio
# or, in a uv project
>> uv add dynamicio
```

To contribute, see [CONTRIBUTING.md](CONTRIBUTING.md) (the project is managed with [uv](https://docs.astral.sh/uv/)).

## API Documentation
Read our docs here: https://vortechsa.github.io/dynamicio/

## How to use

We will go over an end-to-end example for reading and writing a single dataset, covering:

1. all components involved and how they are configured, and;
2. how these components are pieced together

You can find this example under the demo directory of this repo.

### Keywords:

- **resource definitions**: the YAML files that say *where* each dataset lives (path, bucket, table, topic...) for each environment.
- **source configs**: what `IOConfig` parses a resource definition into, one per dataset; you hand one to an IO class to read or write that dataset.
- **schema definitions**: the Python classes (Pandera models, see Step 4.1) that say *what* a dataset looks like: columns, dtypes, validations and metrics.
  Older projects may still declare them in YAML or as plain dictionaries; both keep working.

### Let's start

Suppose you want to ingest the `foo` and `bar` datasets, respectively from `S3` and `Postgres` and stage them
to S3 for further processing.

Assume you want to build a pipeline that looks something like the image below:

<img src="https://github.com/VorTECHsa/dynamicio/blob/master/docs/images/sample-pipeline.png?raw=True" width="600">

Assume the below repository structure, which implements this pipeline, for the purpose of this tutorial. It is also the
structure we recommend for a data pipeline repository (see [Recommended project structure](#recommended-project-structure) below):

```shell
demo
├── __init__.py
├── src
│   ├── __init__.py
│   ├── __main__.py
│   ├── config
│   │   ├── __init__.py
│   │   ├── constants.py
│   │   └── environment.py
│   ├── resources
│   │   ├── __init__.py
│   │   ├── definitions
│   │   │   ├── input.yaml
│   │   │   ├── processed.yaml
│   │   │   └── raw.yaml
│   │   ├── io.py
│   │   └── schemas
│   │       ├── __init__.py
│   │       ├── input.py
│   │       └── processed.py
│   └── runners
│       ├── __init__.py
│       ├── staging.py
│       ├── transform.py
│       └── utils
│           ├── __init__.py
│           └── runner_selection.py
└── tests
    ├── __init__.py
    ├── conftest.py
    ├── config
    │   ├── __init__.py
    │   └── constants.py
    ├── data
    │   ├── input
    │   │   ├── bar.parquet
    │   │   └── foo.csv
    │   ├── processed
    │   │   └── expected
    │   │       ├── final_bar.parquet
    │   │       └── final_foo.parquet
    │   └── raw
    │       └── expected
    │           ├── staged_bar.parquet
    │           └── staged_foo.parquet
    ├── resources
    │   ├── __init__.py
    │   └── test_prefix_schema_evolution.py
    ├── runners
    │   ├── __init__.py
    │   ├── conftest.py
    │   ├── test_staging.py
    │   ├── test_transform.py
    │   └── utils
    │       ├── __init__.py
    │       └── test_runner_selection.py
    └── test_pipeline.py
```

##### Recommended project structure

Everything that describes *what data the pipeline touches* lives together under `src/resources`; everything that describes
*what the pipeline does with it* lives outside:

| Path | Holds |
|---|---|
| `src/resources/definitions/*.yaml` | Resource definitions: where each dataset lives per environment, its format and options. YAML, so a bucket or path can change without touching code |
| `src/resources/schemas/*.py` | The datasets' Pandera models: columns, dtypes, validations, metrics. One module per definitions file (`input.py` next to `definitions/input.yaml`), or per runner and direction in a bigger pipeline (`stage.py`, `transform.py`, ...) |
| `src/resources/io.py` | The `UnifiedIO`/`DynamicDataIO` subclasses used to read and write them |
| `src/config/environment.py` | The values behind the `[[ DYNAMIC ]]` variables in the definitions, and the `RESOURCES` path |
| `src/config/constants.py` | Constants shared by the runners, e.g. writer options |
| `src/runners/` | The pipeline steps: they import from `src.resources` and never open a file or table directly |
| `tests/` | Tests mirror the `src/` tree (`tests/runners/test_staging.py` tests `src/runners/staging.py`, and so on), plus `tests/test_pipeline.py` for the end-to-end run, and the sample data under `tests/data` (the `sample` environment points at it) |

Rules of thumb: a dataset is added by touching `src/resources` only (definition, model, optionally an IO class); a runner
changes only when its logic changes. Keep the models next to the definitions that use them so a resource and its contract
are reviewed together. `demo/` follows this structure.

#### Step 1: Resource Definitions

We will start with defining our input and output resources as yaml files. These need to be defined under `src/resources/definitions`:

```shell
resources
└── definitions
    ├── input.yaml
    ├── processed.yaml
    └── raw.yaml
```

Schemas are not part of the resource definitions: they are declared as Pandera models in the `src/resources/schemas/` package, one module per group of datasets (see Step 4.1).

You will need to define your pipeline's resources by creating three `yaml` files. The first is:

- `input.yaml` which concerns data read by the **staging** task;

```yaml
---
FOO:
  sample:
    type: "local"
    local:
      file_path: "[[ TEST_RESOURCES ]]/data/input/foo.csv"
      file_type: "csv"
  actual:
    type: "s3"
    s3:
      bucket: "[[ S3_YOUR_INPUT_BUCKET ]]"
      file_path: "data/foo.h5"
      file_type: "hdf"

BAR:
  sample:
    type: "local"
    local:
      file_path: "[[ TEST_RESOURCES ]]/data/input/bar.parquet"
      file_type: "parquet"
  actual:
    type: "postgres"
    postgres:
      db_host: "[[ DB_HOST ]]"
      db_port: "[[ DB_PORT ]]"
      db_name: "[[ DB_NAME ]]"
      db_user: "[[ DB_USER ]]"
      db_password: "[[ DB_PASS ]]"
```

- the `raw.yaml`, which concerns data coming out of the **staging** task and go into the **transform** task:

```yaml
---
STAGED_FOO:
  sample:
    type: "local"
    local:
      file_path: "[[ TEST_RESOURCES ]]/data/raw/staged_foo.parquet"
      file_type: "parquet"
  actual:
    type: "s3"
    s3:
      bucket: "[[ S3_YOUR_OUTPUT_BUCKET ]]"
      file_path: "live/data/raw/staged_foo.parquet"
      file_type: "parquet"

STAGED_BAR:
  sample:
    type: "local"
    local:
      file_path: "[[ TEST_RESOURCES ]]/data/raw/staged_bar.parquet"
      file_type: "parquet"
  actual:
    type: "s3"
    s3:
      bucket: "[[ S3_YOUR_OUTPUT_BUCKET ]]"
      file_path: "live/data/raw/staged_bar.parquet"
      file_type: "parquet"
```
- and the `processed.yaml`, which concerns data coming out of the **transform** task:
```yaml
---
FINAL_FOO:
  sample:
    type: "local"
    local:
      file_path: "[[ TEST_RESOURCES ]]/data/processed/final_foo.parquet"
      file_type: "parquet"
  actual:
    type: "s3"
    s3:
      bucket: "[[ S3_YOUR_OUTPUT_BUCKET ]]"
      file_path: "live/data/processed/final_foo.parquet"
      file_type: "parquet"

FINAL_BAR:
  sample:
    type: "local"
    local:
      file_path: "[[ TEST_RESOURCES ]]/data/processed/final_bar.parquet"
      file_type: "parquet"
    options:
      use_deprecated_int96_timestamps: true
      coerce_timestamps: "ms"
      allow_truncated_timestamps: false
      row_group_size: 1000000
  actual:
    type: "kafka"
    kafka:
      kafka_server: "[[ KAFKA_SERVER ]]"
      kafka_topic: "[[ KAFKA_TOPIC ]]"
    options:
      compression_type: "snappy"
      max_in_flight_requests_per_connection: 10
      batch_size: 262144
      request_timeout_ms: 60000 # 60s
      buffer_memory: 134217728  # 128MB

```

We will hence refer to these files as **"resource definitions"**.

The first, `input.yaml` defines the input sources for the **staging** task, handled by the respective module (`runners/staging.py`) while the
second one, defines its output; similarly for **transform**. These files are parsed by `dynamicio.config.IOConfig` to generated configuration i/o
instances referred to as "source configs" (see `demo/src/__init__.py`).

Notice that under every source there are two layers: `sample` and `actual`. They point to the variants of the same
dataset, depending on whether it is called from the local environment or from the cloud (we will showcase how this
distinction takes place later).

Resource definitions say nothing about the *shape* of the data. That is the job of the **"schema definition"** of your dataset, a Python
class kept next to them in a module of `src/resources/schemas/` (we will cover this in detail in Step 4).

#### Step 2: Defining your environment variables

Also notice that paths to datasets are embedded with dynamic values identified with double squared brackets, e.g.
`[[ S3_YOUR_OUTPUT_BUCKET ]]`. These can be defined in a module in your repository.

Resource definitions (`*.yaml` files) work in conjunction with `global` and `environment` variables:

- `config/environment.py`

```shell
├── __init__.py
├── src
    └── config
        └── environment.py
...
```

Let's have a look inside.

```python
"""A module for configuring all environment variables."""
import os

ENVIRONMENT = "sample"
CLOUD_ENV = "DEV"
RESOURCES = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../resources")
TEST_RESOURCES = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../tests")
S3_YOUR_INPUT_BUCKET = None
S3_YOUR_OUTPUT_BUCKET = None
KAFKA_SERVER = None
KAFKA_TOPIC = None
DB_HOST = None
DB_PORT = None
DB_NAME = None
DB_USER = None
DB_PASS = None
REFERENCE_DATA_STATE_KEY = None
LOWER_THAN_LIMIT = 1000         # We will discuss this one later in step 4. 
```

This module will be passed as an input parameter to instances of the `dynamicio.config.IOConfig` class. Let's cover
some of its variables:

- ```python
  "ENVIRONMENT": "sample",
  ```
  used to distinguish between local and cloud runs of your module. It assumes that this environment variable is
  defined in the cloud environment where your module is executed from.
- ```python
  "TEST_RESOURCES": os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../tests"),
  ```
  It is defined in the resource definitions, e.g.:

```yaml
---
FOO:
  sample:
    type: "local"
    local:
      file_path: "[[ TEST_RESOURCES ]]/data/input/foo.csv"
      file_type: "csv"
  actual:
    type: "s3"
    s3:
      bucket: "[[ S3_YOUR_INPUT_BUCKET ]]"
      file_path: "data/foo.h5"
      file_type: "hdf"
```

and therefore needs to be defined here as well.

Any other dynamic variable (identified with the doubly squared brackets) defined in the resource definitions needs to
also be defined here and can be either statically or dynamically defined (i.e. hardcoded or defined as an environment
value).

#### Step 3: Read in your resource definitions

Reading in the resources definitions can be done by means of instantiating instances of the `dynamicio.config.IOConfig`
class (the, so called, "source configs"). This is done in:

```shell
src
├── __init__.py
```

which allows it to be automatically loaded on call of any module within the `demo.src` package.

```python
"""Set config IOs"""
__all__ = ["input_config", "raw_config", "processed_config"]

import logging
import os

from demo.src.config import environment
from demo.src.config.environment import ENVIRONMENT, RESOURCES
from dynamicio.config import IOConfig

logging.basicConfig(level=logging.INFO)
logging.getLogger("kafka").setLevel(logging.WARNING)


input_config = IOConfig(
    path_to_source_yaml=(os.path.join(RESOURCES, "definitions/input.yaml")),
    env_identifier=ENVIRONMENT,
    dynamic_vars=environment,
)
raw_config = IOConfig(
    path_to_source_yaml=(os.path.join(RESOURCES, "definitions/raw.yaml")),
    env_identifier=ENVIRONMENT,
    dynamic_vars=environment,
)
processed_config = IOConfig(
    path_to_source_yaml=(os.path.join(RESOURCES, "definitions/processed.yaml")),
    env_identifier=ENVIRONMENT,
    dynamic_vars=environment,
)

```

On loading, `IOConfig` will load the respective configs for all resources in the form of a multi-layered dictionary, e.g.,
for `actual`:

```python
import demo.src.config.environment

{
    "FOO": {
        "sample": {
            "type": "local",
            "local": {
                "file_path": f"{demo.src.config.environment.TEST_RESOURCES}/data/input/foo.csv",
                "file_type": "csv",
            },
        },
        "actual": {
            "type": "s3",
            "s3": {
                "bucket": f"{demo.src.config.environment.S3_YOUR_INPUT_BUCKET}",
                "file_path": "data/foo.h5",
                "file_type": "hdf"
            }
        },
    }
}
```

Then, depending on the value of the `env_identifier` parameter, the respective sub-dictionary is returned. For example,
with:

```python
foo_io = input_config.get(source_key="FOO")
```

and with `env_identifier="actual"`, the output would be:

```python
"type": "s3",
"s3": {
    "bucket": f"{demo.src.config.environment.S3_YOUR_INPUT_BUCKET}",
    "file_path": "data/foo.h5",
    "file_type": "hdf"
}
```

#### Step 4: Loading the data resources

To load a resource, you will need to generate instances of subclasses of `from dynamicio import UnifiedIO` class. Note
that the `UnifiedIO` class operates as an abstract class and cannot be used for instantiating objects.

You will need to implement your own subclasses for each of the inputs you care to load. You can do this in the `io.py`
module, under `src/resources`:

```shell
.
├── src
│   ├── __init__.py
│   ├── resources
│   │   └── io.py

```

The file looks like this:

```python
"""Responsible for configuring io operations for input data."""
# pylint: disable=too-few-public-methods
__all__ = ["InputIO", "StagedFoo", "StagedBar"]

from demo.src.resources.schemas.raw import StagedBarModel, StagedFooModel
from sqlalchemy.ext.declarative import declarative_base

from dynamicio import UnifiedIO, WithLocal, WithPostgres, WithS3File
from dynamicio.core import SCHEMA_FROM_ARGUMENT, DynamicDataIO

Base = declarative_base()


class InputIO(UnifiedIO):
    """UnifiedIO subclass for the input and processed datasets; each instance takes its Pandera model."""

    schema = SCHEMA_FROM_ARGUMENT


class StagedFoo(WithS3File, WithLocal, DynamicDataIO):
    """IO class for staged foos: it needs S3 and local mixins, so the model is fixed on the class."""

    schema = StagedFooModel


class StagedBar(WithLocal, WithPostgres, DynamicDataIO):
    """IO class for staged bars: it needs local and postgres mixins, so the model is fixed on the class."""

    schema = StagedBarModel

```

Instances of the `DynamicDataIO` class can either inherit directly from `UnifiedIO` (e.g. `InputIO` inherits from `UnifiedIO`) or user can choose the mixins they want to use 
(e.g. `StagedFoo` inherits from `WithS3File` and `WithLocal` mixins and needs to inherit from `DynamicDataIO`; note that Python's method resolution order (MRO) kicks in to address polymorphic conflicts--i.e. order matters). 
Also, all instances of the `DynamicDataIO` **must** define a class `schema`. The schema is a Pandera model, either fixed on the class (`schema = StagedFooModel`, see `StagedFoo`), supplied per instance (`schema = SCHEMA_FROM_ARGUMENT`, see `InputIO` and Step 4.1), or read from the YAML resource definition (`schema = SCHEMA_FROM_FILE`, Step 4.1b).

**N.B.** For convenience's sake and to reduce the need of boilerplate code, using a single class definition like `InputIO` is recommended (this way all your datasets can be loaded with instances of the same class). 
However, if you need to use different mixins for different datasets, you can do so by defining a class for each dataset (e.g. `StagedFoo` and `StagedBar`). For such a shared class, set `schema = SCHEMA_FROM_ARGUMENT` and pass the model per instance, `InputIO(source_config=..., schema=Foo)`.
You only need your own class per dataset when the schema is fixed on the class, as in `StagedFoo`. The dataset's name always comes from the model's `Config.name` (upper-cased, e.g. `staged_foo` becomes `STAGED_FOO`).

##### Step 4.1. Pandera models

Schemas are declared in Python as [Pandera](https://pandera.readthedocs.io) `DataFrameModel` classes. Resource definitions
stay in YAML; only the schema moves into code. Pandera is the *declaration* layer: dynamicio still runs the validations and
emits the metrics, so error payloads and `METRIC` log lines are identical to the YAML schemas'.

```python
import pandera.pandas as pa
from pandera.typing import Series

from demo.src.config.environment import LOWER_THAN_LIMIT


class Foo(pa.DataFrameModel):
    column_a: Series[str] = pa.Field(nullable=True, unique=True, metadata={"dynamicio": {"metrics": ["Counts"]}})
    column_b: Series[str] = pa.Field(nullable=False, metadata={"dynamicio": {"metrics": ["CountsPerLabel"]}})
    column_c: Series[float] = pa.Field(nullable=True, gt=1000)
    column_d: Series[float] = pa.Field(nullable=True, lt=LOWER_THAN_LIMIT, metadata={"dynamicio": {"metrics": ["Min", "Max", "Mean", "Std", "Variance"]}})

    class Config:
        name = "foo"  # the dataset name in metric lines is Config.name upper-cased; without it, the class name is used
```

Use the model as a class attribute when the IO class serves one dataset (`schema = Foo`), or let one class serve many by
taking the model per instance:

```python
from dynamicio.core import SCHEMA_FROM_ARGUMENT


class InputIO(UnifiedIO):
    schema = SCHEMA_FROM_ARGUMENT


foo_df = InputIO(source_config=input_config.get(source_key="FOO"), schema=Foo, apply_schema_validations=True, log_schema_metrics=True).read()
```

The `schema=` argument takes precedence over the class attribute; omitting it with `SCHEMA_FROM_ARGUMENT` raises `SchemaNotFoundError`.

How `pa.Field` arguments map onto dynamicio's validations:

| `pa.Field(...)` | dynamicio validation |
|---|---|
| `nullable=False` | `has_no_null_values` |
| `unique=True` | `has_unique_values` |
| `gt` / `ge` / `lt` / `le` | `is_greater_than` / `..._or_equal` / `is_lower_than` / `..._or_equal` |
| `in_range={"min_value", "max_value", "include_min", "include_max"}` | `is_between` |
| `isin=[...]` | `is_in` |
| `metadata={"dynamicio": {"validations": {...}, "metrics": [...]}}` | anything else, e.g. `has_acceptable_percentage_of_nulls`, and all metrics |

Two traps when writing models by hand:

- **Always set `nullable=` explicitly.** Pandera's default is `nullable=False`, which adds `has_no_null_values`; the YAML schemas allowed nulls unless the validation was declared.
- **Always set `include_min` / `include_max` on `in_range`.** Pandera includes the bounds by default; dynamicio's `is_between` excludes them.

YAML schemas (`SCHEMA_FROM_FILE`) and dict schemas keep working.

##### Step 4.1b. `SCHEMA_FROM_FILE`

`from dynamicio.core import SCHEMA_FROM_FILE` is a unique dynamic(i/o) object used as a placeholder.
It is used to indicate that a schema is provided as part of a _resource definition_.

For example:

```yaml
---
FOO:
  sample:
  ...
  actual:
  ...
  schema:
    file_path: "[[ RESOURCES ]]/schemas/input/foo.yaml"
```

`foo.yaml` is effectively a schema definition and looks like this:

```yaml
---
name: foo
columns:
  column_a:
    type: "object"
    validations:
      has_unique_values:
        apply: true
        options: {}
    metrics:
      - Counts
  column_b:
    type: "object"
    validations:
      has_no_null_values:
        apply: true
        options: {}
    metrics:
      - CountsPerLabel
  column_c:
    type: float64
    validations:
      is_greater_than:
        apply: true
        options:
          threshold: 1000
    metrics: []
  column_d:
    type: float64
    validations:
      is_lower_than:
        apply: true
        options:
          threshold: 1000
    metrics:
      - Min
      - Max
      - Mean
      - Std
      - Variance
```

The file is quite self-explanatory. The format is:

`DataSet`:

- `Column`
  - `type`
  - `validations`
  - `metrics`

For a dataset, each of the desired columns are dictated here, along with their
designated `dtypes`. The `columns` are used to filter out undesired columns in an optimal manner.
This means that it will happen on loading for `*.csv` and `*.parquet` files as well as when
interacting with a database, but will happen post-loading in the case of `*.h5` or `*.json`.

`dtypes` are then used to validate the types of the columns. If types don't match, `dynamic(i/o)`
will attempt to cast them and will issue a `WARNING`. If casting does not work either, it will
raise a `ColumnsDataTypeError`.

`validations` and `metrics` are there to document the user's expectations of the quality of the dataset.
They can be automatically applied on loading or on writing out.

Specifically, you can use the following **validations**:

- `has_unique_values`  # no options
- `has_no_null_values`  # no options
- `has_acceptable_percentage_of_nulls`
- `is_in`:
  ```yaml
    validations:
      is_in:
        apply: true
        options:
          categorical_values:
            - class_a
            - class_b
            - class_c
          match_all: false # true by default, if false, then the column unique categoricals must be equal to the acceptable ones, else they must be a subset
  ```
- `is_greater_than`
  ```yaml
    validations:
      is_greater_than:
        apply: true
        options:
          threshold: 1000
  ```
- `is_greater_than_or_equal`  # same as `is_greater_than`
- `is_lower_than`  # same as `is_greater_than`
- `is_lower_than_or_equal`  # same as `is_greater_than`
- `is_between`  # same as `is_greater_than`
  ```yaml
    validations:
      is_between:
        apply: true
        options:
          lower: 0
          upper: 1000
          include_left: false
          include_right: true # false by default, so bounds are excluded unless you ask
  ```

and **metrics**:

- `Min`
- `Max`
- `Mean`
- `Std`
- `Variance`
- `Counts`
- `UniqueCounts`
- `CountsPerLabel`

imposed as per below:
```shell
  column_c:
    type: float64
    validations: {}
    metrics:
      - Min
      - Max
      - Mean
      - Std
      - ...
```

Note that you can also use dynamic fields to define validations, e.g. see `LOWER_THAN_LIMIT` in the file below: 
```yaml
---
name: bar
columns:
  column_a:
    type: "object"
    validations:
      has_unique_values:
        apply: true
        options: {}
    metrics:
      - Counts
  column_b:
    type: "object"
    validations:
      has_no_null_values:
        apply: true
        options: {}
    metrics:
      - CountsPerLabel
  column_c:
    type: float64
    validations:
      is_greater_than:
        apply: true
        options:
          threshold: 1000
    metrics: []
  column_d:
    type: float64
    validations:
      is_lower_than:
        apply: true
        options:
          threshold: "[[ LOWER_THAN_LIMIT ]]"
    metrics:
      - Min
      - Max
      - Mean
      - Std
      - Variance
```
Similar to resource definitions, this value needs to be defined in `config/environment.py`

##### Step 4.2. Use the dynamicio cli
The `dynamicio` cli can be used to automatically generate schema definitions for you, provided either a path to
a dataset (`json`, `parquet`, `hdf`, `csv`) or to a directory. Here is how you can use it:

```shell
usage: dynamicio [-h] (--batch | --single) -p PATH -o OUTPUT

Generate dataset schemas

optional arguments:
  -h, --help            show this help message and exit
  --batch               used to generate multiple schemas provided a datasets directory.
  --single              used to generate a schema provided a single dataset.
  -p PATH, --path PATH  the path to the dataset/datasets-directory.
  -o OUTPUT, --output OUTPUT
                        the path to the schemas output directory.
```

The generated schema definitions will not have any validations or metrics automatically selected for you.

###### Migrating YAML schemas to Pandera models

```shell
dynamicio migrate src/resources/schemas -o src/resources/schemas --scan src
```

This converts every YAML schema under the path into Pandera models, one module per sub-directory of the YAML schemas (so
`schemas/stage/*.yaml` becomes `schemas/stage.py`: the grouping you already had, by runner and input or output, carries over; this is the
recommended layout, with one models module per runner stage or definitions file rather than a single catch-all module). Pass `-o schemas.py` instead to get one module. Class names come from the file names, `Config.name` keeps the
schema's `name`, so metric lines do not change, and checks that each generated model compiles back to a schema equivalent to its source.
It also writes `MIGRATION_PROMPT.md` next to the module, listing the models and every remaining call site (`SCHEMA_FROM_FILE`,
dict schemas, `schema:` blocks in resource definitions) with file and line. Hand that prompt to an LLM, or follow it yourself:
set `schema = <Model>` or `schema = SCHEMA_FROM_ARGUMENT` with `schema=<Model>` on construction, and delete the `schema:` blocks from the
definitions. Dynamic variables (`[[ NAME ]]`) in the YAML become plain names marked `# TODO` in the module, to be replaced by imports.
Dict-style schemas are not converted automatically. The `demo/` project was migrated this way.

##### Step 4.3a: Loading from `S3`

To then load from `S3` you simply do:

```python
    foo_df = InputIO(source_config=input_config.get(source_key="FOO"), schema=Foo, apply_schema_validations=True, log_schema_metrics=True).read()
```

which will load the `FOO` dataset from the `actual` definition (here an `S3` object) as a dataframe. With the environment set to `sample` the same call reads the local sample file instead.

##### Step 4.3b: Loading from `Postgres`

Likewise to `S3` resources, `postgres` resources need the same number of options to be defined for their loading.

When the dataset has a schema (a Pandera model or a YAML/dict schema), dynamicio infers a SQL data model from it rather than requiring you to define one.
This data model defines the table, the columns and their respective SQL types.

To, then, load from `postgres` you simply do:

```python
    bar_df = InputIO(source_config=input_config.get(source_key="BAR"), schema=Bar, apply_schema_validations=True, log_schema_metrics=True).read()
```

which will load the table defined for `BAR` as a dataframe.

##### Step 4.3c: Loading from `Athena`

`athena` resources need an `s3_output` location for query results and a query, either in the resource definition or
passed when instantiating the IO class (which takes precedence):

```python
    events_df = InputIO(source_config=input_config.get(source_key="EVENTS_ATHENA"), query="SELECT id, ts FROM events LIMIT 10").read()
```

where `InputIO` mixes in `WithAthena`. See [Athena I/O](#athena-i-o) for the available options.

#### Step 5: Writing out

Sinking data is done in a very similar way. You need to:

1. Define your output resource definitions, in our case in `raw.yaml`

```yaml
---
STAGED_FOO:
  sample:
    type: "local"
    local:
      file_path: "[[ TEST_RESOURCES ]]/data/raw/staged_foo.parquet"
      file_type: "parquet"
  actual:
    type: "s3"
    s3:
      bucket: "[[ S3_YOUR_OUTPUT_BUCKET ]]"
      file_path: "live/data/raw/staged_foo.parquet"
      file_type: "parquet"

STAGED_BAR:
  sample:
    type: "local"
    local:
      file_path: "[[ TEST_RESOURCES ]]/data/raw/staged_bar.parquet"
      file_type: "parquet"
  actual:
    type: "s3"
    s3:
      bucket: "[[ S3_YOUR_OUTPUT_BUCKET ]]"
      file_path: "live/data/raw/staged_bar.parquet"
      file_type: "parquet"
```

2. You need to define the respective dynamic values found in your resource definitions
   in your `src/config/environment.py`
3. You need to create an instance of the `IOConfig` class for the `raw.yaml` in the
   `__init__.py` file (we already did this).
4. Define the additional `DynamicDataIO` subclasses in the `src/resources/io.py` module,
   dictating through the schema the list of columns, and their types (also used for schema validation).
5. Finally, instantiate instances of those subclasses and call the `.write()` method,
   passing in the dataframe you want to write out, e.g.

`demo/src/runners/staging.py`:
```python
    ...
    StagedFoo(source_config=raw_config.get(source_key="STAGED_FOO"), **constants.TO_PARQUET_KWARGS).write(foo_df)
    StagedBar(source_config=raw_config.get(source_key="STAGED_BAR")).write(bar_df)
```

Notice that you can pass all `pandas` options to write out, when for instance you are writing out `parquet`.

`demo/src/config/constants.py`:
```python
# Parquet
TO_PARQUET_KWARGS = {
    "use_deprecated_int96_timestamps": False,
    "coerce_timestamps": "ms",
    "allow_truncated_timestamps": True,
}
```

Of course this is not a problem as parquet is the format used by both resources in either environment. This is not always the case however. See in 
`demo/src/resources/definitions/processed.yaml`:

```yaml
---
...

FINAL_BAR:
  sample:
    type: "local"
    local:
      file_path: "[[ TEST_RESOURCES ]]/data/processed/final_bar.parquet"
      file_type: "parquet"
    options:                                      <---- Options for Local writing as parquet
      use_deprecated_int96_timestamps: true
      coerce_timestamps: "ms"
      allow_truncated_timestamps: false
      row_group_size: 1000000
  actual:
    type: "kafka"
    kafka:
      kafka_server: "[[ KAFKA_SERVER ]]"
      kafka_topic: "[[ KAFKA_TOPIC ]]"
    options:                                      <---- Options for writing to a Kafka Topic
      compression_type: "snappy"
      max_in_flight_requests_per_connection: 10
      batch_size: 262144
      request_timeout_ms: 60000 # 60s
      buffer_memory: 134217728  # 128MB
```

Here, we have a case where different options need to be used for each environment as it deals with a different source. This is gracefully managed through resource 
definitions passing these arguments in the `options` key per environment.    

#### Step 6: Full Code

The full code for the loading module in our example would live under:

```shell
├── __init__.py
├── src
...
│   ├── runners
│   │   └── staging.py
```

and looks like:

```python
"""Add module docstring...."""
import logging

from demo.src import input_config, raw_config
from demo.src.config import constants
from demo.src.resources.io import InputIO, StagedBar, StagedFoo

logger = logging.getLogger(__name__)


def main() -> None:
    """The entry point for the Airflow Staging task.

    Returns:
        Void function.
    """
    # LOAD DATA
    logger.info("Loading data from live sources...")

    bar_df = InputIO(source_config=input_config.get(source_key="BAR"), schema=Bar, apply_schema_validations=True, log_schema_metrics=True).read()
    foo_df = InputIO(source_config=input_config.get(source_key="FOO"), schema=Foo, apply_schema_validations=True, log_schema_metrics=True).read()

    logger.info("Data successfully loaded from live sources...")

    # TRANSFORM  DATA
    logger.info("Apply transformations...")

    # TODO: Apply your transformations

    logger.info("Transformations applied successfully...")

    # SINK DATA
    logger.info("Begin sinking data to staging area:")
    StagedFoo(source_config=raw_config.get(source_key="STAGED_FOO"), **constants.TO_PARQUET_KWARGS).write(foo_df)
    StagedBar(source_config=raw_config.get(source_key="STAGED_BAR")).write(bar_df)
    logger.info("Data staging is complete...")

```
### Utilising `asyncio`
`Dynamic(i/o)` supports use of `asyncio` to speed up `I/O bound` operations through leveraging multithreading. 

An example can be found in the second of the two demo tasks, namely, the `transform.py` task.
```python
"""Add module docstring...."""
import asyncio
import logging

import demo.src.config.environment
from demo.src import processed_config, raw_config
from demo.src.resources.io import InputIO, StagedBar, StagedFoo

logger = logging.getLogger(__name__)


async def main() -> None:
    """The entry point for the Airflow Staging task.

    Returns:
        Void function.
    """
    # LOAD DATA
    logger.info("Loading data from live sources...")

    [bar_df, foo_df] = await asyncio.gather(
        StagedBar(source_config=raw_config.get(source_key="STAGED_BAR")).async_read(),
        StagedFoo(source_config=raw_config.get(source_key="STAGED_FOO")).async_read()
    )

    logger.info("Data successfully loaded from live sources...")

    # TRANSFORM  DATA
    logger.info("Apply transformations...")

    # TODO: Apply your transformations

    logger.info("Transformations applied successfully...")

    # SINK DATA
    logger.info(f"Begin sinking data to staging area: S3:{demo.src.config.environment.S3_YOUR_OUTPUT_BUCKET}:live/data/raw")
    await asyncio.gather(
        InputIO(source_config=processed_config.get(source_key="FINAL_FOO"), schema=FinalFoo, apply_schema_validations=True, log_schema_metrics=True).async_write(foo_df),
        InputIO(source_config=processed_config.get(source_key="FINAL_BAR"), schema=FinalBar, apply_schema_validations=True, log_schema_metrics=True).async_write(bar_df),
    )
    logger.info("Data staging is complete...")

```
In short, you simply need to utilise the `async_read()` or the `async_write()` methods instead, plus await and gather your calls.  

## Testing Locally

After following the above documentation, at this point it should be clear that `dynamic(i/o)` is optimised for enabling
seamless local testing for your pipelines.

Simply by configuring your `ENVIRONMENT`'s default value to `sample` and provided that you have the required tests data
sources in the necessary directories, it becomes very simple to test your pipelines end-to-end in seconds, eliminating
the need to deploy your dags and wait for their tasks to be provided access to processing resources.

All you need to do is mimic the order of execution of your tasks, running them in procedural order.

In the case of our example, you would have to:

1. Add the necessary data under `tests/data`:

```shell
└── tests
    ├── __init__.py
    ├── conftest.py
    ├── config
    │   ├── __init__.py
    │   └── constants.py
    ├── data
    │   ├── input
    │   │   ├── bar.parquet
    │   │   └── foo.csv
    │   ├── processed
    │   │   └── expected
    │   │       ├── final_bar.parquet
    │   │       └── final_foo.parquet
    │   └── raw
    │       └── expected
    │           ├── staged_bar.parquet
    │           └── staged_foo.parquet
    ├── resources
    │   ├── __init__.py
    │   └── test_prefix_schema_evolution.py
    ├── runners
    │   ├── __init__.py
    │   ├── conftest.py
    │   ├── test_staging.py
    │   ├── test_transform.py
    │   └── utils
    │       ├── __init__.py
    │       └── test_runner_selection.py
    └── test_pipeline.py
```

2. Implement an end-to-end, black-box style test that simply generates the expected data output given a
   specific input (deleting the output after the assertion)

An example end-to-end test in this case, for a single airflow task would look like:

```python
"""An example pipeline to showcase how dynamicio can bt used for setting up a local e2e testing!"""
# pylint: disable=missing-module-docstring, missing-class-docstring, missing-function-docstring, unused-argument, too-few-public-methods
# noqa
import os

import pandas as pd
import pytest

from demo.src import processed_config, raw_config
from demo.src.runners import staging, transform


class TestPipeline:
    """Example e2e test."""

    @pytest.mark.end_to_end
    def test_dag_with_mock_sample_input_data(
        self,
        expected_staged_foo_df,
        expected_staged_bar_df,
        expected_final_foo_df,
        expected_final_bar_df,
    ):
        """Showcases how you can leverage dynamicio to read local data for fast feedback when you want to run your pipelines locally."""
        # Given
        # The src/resources/definitions/input.yaml

        # When
        staging.main()
        transform.main()

        # Then
        try:
            assert expected_staged_foo_df.equals(pd.read_parquet(raw_config.get(source_key="STAGED_FOO")["local"]["file_path"]))
            assert expected_staged_bar_df.equals(pd.read_parquet(raw_config.get(source_key="STAGED_BAR")["local"]["file_path"]))
            assert expected_final_foo_df.equals(pd.read_parquet(processed_config.get(source_key="FINAL_FOO")["local"]["file_path"]))
            assert expected_final_bar_df.equals(pd.read_parquet(processed_config.get(source_key="FINAL_BAR")["local"]["file_path"]))
        finally:
            os.remove(raw_config.get(source_key="STAGED_FOO")["local"]["file_path"])
            os.remove(raw_config.get(source_key="STAGED_BAR")["local"]["file_path"])
            os.remove(processed_config.get(source_key="FINAL_FOO")["local"]["file_path"])
            os.remove(processed_config.get(source_key="FINAL_BAR")["local"]["file_path"])

```

##  Last notes

Hope this was helpful.

A few things worth remembering:

- `apply_schema_validations` and `log_schema_metrics` are both `False` by default: without them a schema only selects and casts columns.
- On read, dynamicio casts, then validates, then logs metrics. On write it keeps only the schema's columns, validates, logs metrics, then casts.
- A failed validation raises `dynamicio.errors.SchemaValidationError`; its `.message` is a dict of validation name to details, with every violation collected, not just the first.
- Metrics are emitted as `METRIC` JSON log lines on the `dynamicio.metrics` logger, so they are easy to parse in your log dashboards.
- See `CHANGELOG.md` for what changed between releases and `docs/adr/` for the reasoning behind the larger decisions.

Please do reach out with comments and your views about how the library or the docs can be improved, and by all means, come along and contribute to our project!

