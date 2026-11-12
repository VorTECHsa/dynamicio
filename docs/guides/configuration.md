# Environments and configuration

## Step 2: Defining your environment variables

Also notice that paths to datasets are embedded with dynamic values identified with double squared brackets, e.g.
`[[ DEV_S3_OUTPUT_BUCKET ]]`. These can be defined in a module in your repository.

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

ENVIRONMENT = "LOCAL"
RESOURCES = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../resources")
TEST_RESOURCES = os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../tests")
DEV_S3_INPUT_BUCKET = None
DEV_S3_OUTPUT_BUCKET = None
PROD_S3_INPUT_BUCKET = None
PROD_S3_OUTPUT_BUCKET = None
DEV_KAFKA_BROKER = None
PROD_KAFKA_BROKER = None
KAFKA_TOPIC = None
DEV_DB_HOST = None
PROD_DB_HOST = None
DB_PORT = None
DB_NAME = None
DB_USER = None
DB_PASS = None
REFERENCE_DATA_STATE_KEY = None
LOWER_THAN_LIMIT = 1000
```

This module will be passed as an input parameter to instances of the `dynamicio.config.IOConfig` class. Let's cover
some of its variables:

- ```python
  ENVIRONMENT = "LOCAL"
  ```
  the environment your module runs in, one of the keys under each resource in the resource definitions (`LOCAL`, `DEV` or `PROD`). In the cloud, set it from
  an environment variable of the place where your module is executed from, e.g. `os.getenv("ENVIRONMENT", "LOCAL")`.
- ```python
  "TEST_RESOURCES": os.path.join(os.path.dirname(os.path.realpath(__file__)), "../../tests"),
  ```
  It is defined in the resource definitions, e.g.:

```yaml
---
FOO:
  LOCAL:
    type: "local"
    local:
      file_path: "[[ TEST_RESOURCES ]]/data/input/foo.csv"
      file_type: "csv"
  DEV:
    type: "s3"
    s3:
      bucket: "[[ DEV_S3_INPUT_BUCKET ]]"
      file_path: "data/foo.h5"
      file_type: "hdf"
```

and therefore needs to be defined here as well.

Any other dynamic variable (identified with the doubly squared brackets) defined in the resource definitions needs to
also be defined here and can be either statically or dynamically defined (i.e. hardcoded or defined as an environment
value).

## Step 3: Read in your resource definitions

Reading in the resources definitions can be done by means of instantiating instances of the `dynamicio.config.IOConfig`
class (the, so called, "source configs"). This is done in:

```shell
src
├── __init__.py
```

which allows it to be automatically loaded on call of any module within the `demo.src` package.

```python
"""Set config IOs."""
__all__ = ["staging_input_config", "staging_output_config", "transform_input_config", "transform_output_config"]

import logging
import os

from demo.src.config import environment
from demo.src.config.environment import ENVIRONMENT, RESOURCES
from dynamicio.config import IOConfig

logging.basicConfig(level=logging.INFO)
logging.getLogger("kafka").setLevel(logging.WARNING)


# One input and one output definition per runner.
staging_input_config = IOConfig(os.path.join(RESOURCES, "definitions/staging_input.yaml"), env_identifier=ENVIRONMENT, dynamic_vars=environment)
staging_output_config = IOConfig(os.path.join(RESOURCES, "definitions/staging_output.yaml"), env_identifier=ENVIRONMENT, dynamic_vars=environment)
transform_input_config = IOConfig(os.path.join(RESOURCES, "definitions/transform_input.yaml"), env_identifier=ENVIRONMENT, dynamic_vars=environment)
transform_output_config = IOConfig(os.path.join(RESOURCES, "definitions/transform_output.yaml"), env_identifier=ENVIRONMENT, dynamic_vars=environment)
```

On loading, `IOConfig` will load the respective configs for all resources in the form of a multi-layered dictionary, e.g.,
for `FOO`:

```python
import demo.src.config.environment

{
    "FOO": {
        "LOCAL": {
            "type": "local",
            "local": {
                "file_path": f"{demo.src.config.environment.TEST_RESOURCES}/data/input/foo.csv",
                "file_type": "csv",
            },
        },
        "DEV": {
            "type": "s3",
            "s3": {
                "bucket": f"{demo.src.config.environment.DEV_S3_INPUT_BUCKET}",
                "file_path": "data/foo.h5",
                "file_type": "hdf"
            }
        },
        # "PROD": ...
    }
}
```

Then, depending on the value of the `env_identifier` parameter, the respective sub-dictionary is returned. For example,
with:

```python
foo_io = staging_input_config.get(source_key="FOO")
```

and with `env_identifier="DEV"`, the output would be:

```python
"type": "s3",
"s3": {
    "bucket": f"{demo.src.config.environment.DEV_S3_INPUT_BUCKET}",
    "file_path": "data/foo.h5",
    "file_type": "hdf"
}
```
