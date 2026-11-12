# Reading data

## Step 4: Loading the data resources

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


## Step 4.3a: Loading from `S3`

To then load from `S3` you simply do:

```python
    foo_df = InputIO(source_config=staging_input_config.get(source_key="FOO"), schema=Foo, apply_schema_validations=True, log_schema_metrics=True).read()
```

which will load the `FOO` dataset from the `DEV` definition (here an `S3` object) as a dataframe. With the environment set to `LOCAL` the same call reads the local sample file instead.

## Step 4.3b: Loading from `Postgres`

Likewise to `S3` resources, `postgres` resources need the same number of options to be defined for their loading.

When the dataset has a schema (a Pandera model or a YAML/dict schema), dynamicio infers a SQL data model from it rather than requiring you to define one.
This data model defines the table, the columns and their respective SQL types.

To, then, load from `postgres` you simply do:

```python
    bar_df = InputIO(source_config=staging_input_config.get(source_key="BAR"), schema=Bar, apply_schema_validations=True, log_schema_metrics=True).read()
```

which will load the table defined for `BAR` as a dataframe.

## Step 4.3c: Loading from `Athena`

`athena` resources need an `s3_output` location for query results and a query, either in the resource definition or
passed when instantiating the IO class (which takes precedence):

```python
    events_df = InputIO(source_config=staging_input_config.get(source_key="EVENTS_ATHENA"), query="SELECT id, ts FROM events LIMIT 10").read()
```

where `InputIO` mixes in `WithAthena`. See [Athena I/O](sources-and-formats.md#athena-io) for the available options.
