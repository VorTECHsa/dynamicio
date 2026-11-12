# Installation

```shell
pip install dynamicio
# or, in a uv project
uv add dynamicio
```

dynamicio requires Python 3.10 or newer; CI runs 3.10 to 3.14. The published ranges of its dependencies are wide on
purpose (`pandas>=2.2.2`, `pandera>=0.34.1`, `pyarrow>=16`, `awswrangler>=3.9`, ...), so dynamicio can sit in an existing
environment. The development environment is kept on pandas 2 and SQLAlchemy 2.0.

All the backends ship with the package: there are no extras to install for S3, Postgres, Athena or Kafka. Their
libraries are only exercised when a resource of that type is used. A `local` resource needs no credentials, no network
and no services, which is why the examples in this documentation use it.

## Credentials

dynamicio never asks for credentials itself.

- **AWS (S3, Athena)**: the usual `boto3` credential chain (environment variables, profile, IAM role).
- **Postgres and Kafka**: the connection values in the resource definition, usually injected from your environment
  (see [Environments and configuration](../guides/configuration.md)).

## Check the installation

```shell
python -c "import dynamicio; print(dynamicio.__version__)"
dynamicio --help
```

## Contributing

To work on dynamicio itself, see [Contributing](../development/contributing.md).
