# Testing

```shell
uv sync                          # dev environment from uv.lock
uv run pytest                    # unit tests with mocked backends, plus the documentation examples
uv run pytest demo/tests         # the demo pipeline
uv run pytest -m floci tests/integration   # S3, Postgres and Kafka on floci, needs Docker
```

The checks CI runs before anything is published:

```shell
uv run black --check dynamicio tests
uv run flake8 --verbose dynamicio tests
uv run pylint -v dynamicio tests            # must stay 10.00/10
uv run yamllint -v dynamicio tests
uv run pytest -vv
uv run pytest -vv -m floci tests/integration
make generate-docs                          # strict documentation build
```

## What is covered

- `tests/`: unit tests with mocked backends (moto for S3). `ENVIRONMENT=LOCAL` tests do not exercise the S3 code paths: when
  changing a reader or writer, add a test against a `CLOUD` resource definition with an in-memory object store (see
  `_json_object_store` in `tests/test_mixins/test_s3_mixins.py`).
- `tests/integration/`: the S3, Postgres and Kafka I/O against [floci](https://github.com/floci-io/floci), a local AWS
  emulator. A package fixture starts floci in Docker with an RDS Postgres instance and an MSK (Redpanda) cluster, and
  removes them after the tests. floci, Postgres and Redpanda have pinned images. The fixture needs the Docker socket,
  because floci starts Postgres and Redpanda as their own containers. The default `pytest` run leaves these tests out
  (`-m 'not floci'` in `pyproject.toml`), and the CI job `integration` runs them. Without Docker, they skip, and they fail when the environment variable `CI` is set.
  floci accepts a `DeleteObjects` call with more than 1000 keys, which S3 refuses, so the 1000-key batches stay a moto
  unit test. floci does not emulate Athena, so Athena stays a mock.
- `tests/test_docs_examples.py`: runs the `<!-- doctest -->` examples of the documentation.
- `tests/test_release_tools.py`: the tag, changelog and distribution checks of `.github/tools/release_tools.py`.
- `demo/tests`: the demo pipeline end to end, using the test harness.

Unit tests mock AWS, so they cannot prove an I/O change works against a real account. Before opening a PR that changes S3
or Athena behaviour, verify it against a development account with the working tree (see `AGENTS.md`).

## Dependency ranges

CI also installs the lowest and the newest versions of the declared dependencies and runs the suite
(`dependency-ranges`). Raise a floor in `pyproject.toml` only when that job proves it necessary.
