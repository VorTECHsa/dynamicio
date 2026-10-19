# AGENTS.md: working on dynamicio as an agent

`dynamicio` is a **public** open-source library (pandas I/O driven by YAML resource definitions). Everything you write
(commits, PR titles/bodies, issues, code comments, docs, screenshots) is public: never mention private repositories,
internal systems, employers' infrastructure, bucket names, hostnames or credentials.

## Setup

Tooling is [uv](https://docs.astral.sh/uv/); there is no Poetry, `requirements.txt` or `setup.py`.

```shell
uv sync                    # .venv from uv.lock (dev group); add --group docs for mkdocs
uv run pytest              # always prefix commands with `uv run`, never activate or pip install
```

- Python `>=3.10,<3.13`; CI runs 3.10, 3.11 and 3.12. `.python-version` is the local default.
- Dependencies: `uv add <pkg>` / `uv add --group dev <pkg>` / `uv remove <pkg>`. Commit `pyproject.toml` and `uv.lock` together.
  CI uses `uv sync --locked`, so a stale lock fails.
- `[tool.uv] constraint-dependencies` keeps the dev environment on pandas 2 / SQLAlchemy 2.0 (not yet supported on 3 / 2.1).

## Checks (all must pass before you push)

CI ("Build and Publish" workflow) runs exactly these:

```shell
uv run black --check dynamicio tests       # line length 185; `uv run black dynamicio tests` to fix
uv run flake8 --verbose dynamicio tests    # import order (I100) failures have blocked a release before
uv run pylint -v dynamicio tests           # must stay 10.00/10
uv run yamllint -v dynamicio tests
uv run pytest -vv                          # also: uv run pytest demo/tests
```

`make check-linting`, `make run-tests` and `make generate-docs` wrap the same commands. Pre-commit hooks run the linters
and tests through `uv run`.

## Code map

- `dynamicio/core.py`: `DynamicDataIO`, the base class that resolves a resource for an environment and applies schema validation.
- `dynamicio/mixins/with_*.py`: one mixin per backend (`with_local`, `with_s3`, `with_postgres`, `with_kafka`, `with_athena`).
  Options in resource YAML are passed through to pandas/awswrangler; `utils.allow_options` filters kwargs to what the target accepts.
- `dynamicio/config/`: YAML resource definition loading and pydantic models.
- `tests/`: unit tests with mocked backends. `demo/`: runnable example, with its own tests.

## Compatibility rules (read before touching I/O)

Downstream pipelines upgrade by a **version bump only**, so I/O behaviour must stay byte-compatible with the previous release.

- Default to the underlying library's own defaults (pandas/pyarrow). 8.1.0 broke consumers by defaulting S3 JSON to
  `lines=True` and `index=False`; 8.1.1 restored `lines=False` and a per-orient `index` default. Never "improve" defaults silently.
- When changing a reader or writer, add a test that runs against a **CLOUD** resource definition with an in-memory object store
  (see `_json_object_store` in `tests/test_mixins/test_s3_mixins.py`). `ENVIRONMENT=LOCAL` tests do not exercise the S3 paths.
- Add a `CHANGELOG.md` entry for every user-visible change, and update README/docs when options change.

## Releases

- The version comes from the git tag (`hatch-vcs`); `pyproject.toml` has no version. Never edit a version by hand.
- Pushing a `v*` tag runs style checks and tests, then builds and publishes to PyPI. Release candidates use `vX.Y.Z-rc.N`.
- Tag from **master after the PR is merged** (or an rc from the feature branch), and tag an explicit commit sha:
  `git tag -a vX.Y.Z <sha> -m "..."` then `git push origin vX.Y.Z`. Check `git pull` actually succeeded first; a failed pull
  silently tags a stale commit.
- Do not merge pull requests or publish a final release unless the maintainer asked you to; open the PR and hand over.

## Conventions

- Commit and PR titles are semantic: `type(RND-12345): subject` (types: feat, fix, perf, docs, test, chore, ci, ...); the PR title
  checker enforces it.
- Keep diffs focused; keep the surrounding style (Google-style docstrings, `pydocstyle` convention google).
