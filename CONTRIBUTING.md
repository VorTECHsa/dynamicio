## 1. Local environment setup

`dynamicio` uses [uv](https://docs.astral.sh/uv/) for Python, virtual environment and dependency management.

1. Install `uv` ([instructions](https://docs.astral.sh/uv/getting-started/installation/)), e.g. `brew install uv`.
2. From the repo root run:
```shell
>> make dev-env-setup
```
This installs the OS libraries needed on macOS (via `brew`), creates a `.venv` in the repo's root directory from `uv.lock`
with the `dev` and `docs` dependency groups, and installs the `pre-commit` hooks. uv downloads the Python version named
in `.python-version` if you do not already have it. The supported range is `>=3.10` (CI runs 3.10 to 3.14).

You do not need to activate the environment: prefix commands with `uv run` (e.g. `uv run pytest`). To activate it anyway:
```shell
>> source .venv/bin/activate
>> deactivate
```

### Managing dependencies
- Runtime dependencies live in `[project] dependencies` and tooling in `[dependency-groups]` in `pyproject.toml`.
- Add or remove with `uv add <pkg>` / `uv remove <pkg>` (`--group dev` for tooling), which also updates `uv.lock`.
  Always commit `pyproject.toml` and `uv.lock` together.
- `make update-lock` re-resolves everything to the latest allowed versions. CI installs with the lock, so a stale lock is a CI failure.

### 1.1. PyCharm
1. Run `make dev-env-setup`.
2. Go to `Preferences > Python Interpreter`, choose `Add Interpreter > Existing` and select:
```shell
>> /Users/{your-pc-name}/path/to/dynamicio/.venv/bin/python
```

## 2. Committing
Based on how the repository has been configured, when you stage any new files to be committed, they will first be
inspected by `isort`, `black`, `flake8`, `yamllint` and `pylint` by means of `pre-commit`. If errors are found by
`black`, you can simply re-add updated files to staging and try to commit again. Else, you will need to make some
changes, stage the updated files and then try to commit again.

Hooks have not been configured for docs generation and for running tests, as docs can skew the purpose of a commit
and should be handled independently, while tests are not always part of a commit either. You can manually run these
checks by using the below make instructions:

```shell
>> make check-linting
>> make run-tests
>> make generate-docs
```

Commit and pull request titles are semantic: `type(RND-12345): subject` (`feat`, `fix`, `docs`, `chore`, ...).

## 3. Documentation and the changelog

- The documentation lives in `docs/` and is built with MkDocs (`make serve-docs` to preview, `make generate-docs` for the strict build CI runs).
  See "Writing documentation" for the rules, including how examples are tested.
- Add a `CHANGELOG.md` entry for every user-visible change, under the `## vX.Y.Z` heading of the version the branch will
  release (patch for fixes, minor for features, major for breaking changes). There is no `## Unreleased` section. Internal-only changes may omit an entry.
- A new option of a reader or writer must be on its `allow_options` list, and documented.

## 4. Releasing

Versions are not stored in the repo: the package version is derived from the git tag at build time (`hatch-vcs`). After the
pull request is merged, tag the merge commit on `master` (`vX.Y.Z`, or `vX.Y.Z-rc.N` for a release candidate from a feature branch). The "Build and Publish"
workflow then validates, builds and publishes it. The steps, and how to recover from a failed release, are in "Releasing".
