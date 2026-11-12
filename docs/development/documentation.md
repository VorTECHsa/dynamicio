# Writing documentation

The documentation is Markdown in `docs/`, built with [MkDocs](https://www.mkdocs.org/) and the
[Material](https://squidfunk.github.io/mkdocs-material/) theme, with the API reference generated from docstrings by
[mkdocstrings](https://mkdocstrings.github.io/). It is hosted on Read the Docs (see
[Read the Docs setup](readthedocs-setup.md)). The configuration is `mkdocs.yaml`.

## Build and preview locally

```shell
uv sync --group docs        # once; the docs group is pinned in uv.lock
make serve-docs             # live preview at http://127.0.0.1:8000
make generate-docs          # the strict build CI runs: any warning fails it
```

Nothing needs credentials or network access to the services dynamicio talks to.

## Rules

- **Strict build.** `strict: true` is set, and Read the Docs also fails on warnings. Fix the cause of a warning (a broken
  link, a missing anchor, a docstring griffe cannot parse) instead of silencing it.
- **Navigation.** Add every new page to `nav:` in `mkdocs.yaml`; a page that is not listed is a warning.
- **Examples are tested.** A `python` block that follows a `<!-- doctest -->` comment is executed by
  `tests/test_docs_examples.py`. The blocks of a page run in order, in one namespace, in an empty directory. Use `LOCAL`
  resources only, so no example needs credentials. Blocks without the comment (S3, Postgres, Kafka, fragments) are not run, so keep them
  short and copy them from code that is.
- **Only document what exists.** dynamicio executes on pandas. Polars is not supported; do not describe it as available,
  and do not add benchmarks or performance claims that are not backed by an ADR.
- **No private details.** The documentation is public: no bucket names, hostnames, account ids or internal systems.
- **Images** live in `docs/images/` and are referenced relatively. Check that nothing references an image before you delete it.

## API reference

`docs/reference/index.md` lists modules with `::: package.module`. Docstrings are Google style. To document a new public
module add one line there. Names starting with an underscore are filtered out. Every public function needs `Args:`
and `Returns:` entries that match its signature; the `Returns:` line reads `type: description` when the function has no return annotation.

## Documenting a new feature

1. Update the guide the feature belongs to (and `docs/guides/sources-and-formats.md` for a new source or format).
2. If the feature is an option of a reader or writer, make sure it is on that reader's `allow_options` list, and document it.
3. Add a runnable `<!-- doctest -->` example when it can run locally.
4. Add a [changelog](releasing.md#changelog-entries) entry.
5. Run `make generate-docs` and `uv run pytest tests/test_docs_examples.py`.

## Pinned tooling

`docs/requirements.txt` is what Read the Docs installs. It is exported from `uv.lock`; after changing the `docs`
dependency group, run `uv lock && make docs-requirements` and commit both. CI fails when they disagree.

## Choice of generator

MkDocs with Material was kept because the repository already used it, Read the Docs supports MkDocs natively, and
mkdocstrings (which generates the API reference) is proven with it. Two things to watch:

- **Material for MkDocs** is in maintenance mode, and its authors have announced that MkDocs 2.0 will drop the plugin
  system. The docs group stays on MkDocs 1.x (`mkdocs<2`) until a successor is chosen.
- **[Zensical](https://zensical.org/)** is the successor from the Material authors and reads `mkdocs.yml`. It was evaluated and not adopted: its
  compatibility page does not confirm support for the mkdocstrings Python handler, and the API reference depends on it.
  Revisit when it does. The migration should then be a change of the build command and of the docs dependencies
  (`docs/requirements.txt`, `.readthedocs.yaml`), because the content is plain Markdown.
