# Releasing

A release is a git tag on a commit that already contains its changelog entry. Nothing is committed back to `master` by
a workflow. One trigger exists: pushing a `v*` tag to the "Build and Publish" workflow.

## Versioning

- Versions follow [Semantic Versioning](https://semver.org/): patch for fixes, minor for features, major for breaking changes.
- Tags are `vX.Y.Z` (stable) or `vX.Y.Z-rc.N` (release candidate). The version is derived from the tag at build time
  (`hatch-vcs`): there is no version in the repository to edit.
- Release candidates are for remote or post-deployment runs of the published package. Do not publish one just to test locally.

## Changelog entries

`CHANGELOG.md` is written for users of the library. It deliberately differs from [Keep a Changelog](https://keepachangelog.com/)
in two ways, because the publishing workflow depends on them:

- Entries go under the heading of the version the branch will release, `## vX.Y.Z`, written with the branch (patch for
  fixes, minor for features, major for breaking changes). If the branch has no such heading yet, add it above the latest
  release. There is **no `## Unreleased` section**: the heading is a prediction, and the tag decides.
  If another release took the version first, or the bump was wrong, rename the heading in the pull request before merging.
- Entries are prefixed with their type (`feat:`, `fix:`, `docs:`, `chore:`, ...) instead of Added/Changed/Fixed sections, and headings carry no date.

A pull request that changes user-visible behaviour (a public API, an option, an I/O result, a dependency range or a
supported Python version) adds an entry that says what changes for the user and, for a breaking change, what to do about
it. Internal-only changes (refactors, tests, CI) may omit one. Do not invent entries for history that was not recorded.

## Preparing a release

1. Decide the version from the changes since the last tag.
2. Check that the `## vX.Y.Z` section of `CHANGELOG.md` is complete and curated. This section becomes the GitHub release
   notes, so write it for readers of the release page.
3. Merge the pull request into `master`.
4. Pull master, and confirm the pull succeeded (a failed pull tags a stale commit):

    ```shell
    git checkout master && git pull --ff-only
    make check-release TAG=vX.Y.Z      # tag format and changelog section
    ```

5. Tag an explicit commit and push the tag:

    ```shell
    git tag -a vX.Y.Z <sha> -m "vX.Y.Z"
    git push origin vX.Y.Z
    ```

## What the tag triggers

1. `validate-release`: the tag format, a non-empty changelog section (stable tags), and that the commit is on `master` (stable tags).
2. Style checks, the test matrix (Python 3.10 to 3.14), the dependency-range jobs and the strict documentation build.
3. `package`: one wheel and one sdist, validated with `twine check --strict` and checked against the tag (file names and wheel metadata must carry the tag's version).
4. `publish`, only when everything above passed: uploads exactly those distributions to PyPI and creates or updates the
   GitHub release from the changelog section, with links to PyPI, the version's documentation, the changelog and the source.
   Release candidates are published to PyPI but get no GitHub release.

Read the Docs builds the same tag through its own webhook (see [Read the Docs setup](readthedocs-setup.md)). It is
independent of the workflow: **PyPI publication and the documentation build are not atomic.**

## After the release

- Check the workflow run, PyPI (`pip index versions dynamicio`) and the GitHub release.
- Check the Read the Docs build of the tag, and that `stable` moved to it.

## Recovering a failed release

| Failure | Recovery |
|---|---|
| Validation, tests or documentation build fail before publishing | Nothing was published. Fix on `master`, delete the tag locally and on `origin` (`git push --delete origin vX.Y.Z`), and tag the fixed commit. Only delete a tag that was never published |
| PyPI upload failed or the run was interrupted | Re-run the failed jobs. `--check-url` makes the upload skip files PyPI already holds |
| GitHub release step failed | Re-run the `publish` job: it edits the release if it exists |
| PyPI has the version but the code is wrong | A version is never reused. Yank it on PyPI if needed, fix, and release the next patch |
| Read the Docs build of the tag failed (PyPI is fine) | The failure shows in the Read the Docs build list and as a failed status check on the commit. A tag is built from its own commit, so a fix on `master` does not change it. If the cause is on the hosted side (an outage, a transient error), press **Rebuild** on the version. If the tag's content is at fault, release a patch version |
| `stable` did not move | Check that the tag matches the activation rule and the version is active and not hidden; run **Sync versions** from the project's Versions page |

The strict documentation build runs in this repository's CI before publishing, so a documentation failure on a tag that
passed CI is almost always an environment problem on the hosted side.

## Publishing credentials

The workflow publishes with the `PYPI_API_TOKEN` repository secret through `uv publish --token`. PyPI Trusted Publishing is the
better mechanism (no long-lived token), but it needs a one-time setup on PyPI by the project owner:

1. On PyPI, open the project's **Publishing** settings and add a trusted publisher for GitHub:
   owner `VorTECHsa`, repository `dynamicio`, workflow `build_and_publish.yml`, and (recommended) an environment named `pypi`.
2. In the `publish` job, add `id-token: write` to `permissions`, `environment: pypi`, and replace the upload command with
   `uv publish --trusted-publishing always --check-url https://pypi.org/simple/ dist/*`. Create the `pypi` environment in the repository settings
   (optionally with required reviewers).
3. After one successful release, delete the `PYPI_API_TOKEN` secret and revoke the token on PyPI.

Until then the existing token keeps working, so the workflow is unchanged in behaviour.
