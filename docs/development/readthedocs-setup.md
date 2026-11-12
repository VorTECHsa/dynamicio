# Read the Docs setup

Checklist for the repository owner. The repository is already configured (`.readthedocs.yaml`, `mkdocs.yaml`,
`docs/requirements.txt`); these are the steps that only the Read the Docs dashboard can do. The hosted build has not been verified
by the maintainers of this change: it needs this setup first.

**No step creates or stores a credential in this repository or in GitHub Actions.** Connecting GitHub is an OAuth/GitHub
App authorisation done in the browser. Do not create a `READTHEDOCS_TOKEN` secret: the webhooks need none.

## One-time setup

| # | Step | Needs credentials? |
|---|---|---|
| 1 | Create a [Read the Docs Community](https://app.readthedocs.org/) account (a free account is enough) | Account sign-up |
| 2 | **Connect GitHub**: choose "Sign in with GitHub" or connect it under *Settings, Connected Services* | GitHub authorisation, in the browser |
| 3 | Install the **Read the Docs GitHub App** and grant it access **only to `VorTECHsa/dynamicio`**, not to all repositories | GitHub organisation owner approval |
| 4 | **Import the project**: *Add project*, pick `VorTECHsa/dynamicio`. The configuration file is detected as `.readthedocs.yaml`. Pick the project name (it sets the URL); note the URL and see step 14 | No |
| 5 | *Settings, Advanced*: **Default branch** `master` (it backs the `latest` version) | No |
| 6 | *Automation rules* (below) | No |
| 7 | *Settings, Advanced*: **Default version**: see "Default version" below | No |
| 8 | *Settings, Advanced*: enable **Build pull requests for this project** | No |
| 9 | Open a pull request and check the Read the Docs status check on it, which links to the preview | No |
| 10 | Optional: *Domains* for a custom domain | DNS access |
| 11 | *Notifications*: add an email (or webhook) for **build failures** | No |
| 12 | Optional: PyPI trusted publishing, see [Releasing](releasing.md#publishing-credentials) | PyPI project owner |
| 13 | Verify the first build of `latest` (below) | No |
| 14 | Once the URL is confirmed: replace the `Documentation` URL in `pyproject.toml`, add the URL to the README, and set the optional repository variable `DOCS_BASE_URL` (*Settings, Secrets and variables, Variables*, a variable, not a secret) so GitHub releases link to the version's documentation | No |

If step 3 is skipped, the project can still be imported manually, but pull request previews and the commit status
need the GitHub App integration (webhooks are created by the App).

## Versions and automation rules

Read the Docs maps git refs to versions:

- **`latest`** is the default branch (`master`): development documentation, possibly unreleased.
- **`stable`** is created and moved by Read the Docs to the highest active, non-pre-release tag. It never moves to a release candidate.
- **`vX.Y.Z`** is the documentation of an individual tag. Building a tag documents exactly that commit, so a version stays as released
  even when `master` moves on.

In *Admin, Automation rules* add, in this order:

| Order | Match | Custom match | Version type | Action |
|---|---|---|---|---|
| 1 | Custom match | `^v[0-9]+\.[0-9]+\.[0-9]+$` | Tag | Activate version |

Final tags are activated and built when pushed. Release candidates (`vX.Y.Z-rc.N`) do not match, so they stay inactive and can
never become `stable`. Do not use "set as default version" as an action: it would promote every tag, not only the latest stable.

Older tags (before the first release that contains `.readthedocs.yaml`) cannot be built: Read the Docs builds a tag from
its own commit, which has no configuration. Leave them inactive.

### Default version

Until a tag containing `.readthedocs.yaml` is released there is no `stable`, so keep the default version **`latest`**. After the first
release, confirm `stable` exists (*Versions*) and change the default version to **`stable`**. From then on the site root opens the newest stable release.

## First build

1. Trigger a build of `latest` (*Builds, Build version*, or push to `master`). It runs `mkdocs build` in strict mode with the pinned
   `docs/requirements.txt` on Python 3.12, with no credentials and no access to any service.
2. Open the site, check the version selector, search, the API reference and the changelog page.
3. If it fails, the build log shows the same warning `make generate-docs` reports locally.

## At the next release

After pushing the tag (see [Releasing](releasing.md)), open *Builds* and check the new `vX.Y.Z` build, that the version is active,
and that `stable` points to it. If it did not appear, run *Sync versions* in the *Versions* page.

## Diagnosing a failed documentation build

- Read the build log. Reproduce it with `make generate-docs`; the Read the Docs build uses the same configuration and the same pinned tools.
- A stale `docs/requirements.txt` is caught in CI (`make docs-requirements`).
- A Read the Docs build never publishes to PyPI and PyPI never waits for it: see [Releasing](releasing.md#recovering-a-failed-release).
