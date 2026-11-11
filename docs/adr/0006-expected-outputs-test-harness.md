# 0006. Test harness: expected outputs declared in resource definitions

- Status: Accepted (PR #120)
- Date: 2026-11-11

## Context

Pipelines built on dynamicio were tested by hand-written glue per project: patch the environment, run the task, read the files back with
pandas and compare them with a reference. Each project reimplemented the comparison, the cleanup of generated files, the handling of
parametrised paths and the regeneration of references, and each did so differently. Failures stopped at the first mismatch.

## Decision

1. The reference file of an output is declared where the output is: an optional `expected: {path, file_type, options}` key per environment in the
   resource definition. Reading and writing ignore it, so existing definitions behave byte-for-byte as before (see the compatibility rules).
2. `dynamicio.testharness` runs the task in a local environment and lazily compares each generated output with its `expected` file, with plain
   pandas, so no schema or cast can hide a difference. All mismatches are collected and raised once as `OutputMismatchError`; an error raised by the
   task itself is never masked. `Expectations` makes the comparison stricter or looser per session or per check; `DYNAMICIO_UPDATE_EXPECTED=1`
   regenerates references for review in the diff.
3. The test states its environment (`ExpectedOutputs(environment=...)`); every `IOConfig` that defines it is switched for the `with` block and restored
   after. Without it the default is the first environment a definition lists, whatever the process runs in. No patching of `env_identifier`.
4. `check_all(config)` verifies every output of a config that declares `expected`. A hand-written list of `check` calls drifts: an output added to
   the definition stays silently unverified, the worst failure of a regression test. The definition is already the single source of truth for which
   outputs have a reference. `check` and `check_each` remain for explicit control; the choice is the user's.
5. Because `check_all` decides at runtime, a green run reports what it verified. `SessionReport` is printed at the end of a session and shown in
   pytest's terminal summary by a small `pytest11` plugin (green matched, red failed, yellow updated or skipped; `-p no:dynamicio` turns it off), and
   `OutputMismatchError` also lists the outputs that matched. Hiding the list was the cost of `check_all`; the report removes it without giving up
   the guard.

## Alternatives considered

- Explicit `check_all(expect=[...])` guard: restores readability in the test file but brings back the list to maintain. Rejected; the report gives
  visibility without duplication.
- Retracting `check_all`: simplest API, but reintroduces forgotten outputs. Rejected.
- Making the report opt-in only: a passing run is exactly when nobody reads anything, so the default must inform. Opt-out through the plugin switch.

## Consequences

- Reference files live in version control next to sample data; a change of output shows in the diff.
- The pytest plugin is loaded by the entry point whenever dynamicio is installed, which imports the package before coverage starts: this repository
  disables it in its own pytest config (`-p no:dynamicio`) and tests its hook directly.
- Output is plain stdout plus pytest markup; nothing is emitted when a session checked nothing.
- Pandas only, like validation and metrics (see ADR 0005).
