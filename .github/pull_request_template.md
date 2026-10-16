<!--
INSTRUCTIONS:
- Keep sections that apply, delete sections that don't.
- For Change Type and Risk Level: keep only the one that applies, delete the rest.
- Evidence/Performance sections: only include if needed beyond standard tests, otherwise delete.
- Simple PRs can be just Summary + What Changed + Checklist.
- Make it easy for reviewers: remove noise, keep signal.
-->

## Summary

Short description of the change in 2-4 sentences: what changed, why, and the key decision made.

## Links

RND-<Fill in JIRA number> for autoreference and linking to the ticket.

## Change Type

[Keep only the one that applies, delete the rest]
- Tests / coverage only
- Pure refactor (no behavioral change)
- Small behavior change (bounded, localized)
- New feature / mixin / I/O backend
- High-risk change (breaking interface, dependency removal, cross-cutting mixin change)

## Risk Level

[Keep only the one that applies, delete the rest]
- 🟢 Low: cosmetic, docs, or local change
- 🟠 Medium: logic or behavior change with bounded impact (e.g. one mixin, one file type)
- 🔴 High: breaking change, dependency swap affecting multiple consumers, or change to a code path
  shared across mixins (`core.py`, `utils.py`, schema validation)

## What Changed

- ...
- ...
- ...

## What Must Remain True

[Optional: delete if obvious or not applicable]
- Public I/O interface (`.read()` / `.write()`) is unchanged for existing consumers
- Schema validation behavior is unchanged
- No silent dtype/column drift introduced by the change
- Existing YAML resource definitions remain valid without edits

## Review Guidance

[Optional: delete if review path is obvious]
- Review this first: ...
- Focus on: ...
- Skip: ...

## Checklist

The following won't apply in all cases — mark `N/A` next to the point if not needed.

- [ ] Appropriate unit tests added/updated
- [ ] Module, function, class docstrings updated
- [ ] Comments added to PR where necessary
- [ ] Interface documentation updated
- [ ] Semantic commits used for this PR, so a CHANGELOG can be generated from them (don't delete
  your local branch! when tagging a new release from master)

**If this changes I/O behavior (mixins, readers/writers, engine backends):**
- [ ] Tested against a real/representative dataset, not just unit fixtures
- [ ] Downstream consumers (other repos depending on `dynamicio`) considered for breaking changes
- [ ] Backwards compatibility of existing YAML resource definitions verified

## Performance Evidence

[Optional: delete if not a performance-motivated change. Fill in if claiming a speed/behavior
improvement — see `docs/` for an example report with methodology and charts.]

- What was compared: ...
- Sample size / data used: ...
- Result: ...
- Link to full report (if one exists under `docs/`): ...

## Rollback

- Rollback path: revert this PR / re-pin the previous dependency version
- Recovery steps: ...

## Release-Steps

- [ ] Merge PR
- [ ] Release new tag
