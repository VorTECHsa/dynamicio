---
name: create-pr
description: Create a trust-optimized pull request following verification-first principles. Analyzes changes, generates guarantees, and produces a PR that focuses reviewers on bounded risk rather than raw diffs. Use when creating a PR that needs strong trust signals.
---

# Create Trust-Optimized Pull Request

Create a pull request that follows verification-first principles: smaller scope, clearer guarantees, and explicit risk assessment.

## Workflow

### Step 1: Analyze the changes

Understand what's actually changing:

```bash
git status
git diff --stat
git diff master...HEAD
git log master..HEAD --oneline
```

**Key questions to answer:**
- What is the scope of this change? (single feature, refactor, bug fix, etc.)
- How many files are touched?
- Are there logical boundaries that could split this into smaller PRs?
- What subsystems are affected?

If the changeset is large (>10 files or >500 lines), **recommend decomposition first**:
- Suggest splitting into: tests-first PR, then implementation PR
- Or: refactor PR, then behavioral change PR
- Or: multiple incremental feature PRs

### Step 2: Classify change type and risk

Determine the **Change Type** (pick one):
- Tests / coverage only
- Pure refactor (no behavioral change)
- Small behavior change (bounded, localized)
- New feature increment
- High-risk stateful / distributed change

Determine the **Risk Level** (pick one):
- 🟢 **Low**: cosmetic, local, no runtime impact
- 🟠 **Medium**: logic or behavior change with bounded impact
- 🔴 **High**: stateful, distributed, concurrency-sensitive, recovery-critical, or integration-sensitive

### Step 3: Identify what must remain true

For refactors and behavior-preserving changes, state the **invariants**:

Examples:
- "Ordering is preserved"
- "Behavior is unchanged outside the `PaymentProcessor` boundary"
- "No event loss or duplication"
- "Public API contract remains the same"
- "Data pipeline output schema unchanged"

For new features, state the **boundary**:
- "Only affects the new `/v2/analytics` endpoint"
- "Isolated behind feature flag `ENABLE_NEW_SCHEDULER`"

If invariants are non-obvious, **require evidence**:
- Property tests added
- Integration tests that verify round-tripping
- Diff showing that public interfaces remain unchanged

### Step 4: Generate PR description

Use the repository's `.github/pull_request_template.md` if available. Populate it with:

**Summary** (2-4 sentences):
- What changed
- Why it changed
- Key decision made

**Change Type**: (from Step 2)

**Risk Level**: (from Step 2, with risk emoji)

**What Changed**: Bulleted list of concrete changes, not file names. Example:
- Added retry logic to S3 upload with exponential backoff
- Refactored event serialization to use Avro schema registry
- Fixed race condition in cache invalidation

**What Must Remain True**: (from Step 3) Delete section if obvious or N/A.

**Review Guidance**: (optional, delete if obvious)
- "Review this first: tests in `test_processor.py` show the invariant holds"
- "Focus on: error handling in `handle_failure()`"
- "Skip: auto-generated Avro classes"

**Checklist**: Adapt the template checklist. Mark realistic items. Delete sections that don't apply.

**QA Evidence**: Only include if needed. Examples:
- "Tested with 1M events in dev, no data loss"
- "Dashboard shows p99 latency unchanged"

**Rollback**: How to undo this change safely.

### Step 5: Create the PR

Ensure the branch is pushed:

```bash
git push -u origin HEAD
```

Create the PR using `gh`:

```bash
gh pr create --title "{short title}" --body "$(cat <<'EOF'
{generated PR body from Step 4}
EOF
)"
```

**Title guidelines**:
- Short (< 70 characters)
- Start with type prefix if team uses them: `feat:`, `fix:`, `refactor:`, `test:`
- Focus on *what*, not *how*

### Step 6: Present the result

Show the user:
1. The PR URL
2. A summary of the risk level and scope
3. Any recommendations (e.g., "Consider splitting this into smaller PRs" or "Add property tests to strengthen guarantees")

**Do not automatically add reviewers or labels** — let the user decide.

## Important Notes

### Decomposition heuristics

Recommend splitting when:
- >10 files touched
- Mix of refactor + behavioral change
- Tests can be added separately before the change
- Feature can be delivered incrementally

### Risk signals to flag

Warn the user if:
- No tests were added for a behavioral change
- High-risk classification but no rollback plan
- Claims "behavior unchanged" but no tests verify it
- Large diff with weak guarantees

### Verification emphasis

The goal is to move review burden from "scan the diff" to "check the guarantees." Strong PRs include:
- Explicit invariants
- Evidence (tests, type safety, static analysis)
- Narrow scope
- Clear rollback path

If the PR lacks these, suggest improvements before creating it.

### Template fallback

If `.github/pull_request_template.md` doesn't exist, use this minimal structure:

```markdown
## Summary
{2-4 sentences}

## Risk Level
🟢/🟠/🔴 {reasoning}

## What Changed
- ...

## What Must Remain True
- ... (or delete if obvious)

## Checklist
- [ ] Tests pass
- [ ] Reviewed locally

## Rollback
{how to undo}
```

## Edge Cases

- If the branch has no commits ahead of master, warn and exit
- If there are uncommitted changes, ask whether to commit them first
- If `gh` CLI is not authenticated, prompt: `gh auth login`
- If the PR already exists, show the existing PR URL and ask if they want to update it
