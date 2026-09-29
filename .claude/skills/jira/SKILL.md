---
name: jira
description: Interact with Vortexa's Jira (RND project). Use when searching tickets, creating tickets/epics, managing backlogs, or bulk-updating work items.
---

# Jira Assistant for Vortexa RND

Safely interact with Jira for the RND project, scoped to your team's tickets.

> This skill is for RND-XXXXX project workitems only. JSM Operations alerts (items prefixed `[New Relic]`, `[PagerDuty]`, etc., at `/jira/ops/alerts`) live in a different object model under `api.atlassian.com/jsm/ops/...` and are out of scope here.

## Configuration

**First, load your team configuration from `~/.config/vortexa/jira-assistant.yaml`.**

```bash
cat ~/.config/vortexa/jira-assistant.yaml
```

If the file doesn't exist, help the user create it:

1. Create the directory and file:

```bash
mkdir -p ~/.config/vortexa
cat > ~/.config/vortexa/jira-assistant.yaml << 'EOF'
# Jira Assistant Configuration
user_email: "your.name@vortexa.com"
pod_name: "YOUR-POD-NAME"
pod_id: "YOUR-POD-ID"

# Pod IDs:
#   10857 = ADT
#   10858 = ADT - Ariane
#   10859 = ADT - Artemis
#   10860 = ADT - Voyager
#   10862 = DPT - Lucky Number
#   10864 = DPT - Skakas
#   10865 = DPT - Slime Mould
#   10866 = DPT - Turingeries
#   10867 = DST
#   10869 = Research
#   11547 = DPT - Platypus
EOF
```

2. Ask the user for their pod name and email, then update the file with correct values.

The config provides:

- `user_email` — For API authentication
- `pod_name` — Your team (e.g., "ADT - Voyager")
- `pod_id` — Jira ID for your pod (e.g., "10860")

---

## GUARDRAILS — READ FIRST

This Jira instance is used by the entire Vortexa company. Mistakes can impact other teams.

### Scope Restrictions

- **ONLY operate on tickets in the user's configured pod** (`"R&D Group name" = "<pod_name>"`)
- **NEVER modify tickets belonging to other pods/teams**
- **NEVER modify Initiatives** (RND-17xxx series) — only link epics to them
- **NEVER delete tickets** — use status transitions instead (e.g., "Won't do")

### Before Any Bulk Operation

1. **Always show the user** the list of tickets that will be affected
2. **Get explicit confirmation** before proceeding
3. **Test on a single ticket first** before applying to many
4. **Verify the JQL query** returns only intended tickets

### Query Safety Checklist

Before running any modifying query, verify:

- [ ] JQL includes `"R&D Group name" = "<pod_name>"`
- [ ] JQL includes `project = RND`
- [ ] Result count matches expectations
- [ ] No tickets from other teams in results

### Safe Operations (OK to proceed)

- Reading/searching tickets
- Creating new epics (under user's pod)
- Moving tickets between user's pod epics
- Updating ticket status within normal workflow

### Dangerous Operations (ALWAYS ask first)

- Any bulk update (>5 tickets)
- Changing parent links
- Transitioning tickets to terminal states (Done, Won't do)
- Any operation on tickets outside user's configured pod

### Always Read Comments

Comments often contain critical context:

- Decisions made after the description was written
- Blockers and dependencies discovered
- Conversations with other teams
- Reasons a ticket was parked or deprioritized

```bash
acli jira workitem comment list --key RND-XXXXX --paginate
```

**Before suggesting refinements or changes to a ticket, always read its comments first.**

### Follow slack links

Vortexa uses Slack extensively. Many Jira tickets contain links to Slack conversations. Follow them — they will contain important context about the ticket. If the Slack MCP is enabled, use it. If not, just mention that the conversation may have additional context and continue — don't try to install or configure MCP automatically.

---

## Authentication

### acli (Atlassian CLI) — optional

- If installed via Homebrew, lives at `/opt/homebrew/bin/acli` (Apple Silicon) or `/usr/local/bin/acli` (Intel Mac).
- Authenticated via OAuth: `acli jira auth status`.
- If `acli` is **not** installed (the common case on this repo's host), fall back to curl + `JIRA_API_TOKEN` for every operation in this skill. Don't try to install `acli` automatically — just use the REST API.

### API Token (for all operations when acli isn't available)

The token must be available as the env var `JIRA_API_TOKEN`. **Always use `$JIRA_API_TOKEN` in curl commands** — never prompt the user with `read -s -p` (it doesn't work in Claude's non-interactive Bash tool and will hang or fail).

Resolution order when you need the token:

1. Use `$JIRA_API_TOKEN` if already exported (the common case — most engineers source it from `~/.zshrc`).
2. If unset, try resolving from macOS Keychain: `export JIRA_API_TOKEN=$(security find-generic-password -a "$USER" -s JIRA_API_TOKEN -w 2>/dev/null)`.
3. If still unset, stop and tell the user that you need a token. You **cannot** generate it yourself — it has to be created in a browser at https://id.atlassian.com/manage-profile/security/api-tokens by the user.

```bash
# Use in API calls — JIRA_API_TOKEN should already be in the environment
curl -s -X GET \
  -u "<user_email>:${JIRA_API_TOKEN}" \
  -H "Content-Type: application/json" \
  "https://vortexa.atlassian.net/rest/api/3/issue/RND-XXXXX"
```

**Token-related failure modes:**

- `401 Unauthorized` — token unset, expired, revoked, or paired with the wrong email. Have the user check `echo $JIRA_API_TOKEN` and regenerate if needed.
- `403 Forbidden` on attachments (`"You do not have permission to view attachment with id: <id>"`) — happens for some attachments uploaded by automation against JSM Support tickets and copied across to RND tickets. Try `acli` (OAuth) instead, or download from the Jira web UI.

---

## Quick Ticket Creation

For simple, fast ticket creation (especially for DPT team workflows), use the REST API directly with sensible defaults.

### When to use quick creation

- Creating a single ticket with standard fields
- Tech debt, bug fixes, or feature tracking
- When you already know the summary and basic details

### Quick creation pattern

> **Note:** `JIRA_API_TOKEN` should already be exported in the shell (most commonly from `~/.zshrc`). If it isn't, see the Authentication section above for the resolution order. Do NOT prompt for it interactively.

```bash
# Determine issue type (Bug, Story, Task, Epic)
# - Bug: Defects, broken functionality
# - Story: New features, improvements, tech debt, refactoring
# - Task: Operational work, investigations, one-off actions

# For Story (tech improvements, features)
curl -s -X POST https://vortexa.atlassian.net/rest/api/3/issue \
  -H "Authorization: Basic $(echo -n "<user_email>:${JIRA_API_TOKEN}" | base64)" \
  -H "Content-Type: application/json" \
  -d '{
    "fields": {
      "project": {"key": "RND"},
      "issuetype": {"name": "Story"},
      "summary": "Brief title describing the change",
      "description": {
        "type": "doc",
        "version": 1,
        "content": [
          {
            "type": "paragraph",
            "content": [{"type": "text", "text": "Brief 2-3 sentence summary.\n\nSee full details: link-to-repo-file-or-doc"}]
          }
        ]
      },
      "priority": {"name": "Medium"},
      "labels": ["label1"],
      "customfield_10414": [{"value": "DPT - Turingeries"}],
      "customfield_11245": {"value": "Flows"},
      "customfield_11209": {"value": "Tech Debt"}
    }
  }' | jq -r 'if .key then "✅ Created: https://vortexa.atlassian.net/browse/\(.key)" else "❌ Error: \(.)" end'
```

### Quick creation guidelines

**Keep descriptions short:**

- 2-3 sentence summary
- Link to detailed docs (repo files, Google Docs, PRs)
- JIRA uses Atlassian Document Format (ADF), not markdown

**Issue type selection:**

- **Bug**: Defects, broken functionality, incorrect behavior
- **Story**: New features, improvements, tech debt, refactoring (for DPT tech improvements, always use Story)
- **Task**: Operational work, investigations, research, one-off actions
- **Epic**: Large initiatives spanning multiple stories/tasks

**Required fields by issue type:**

For **Story**:

- `customfield_10414` (R&D Group name) = `"DPT - Turingeries"`
- `customfield_11245` (Product line) = `"Flows"`
- `customfield_11209` (Intent category) = `"Tech Debt"` or `"Feature Enhancement"`

For **Task** or **Bug**:

- `customfield_10414` (R&D Group name) = `"DPT - Turingeries"`

See the **API Token** subsection of [Authentication](#authentication) above for `$JIRA_API_TOKEN` resolution and 401/403 troubleshooting.

---

## Common Operations

### Search Tickets

```bash
# All tickets for your pod
acli jira workitem search \
  --jql "project = RND AND \"R&D Group name\" = \"<pod_name>\"" \
  --paginate --fields "key,summary,status"

# Orphan tickets (no parent epic)
acli jira workitem search \
  --jql "project = RND AND \"R&D Group name\" = \"<pod_name>\" AND parent IS EMPTY AND type NOT IN (Epic, Initiative)" \
  --paginate

# Tickets under a specific epic (include pod scope for safety)
acli jira workitem search \
  --jql "project = RND AND \"R&D Group name\" = \"<pod_name>\" AND parent = RND-XXXXX" \
  --paginate
```

### View Ticket

```bash
acli jira workitem view RND-XXXXX --fields "*all" --json
```

### View Attachments

```bash
# List attachments
acli jira workitem view RND-XXXXX --fields "*all" --json | jq -r '.fields.attachment'

# Download attachment (requires API token; uses $JIRA_API_TOKEN from environment)
curl -s -L -o /tmp/attachment.png \
  -u "<user_email>:${JIRA_API_TOKEN}" \
  "https://vortexa.atlassian.net/rest/api/3/attachment/content/ATTACHMENT_ID"
```

### Transition Ticket

```bash
acli jira workitem transition --key "RND-XXXXX" --status "Done" --yes
```

### Create Epic

Use curl (not acli) for epics — acli's `--from-json` is buggy with custom fields.

```bash
curl -s -X POST \
  -u "<user_email>:${JIRA_API_TOKEN}" \
  -H "Content-Type: application/json" \
  "https://vortexa.atlassian.net/rest/api/3/issue" \
  -d '{
    "fields": {
      "project": {"key": "RND"},
      "issuetype": {"name": "Epic"},
      "summary": "Epic Title",
      "description": {
        "type": "doc",
        "version": 1,
        "content": [
          {"type": "paragraph", "content": [{"type": "text", "text": "Description"}]}
        ]
      },
      "customfield_10414": [{"id": "<pod_id>"}],
      "customfield_11209": {"id": "11667"},
      "customfield_11245": {"id": "11709"}
    }
  }'
```

### Link Ticket to Epic / Epic to Initiative

```bash
curl -s -X PUT \
  -u "<user_email>:${JIRA_API_TOKEN}" \
  -H "Content-Type: application/json" \
  "https://vortexa.atlassian.net/rest/api/3/issue/RND-XXXXX" \
  -d '{"fields": {"parent": {"key": "RND-YYYYY"}}}'
```

---

## Required Fields Reference

For the complete field ID mapping, query the Jira API directly (`/rest/api/3/field`) or refer to a colleague's copy of this skill from another repo that ships `references/field-ids.md`.

### Key Custom Fields

| Field           | Custom Field ID     |
| --------------- | ------------------- |
| R&D Group name  | `customfield_10414` |
| Intent category | `customfield_11209` |
| Product line    | `customfield_11245` |

### Intent Category Options

| ID    | Value               |
| ----- | ------------------- |
| 11664 | KTLO                |
| 11665 | Tech Debt           |
| 11667 | Feature Enhancement |
| 11668 | New Feature         |
| 11669 | New Product         |

### Product Line Options

| ID    | Value                       |
| ----- | --------------------------- |
| 11704 | Onshore Inventories (Crude) |
| 11705 | Freight                     |
| 11706 | Pricing                     |
| 11707 | Flows                       |
| 11709 | Cross-product               |

---

## Company Initiatives

When creating an epic, it should be linked to a parent initiative. Query the current initiatives:

```bash
acli jira workitem search --filter 12593 --paginate --fields "key,summary"
```

Then link the epic to the appropriate initiative using the parent field (see "Link Ticket to Epic" section).

---

## Notes & Gotchas

- Use curl (not acli `--from-json`) for creating issues with custom fields
- Search API: use POST `/rest/api/3/search` with a JSON body containing `jql`, not the old GET endpoint
- HTTP 204 = success for PUT requests (no content returned)
- Parent link: use `parent` field, not `customfield_10018`
