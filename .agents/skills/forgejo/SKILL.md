---
name: forgejo
description: Manages repositories, issues, PRs, releases, and CI on Forgejo via REST API. TRIGGER when needing to create/edit issues, open PRs, view CI status, manage repos, or check releases.
allowed-tools: Bash(curl *som:3000*), Bash(python3 *), Bash(git *)
---

## Instance

The user's Forgejo instance is `som:3000` (Tailscale host, HTTP — no TLS).
Username is `psg24`.

## Authentication

The API token is stored in `~/.config/forgejo/token`. Always read it at the start of a session:

```bash
FJ_TOKEN=$(cat ~/.config/forgejo/token)
```

If the file doesn't exist, ask the user for a token. They can generate one at:
`http://som:3000/user/settings/applications`

**Auth header format:** `Authorization: token $FJ_TOKEN` — NOT `Bearer`. Forgejo uses
its own `token` scheme. `Bearer` will return 401/405.

### Quick auth check

```bash
FJ_TOKEN=$(cat ~/.config/forgejo/token)
curl -s -H "Authorization: token $FJ_TOKEN" "http://som:3000/api/v1/user" | python3 -c "
import json,sys; d=json.load(sys.stdin); print(f'Logged in as: {d[\"login\"]}')"
```

## IMPORTANT: No `fj` CLI — use `curl` + `python3` for everything

`fj` (forgejo-cli) is **not installed** on this system. All Forgejo operations use
`curl` for API calls and `python3 -c` for JSON parsing (no `jq` either).

### Standard API call pattern

```bash
FJ_TOKEN=$(cat ~/.config/forgejo/token)

# GET
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/ENDPOINT" | python3 -c "
import json,sys; d=json.load(sys.stdin); print(json.dumps(d, indent=2))"

# POST / PATCH / PUT / DELETE
curl -s -X POST -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"key": "value"}' \
  "http://som:3000/api/v1/ENDPOINT" | python3 -c "
import json,sys; d=json.load(sys.stdin); print(json.dumps(d, indent=2))"
```

### File upload pattern (creating/updating files in a repo)

```bash
# Base64-encode file content
CONTENT=$(python3 -c "
import base64
with open('/path/to/local/file', 'rb') as f:
    print(base64.b64encode(f.read()).decode())
")

# Create new file
curl -s -X POST -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"content\": \"$CONTENT\", \"message\": \"commit message\"}" \
  "http://som:3000/api/v1/repos/psg24/REPO/contents/path/to/file"

# Update existing file (requires SHA of current file)
curl -s -X PUT -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"content\": \"$CONTENT\", \"sha\": \"CURRENT_SHA\", \"message\": \"commit message\"}" \
  "http://som:3000/api/v1/repos/psg24/REPO/contents/path/to/file"
```

## Pagination

Most list endpoints return 30 items by default (max 50). Use `?page=1&limit=50`.
Check the `x-total-count` response header for totals.

## Git remote URLs

**Use HTTP clone URLs** (SSH requires key setup on the Forgejo instance):

```
http://som:3000/psg24/repo.git
```

The Forgejo instance reports SSH URLs as `ssh://psg24@som/psg24/repo.git` but
SSH auth may not be configured. Prefer HTTP with credential helper or token:

```bash
# Clone with token embedded (for automation)
git clone http://psg24:$FJ_TOKEN@som:3000/psg24/repo.git

# Or configure credential helper for som:3000
git config --global credential.http://som:3000.helper '!f() { echo "username=psg24"; echo "password=$(cat ~/.config/forgejo/token)"; }; f'
```

## Core API operations

### Repos

```bash
# List user's repos
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/user/repos?limit=50" | python3 -c "
import json,sys
for r in json.load(sys.stdin):
    print(f'{r[\"full_name\"]:40s} {r.get(\"description\",\"\")}')"

# Search all repos
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/repos/search?q=QUERY&limit=50" | python3 -c "
import json,sys; d=json.load(sys.stdin)
for r in (d.get('data', d) if isinstance(d, dict) else d):
    print(f'{r[\"full_name\"]:40s} {r.get(\"description\",\"\")}')"

# View repo info
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/repos/psg24/REPO" | python3 -c "
import json,sys; print(json.dumps(json.load(sys.stdin), indent=2))"

# Create repo
curl -s -X POST -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "my-repo", "description": "...", "private": true, "auto_init": true}' \
  "http://som:3000/api/v1/user/repos"

# Delete repo (destructive — confirm with user first)
curl -s -X DELETE -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/repos/psg24/REPO"

# List repo contents
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/repos/psg24/REPO/contents/PATH" | python3 -c "
import json,sys
items=json.load(sys.stdin)
if isinstance(items, list):
    for i in items: print(f'{i[\"type\"]:10s} {i[\"name\"]}')
else:
    import base64; print(base64.b64decode(items['content']).decode())"

# List branches
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/repos/psg24/REPO/branches" | python3 -c "
import json,sys
for b in json.load(sys.stdin):
    print(b['name'])"
```

### Issues

```bash
# Create issue
curl -s -X POST -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"title": "Bug title", "body": "Description"}' \
  "http://som:3000/api/v1/repos/psg24/REPO/issues"

# List open issues
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/repos/psg24/REPO/issues?state=open&limit=50" | python3 -c "
import json,sys
for i in json.load(sys.stdin):
    print(f'#{i[\"number\"]:4d} {i[\"state\"]:8s} {i[\"title\"]}')"

# View issue
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/repos/psg24/REPO/issues/42" | python3 -c "
import json,sys; d=json.load(sys.stdin)
print(f'#{d[\"number\"]} [{d[\"state\"]}] {d[\"title\"]}')
print(d.get('body',''))"

# List issue comments
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/repos/psg24/REPO/issues/42/comments" | python3 -c "
import json,sys
for c in json.load(sys.stdin):
    print(f'--- {c[\"user\"][\"login\"]} ({c[\"created_at\"]}) ---')
    print(c['body'][:200]); print()"

# Comment on issue
curl -s -X POST -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"body": "Comment text"}' \
  "http://som:3000/api/v1/repos/psg24/REPO/issues/42/comments"

# Close issue
curl -s -X PATCH -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"state": "closed"}' \
  "http://som:3000/api/v1/repos/psg24/REPO/issues/42"

# Edit issue
curl -s -X PATCH -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"title": "New title", "body": "New body"}' \
  "http://som:3000/api/v1/repos/psg24/REPO/issues/42"

# Search issues across repos
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/repos/psg24/REPO/issues?state=all&type=issues&q=search+term&limit=50"
```

### Pull Requests

```bash
# Create PR (push branch first!)
curl -s -X POST -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"title": "PR title", "body": "Description", "head": "feature-branch", "base": "main"}' \
  "http://som:3000/api/v1/repos/psg24/REPO/pulls"

# List open PRs
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/repos/psg24/REPO/pulls?state=open&limit=50" | python3 -c "
import json,sys
for p in json.load(sys.stdin):
    print(f'#{p[\"number\"]:4d} {p[\"state\"]:8s} {p[\"title\"]}')"

# View PR
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/repos/psg24/REPO/pulls/42" | python3 -c "
import json,sys; d=json.load(sys.stdin)
print(f'#{d[\"number\"]} [{d[\"state\"]}] {d[\"title\"]}')
print(f'  {d[\"head\"][\"label\"]} → {d[\"base\"][\"label\"]}')
print(f'  mergeable: {d.get(\"mergeable\",\"?\")}')
print(d.get('body',''))"

# View PR diff
curl -s -H "Authorization: token $FJ_TOKEN" \
  -H "Accept: text/plain" \
  "http://som:3000/api/v1/repos/psg24/REPO/pulls/42.diff"

# PR comments
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/repos/psg24/REPO/issues/42/comments"

# Comment on PR (PRs use the issues comment endpoint)
curl -s -X POST -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"body": "Comment text"}' \
  "http://som:3000/api/v1/repos/psg24/REPO/issues/42/comments"

# Merge PR
curl -s -X POST -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"Do": "merge", "delete_branch_after_merge": true}' \
  "http://som:3000/api/v1/repos/psg24/REPO/pulls/42/merge"
# Do options: "merge", "rebase", "rebase-merge", "squash", "manually-merged"

# Close PR without merging
curl -s -X PATCH -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"state": "closed"}' \
  "http://som:3000/api/v1/repos/psg24/REPO/pulls/42"
```

### Releases & Tags

```bash
# List releases
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/repos/psg24/REPO/releases" | python3 -c "
import json,sys
for r in json.load(sys.stdin):
    print(f'{r[\"tag_name\"]:20s} {r[\"name\"]}')"

# Create release
curl -s -X POST -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"tag_name": "v1.0.0", "name": "v1.0.0", "body": "Release notes", "draft": false, "prerelease": false}' \
  "http://som:3000/api/v1/repos/psg24/REPO/releases"

# View release by tag
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/repos/psg24/REPO/releases/tags/v1.0.0"

# List tags
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/repos/psg24/REPO/tags" | python3 -c "
import json,sys
for t in json.load(sys.stdin):
    print(t['name'])"
```

### Actions (CI)

```bash
# List workflow runs
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/repos/psg24/REPO/actions/runs" | python3 -c "
import json,sys; d=json.load(sys.stdin)
for r in d.get('workflow_runs', []):
    print(f'#{r[\"id\"]:4d} {r[\"status\"]:10s} {r[\"title\"]}')"

# Trigger workflow dispatch
curl -s -X POST -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"ref": "main", "inputs": {}}' \
  "http://som:3000/api/v1/repos/psg24/REPO/actions/workflows/WORKFLOW.yaml/dispatches"

# List secrets
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/repos/psg24/REPO/actions/secrets"
```

### Labels

```bash
# List labels
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/repos/psg24/REPO/labels" | python3 -c "
import json,sys
for l in json.load(sys.stdin):
    print(f'{l[\"id\"]:4d} {l[\"name\"]:20s} #{l[\"color\"]}')"

# Create label
curl -s -X POST -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "bug", "color": "#ee0701"}' \
  "http://som:3000/api/v1/repos/psg24/REPO/labels"

# Add labels to issue
curl -s -X POST -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"labels": [1, 2]}' \
  "http://som:3000/api/v1/repos/psg24/REPO/issues/42/labels"
```

### Other useful endpoints

```bash
# Collaborators
curl -s -X PUT -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"permission": "write"}' \
  "http://som:3000/api/v1/repos/psg24/REPO/collaborators/USERNAME"

# Webhooks
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/repos/psg24/REPO/hooks"

# Topics
curl -s -X PUT -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"topics": ["go", "cli"]}' \
  "http://som:3000/api/v1/repos/psg24/REPO/topics"

# Notifications
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/notifications"

# Milestones
curl -s -H "Authorization: token $FJ_TOKEN" \
  "http://som:3000/api/v1/repos/psg24/REPO/milestones"
```

## Discovering API endpoints

Swagger UI (browse in browser): `http://som:3000/api/swagger`
