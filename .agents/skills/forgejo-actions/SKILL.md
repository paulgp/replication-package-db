---
name: forgejo-actions
description: Write and debug Forgejo Actions workflows. TRIGGER when creating/fixing CI jobs, retrieving job logs, diagnosing build failures, or configuring runners.
allowed-tools: Bash(fj *), Bash(curl *code.tailcddce6.ts.net*), Bash(ssh golf *), Bash(scp *golf:*), Bash(zstd *), Bash(url-screenshot *), Bash(*| jq*)
---

## Instance

- Forgejo: `code.tailcddce6.ts.net` (Tailscale tailnet, Forgejo v13)
- Runner host: `golf` (Alpine Linux, reachable via SSH)
- Runner: native `forgejo-runner` binary managed by supervisord
- Container backend: Podman (rootless, via API socket)
- Auth token: `$FJ_TOKEN` env var
- SSH: `ssh golf` is pre-configured in the user's SSH config (used for log retrieval)
- **Do NOT use `gh`** — use `fj` for all Forgejo operations

## Task ID vs Run ID

These are different numbers:
- **Task ID** (`#N` from `fj actions tasks`): identifies a single job execution. Used for log file paths on disk.
- **Run ID** (from the API `/actions/runs`): identifies a workflow run (may contain multiple jobs). Used in the web UI URL.

For a single-job workflow they often match, but not always. When debugging, start with `fj actions tasks` for the task ID, and use the API to find the corresponding run ID if you need the web URL.

## Getting job logs

**Forgejo v13 does NOT expose job logs via REST API.** There are two methods:

### Method 1: Screenshot the run page (quick visual)

Shows step names, durations, and pass/fail icons. Good for identifying which step failed.

```bash
url-screenshot "https://code.tailcddce6.ts.net/{owner}/{repo}/actions/runs/{RUN_ID}" ./tmp/run.png
```

The run ID is NOT the task ID. To find it, use the API:
```bash
curl -sk -H "Authorization: token $FJ_TOKEN" \
  "https://code.tailcddce6.ts.net/api/v1/repos/{owner}/{repo}/actions/runs" | \
  jq '[.workflow_runs[] | {id, title, status, workflow_id}]'
```

### Method 2: Read logs from the server (full text)

Logs are zstd-compressed on golf at:
```
/home/klj39/data/forgejo/gitea/actions_log/{owner}/{repo}/{TASK_ID}/{TASK_ID}.log.zst
```

The `TASK_ID` is the `#N` number from `fj actions tasks` output.

```bash
# Copy and decompress
scp golf:/home/klj39/data/forgejo/gitea/actions_log/{owner}/{repo}/{TASK_ID}/{TASK_ID}.log.zst /tmp/ && \
  zstd -d -c /tmp/{TASK_ID}.log.zst

# Just the tail (failure reason)
zstd -d -c /tmp/{TASK_ID}.log.zst | tail -40

# Search for errors
zstd -d -c /tmp/{TASK_ID}.log.zst | grep -i 'error\|fail\|not found'
```

Log format: each line is `TIMESTAMP MESSAGE`. Key markers:
- `exitcode '0'` = step succeeded
- `exitcode '1'` or `exitcode '101'` = step failed
- `Job failed` / `Job succeeded` = final status
- `command not found` = missing tool in container

### List all logs for a repo

```bash
ssh golf "find /home/klj39/data/forgejo/gitea/actions_log/{owner}/{repo} -name '*.log.zst' | sort"
```

### API endpoints that DO NOT EXIST (don't waste time trying)

- `/api/v1/repos/{owner}/{repo}/actions/tasks/{id}` — 404
- `/api/v1/repos/{owner}/{repo}/actions/tasks/{id}/logs` — 404
- `/api/v1/repos/{owner}/{repo}/actions/runs/{id}/jobs` — 404
- `/api/v1/repos/{owner}/{repo}/actions/jobs` — 404
- The web log route (`/{owner}/{repo}/actions/runs/{id}/jobs/{id}/logs`) requires CSRF tokens and does not accept API token auth.

## Runner details

- Runner name: `golf-runner`
- Capacity: 2 concurrent jobs
- Labels and their container images:
  - `docker` → `node:20-bookworm`
  - `ubuntu-latest` → `node:20-bookworm`
  - `alpine` → `alpine:3.21`
- Config: `/home/klj39/apps/forgejo-runner/config.yaml`
- Registration: `/home/klj39/apps/forgejo-runner/.runner`
- Managed by: supervisord (`forgejo-runner` and `podman-socket` programs)
- Ansible role: `forgejo-runner` in home-provisioning repo

### Checking runner health

```bash
# Runner process status
ssh golf "supervisorctl status forgejo-runner podman-socket"

# Runner daemon logs
ssh golf "tail -30 /var/log/supervisord/forgejo-runner.stderr.log"

# Common runner errors:
# "cannot ping the docker daemon" → podman-socket not running
# "context canceled" → normal cleanup after job completes
```

### Admin UI

Runners overview: `https://code.tailcddce6.ts.net/-/admin/runners`

## Writing workflows for this runner

The runner containers are **minimal** — not GitHub hosted runners. They do NOT have preinstalled tools beyond what the base image provides.

### `node:20-bookworm` has:
node, npm, git, curl, wget, ca-certificates, common Debian packages

### `node:20-bookworm` does NOT have:
rustup, go, python, docker, jq (on some versions), make, gcc

### Patterns for installing tools

**Rust:**
```yaml
- name: Install Rust toolchain
  run: |
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y \
      --default-toolchain 1.93.0 --profile minimal --target wasm32-wasip1 \
      -c rustfmt -c clippy
    echo "$HOME/.cargo/bin" >> "$GITHUB_PATH"
```

**Go:**
```yaml
- name: Install Go
  run: |
    curl -sL https://go.dev/dl/go1.23.6.linux-amd64.tar.gz | tar -C /usr/local -xzf -
    echo "/usr/local/go/bin" >> "$GITHUB_PATH"
```

**Python:**
```yaml
- name: Install Python
  run: |
    apt-get update && apt-get install -y python3 python3-pip
    echo "$HOME/.local/bin" >> "$GITHUB_PATH"
```

### Key workflow tips

- **`$GITHUB_PATH`**: append paths to persist across steps (each step is a fresh shell).
- **`$GITHUB_ENV`**: append `KEY=VALUE` lines to persist env vars across steps.
- **WASM targets**: if `.cargo/config.toml` sets `target = "wasm32-wasip1"`, unit tests will fail with `Exec format error` unless you override: `cargo test --target x86_64-unknown-linux-gnu`.
- **No Docker-in-Docker**: job containers cannot run `docker build` or `docker run`. Workflows needing container builds require additional runner configuration (Podman-in-Podman with fuse + SYS_ADMIN).
- **Actions resolution**: `uses: actions/checkout@v4` resolves from GitHub (`DEFAULT_ACTIONS_URL=https://github.com`).
- **Per-repo opt-in**: Actions must be enabled per-repository in Repository Settings → Units → Actions.

## Debugging checklist

When a CI job fails:

1. Identify the failing task:
   ```bash
   cd /path/to/repo && fj actions tasks
   # Output: #TASK_ID (COMMIT) STATUS JOB_NAME DURATION (EVENT): COMMIT_MESSAGE
   ```
   Note the `#TASK_ID` (for logs) and the run's commit/job name.
2. Screenshot the run page to see which step has the red X:
   ```bash
   url-screenshot "https://code.tailcddce6.ts.net/{owner}/{repo}/actions/runs/{RUN_ID}" ./tmp/run.png
   ```
3. Pull the log from golf to read the actual error:
   ```bash
   scp golf:/home/klj39/data/forgejo/gitea/actions_log/{owner}/{repo}/{TASK_ID}/{TASK_ID}.log.zst /tmp/ && \
     zstd -d -c /tmp/{TASK_ID}.log.zst | tail -40
   ```
4. Common failures:
   - **`command not found`** → tool not installed in container, add an install step
   - **`Exec format error (os error 8)`** → trying to run a WASM/cross-compiled binary natively, override the target
   - **`cannot ping the docker daemon`** → podman-socket supervisord program not running
   - **`docker: command not found`** → workflow needs Docker-in-Docker, not available
   - **DNS resolution failures** → set `network: "host"` in runner config.yaml container section
