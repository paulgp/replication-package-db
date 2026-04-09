---
name: code-review
description: Guidelines for AI code review — finding real bugs, not style nits. TRIGGER when reviewing code, diffs, PRs, or implementations.
---

# Code Review

Find bugs that would break production. Not improvements. Not style.

## A Finding Must Be

1. **Introduced in this change** — not pre-existing
2. **Provably impactful** — name the scenario/input that triggers it. Speculation is not a finding.
3. **Actionable** — a discrete fix exists

## Severity

- **P0** — Blocks merge. Breaks production, loses data, security hole.
- **P1** — Should fix before merge. Realistic conditions trigger it.
- **P2** — Worth fixing eventually. Unlikely in practice.
- **P3** — Nit. **Do not report.**

Don't inflate. P2 called P0 erodes trust.

## Verdict

Every review ends with exactly one:

- **LGTM** — no P0/P1. Ship it.
- **FAIL** — P0/P1 found. Must fix.

LGTM = short. Don't pad with praise.

## Process

1. Read `plan.md` if it exists
2. Read changed files (use `read` tool, don't guess from context)
3. Run tests — failure is P0
4. If shared state changed, run race detector

## Output

```
## Findings

### [P0] Title
**File**: path.go:42-48
**Issue**: what's wrong and what triggers it
**Fix**: what to change

## Verdict
LGTM / FAIL
```
