---
description: Full workflow — scout, clarify, plan-to-consensus, code, review-to-consensus, commit
---
Ship #$@.

If `$@` is a tracker issue number, run the issue workflow.
If `$@` starts with `file:` or `path:`, treat it as a local task file and
ship from local context (skip issue tracker lookup/close).

If multiple issues are specified (commas, ranges), handle each issue separately in sequence.
Each issue gets its own workDir: `/tmp/forge/issue-<N>/` where `<N>` is the individual issue number.

For each issue:

1. Run `git status --porcelain` — if dirty, stop and tell me.
2. `rm -rf /tmp/forge/issue-<N>/ && mkdir -p /tmp/forge/issue-<N>/`
3. Follow the forge /ship workflow strictly:
   - mandatory scout
   - clarify with user if ambiguous
   - mandatory plan debate to LGTM consensus
   - code
   - mandatory review debate to LGTM consensus
   - if review is not LGTM consensus, return to code and iterate
4. Do not ship until `plan-review/consensus.md` and `code-review/consensus.md` exist in the issue workDir.

If an issue fails (blocked, tests won't pass, unresolved ambiguity, or consensus gates not met): hard fail and stop the /ship run.
Between issues (only after a successful issue): compact with context about remaining work.