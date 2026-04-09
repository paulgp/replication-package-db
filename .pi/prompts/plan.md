---
description: Fetch issue, scout, consult if needed, write plan — no implementation
---
Fetch issue #$@ from the repo tracker (use `fj issue view -R origin $@`).
Scout the relevant code. Consult specialists if the issue is complex.
If multiple issues are specified (commas, ranges), use a separate workDir per issue: `/tmp/forge/issue-<N>/`.
For a single issue, write the plan to `/tmp/forge/issue-$@/plan.md`. Stop before coding.
