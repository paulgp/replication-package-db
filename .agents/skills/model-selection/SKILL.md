---
name: model-selection
description: "Discover available LLM models with live benchmarks, pricing, and throughput, while enforcing recency and 429-sensitivity for dispatch."
allowed-tools: Bash(python3 *fetch-models*), Bash(python3 *pick-models*), Bash(cat *)
---

# Model Selection

## Primary workflow (dispatch-safe)

Use the picker script for dispatch staffing:

```bash
python3 SKILL_DIR/pick-models.py --role coder --role reviewer --role scout
python3 SKILL_DIR/pick-models.py --role coder --role reviewer --role scout --json

# Preference-aware:
python3 SKILL_DIR/pick-models.py \
  --role coder --role reviewer --role scout \
  --prefer-provider openai --prefer-provider openai-codex --prefer-provider anthropic \
  --prefer-model codex --avoid-provider github-copilot
```

`pick-models.py` does all of the following for you:

Default policy (when called through `models()` from forge): prioritize:
- **OpenAI + Anthropic** providers
- latest generation first (while keeping latest two generations eligible)
- **Codex** variants for OpenAI where available
- avoid/deflect `github-copilot` unless no strong alternative is available

1. Pulls a live roster from `fetch-models.py --pi --json`.
2. Keeps only the **2 latest generations** per company.
3. Ignores providers currently marked as 429’d.
4. Falls back to alternative providers for the same model when possible.
5. Prefers company diversity across roles (coder/reviewer/scout/planner).
6. Accepts optional preference hints (`--prefer-provider`, `--prefer-company`, `--prefer-model`, `--avoid-provider`) to bias toward user-chosen vendors/variants.

If a 429 happens during dispatch, immediately run:

```bash
python3 SKILL_DIR/pick-models.py --mark-429 <provider>
```

Then re-run the role picker before your next dispatch.

Use `--status` or `--clear-429` to inspect/clear temporary provider blocks.

Default cooldown for 429 blocks is 60 minutes; override with `MODEL_429_COOLDOWN_MIN` environment variable.

Preference helpers:
- `--prefer-provider`: e.g., `openai`, `openai-codex`, `anthropic`
- `--prefer-company`: e.g., `openai`, `anthropic`
- `--prefer-model`: substring hints, e.g., `codex`, `5.4`
- `--avoid-provider`: e.g., `github-copilot`

```bash
# Example (typical default policy)
python3 SKILL_DIR/pick-models.py --role coder --role reviewer --role scout --prefer-provider openai --prefer-provider openai-codex --prefer-provider anthropic --prefer-model codex --avoid-provider github-copilot
```

## Live roster (full)

```bash
python3 SKILL_DIR/fetch-models.py              # all models from anthropic/openai/google/xai
python3 SKILL_DIR/fetch-models.py --pi         # only models configured in pi
python3 SKILL_DIR/fetch-models.py --json       # JSON output
python3 SKILL_DIR/fetch-models.py --pi --json  # pi models as JSON
```

`fetch-models.py` uses OpenRouter data (pricing, throughput, Artificial Analysis
benchmarks). Use `--pi` when you need to know what dispatch can actually use.

## Why this exists

- **Dispatch defaults can lag**: built-in model lists get stale across generations.
- **Recency control**: we pin dispatch to current model generations automatically.
- **Rate-limit aware**: if provider 429s, staffing can shift immediately.

## Key columns

- **Coding / Intel / Agentic** — Artificial Analysis benchmark scores.
  Higher is better, and these are the primary quality signals.
- **$/M in** — cost per million input tokens.
- **tok/s** — output throughput.
- **Max Output** — largest single response.
- **Company** — for diversity: pick coder and reviewer from different companies.
