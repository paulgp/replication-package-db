#!/usr/bin/env python3
"""pick-models — Smart model picker for dispatch.

Filters to the 2 latest generations per company, excludes 429'd providers
(with fallback to alternative providers for the same model), and picks
diverse companies per role.

Usage:
    python3 pick-models.py                            # coder, reviewer, scout
    python3 pick-models.py --role coder               # single role
    python3 pick-models.py --role coder --role reviewer
    python3 pick-models.py --json                     # JSON output
    python3 pick-models.py --mark-429 github-copilot  # record rate limit
    python3 pick-models.py --clear-429                # reset all 429s
    python3 pick-models.py --status                   # show 429 state
    python3 pick-models.py \
        --role coder --role reviewer --role scout \
        --prefer-provider anthropic --prefer-provider openai-codex \
        --prefer-model codex                          # preference overrides
    python3 pick-models.py --avoid-provider github-copilot  # avoid provider unless no alternatives
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
FETCH_SCRIPT = SCRIPT_DIR / "fetch-models.py"
TRACKER_FILE = Path("/tmp/pi-model-429s.json")
COOLDOWN_MINUTES = int(os.environ.get("MODEL_429_COOLDOWN_MIN", "60"))
LATEST_N_GENS = 2


# ── 429 Tracking ─────────────────────────────────────────────────────────────


def load_tracker():
    if not TRACKER_FILE.exists():
        return {}
    try:
        return json.loads(TRACKER_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def save_tracker(data):
    TRACKER_FILE.write_text(json.dumps(data, indent=2))


def mark_429(provider):
    tracker = load_tracker()
    tracker[provider] = datetime.now(timezone.utc).isoformat()
    save_tracker(tracker)
    print(f"Marked {provider} as rate-limited at {tracker[provider]}")


def clear_429():
    if TRACKER_FILE.exists():
        TRACKER_FILE.unlink()
    print("Cleared all 429 records")


def get_blocked_providers():
    """Return dict of {provider: {since, remaining_min}} for active blocks."""
    tracker = load_tracker()
    now = time.time()
    blocked = {}
    for provider, ts_str in tracker.items():
        try:
            ts = datetime.fromisoformat(ts_str).timestamp()
            elapsed = (now - ts) / 60
            if elapsed < COOLDOWN_MINUTES:
                blocked[provider] = {
                    "since": ts_str,
                    "remaining_min": int(COOLDOWN_MINUTES - elapsed),
                }
        except (ValueError, TypeError):
            pass
    return blocked


# ── Provider Fallback ────────────────────────────────────────────────────────


def get_all_pi_providers():
    """Get all provider/model combos from pi, grouped by canonical model name.

    Returns: {"gpt.5.4": [{"provider": "github-copilot", "model": "gpt-5.4"},
                           {"provider": "openai-codex", "model": "gpt-5.4"}], ...}
    """
    try:
        result = subprocess.run(
            ["pi", "--list-models"],
            capture_output=True, text=True, timeout=15,
        )
    except FileNotFoundError:
        return {}
    if result.returncode != 0:
        return {}

    by_model = {}
    for line in result.stdout.strip().split("\n"):
        if line.startswith("provider") or not line.strip():
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        provider, model = parts[0], parts[1]
        if re.search(r"-\d{8}$", model) or model.endswith("-latest"):
            continue
        canon = re.sub(r"(\d)-(\d)", r"\1.\2", model)
        by_model.setdefault(canon, []).append({"provider": provider, "model": model})

    return by_model


# ── Data Fetching & Filtering ────────────────────────────────────────────────


def fetch_roster():
    """Get live model roster via fetch-models.py --pi --json."""
    result = subprocess.run(
        [sys.executable, str(FETCH_SCRIPT), "--pi", "--json"],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        print(f"Error from fetch-models: {result.stderr}", file=sys.stderr)
        sys.exit(1)
    return json.loads(result.stdout)


def extract_generation(short_name):
    """Extract version number from display name: 'GPT-5.4' → 5.4"""
    m = re.search(r"(\d+(?:\.\d+)?)", short_name or "")
    return float(m.group(1)) if m else 0.0


def filter_latest_generations(roster, n=LATEST_N_GENS):
    """Keep only models from the N latest generations per company."""
    company_gens = {}
    for m in roster:
        gen = extract_generation(m["short_name"])
        company_gens.setdefault(m["company"], set()).add(gen)

    company_top = {}
    for company, gens in company_gens.items():
        company_top[company] = set(sorted(gens, reverse=True)[:n])

    return [
        m for m in roster
        if extract_generation(m["short_name"]) in company_top.get(m["company"], set())
    ]


def build_generation_rank(roster):
    """Return rank map: (company, generation) -> 0 (latest), 1 (second-latest), ..."""
    company_gens = {}
    for m in roster:
        company = (m.get("company") or "").lower()
        generation = extract_generation(m.get("short_name"))
        company_gens.setdefault(company, set()).add(generation)

    rank = {}
    for company, generations in company_gens.items():
        for i, generation in enumerate(sorted(generations, reverse=True)):
            rank[(company, generation)] = i
    return rank


def recency_bonus(model, generation_rank):
    """Prefer freshest generation while still allowing previous generation fallback."""
    if not generation_rank:
        return 0.0

    company = (model.get("company") or "").lower()
    generation = extract_generation(model.get("short_name"))
    rank = generation_rank.get((company, generation))

    if rank == 0:
        return 80.0
    if rank == 1:
        return 35.0
    return 0.0


def filter_avoided_providers(roster, avoided_providers):
    """Avoid providers when alternatives exist; fall back only when necessary."""
    avoided = set(avoided_providers or [])
    if not avoided:
        return roster

    non_avoided = [m for m in roster if (m.get("provider") or "").lower() not in avoided]
    return non_avoided if non_avoided else roster


def filter_blocked_providers(roster, blocked, all_providers):
    """Exclude 429'd providers, falling back to alternative providers."""
    if not blocked:
        return roster

    result = []
    for m in roster:
        if m["provider"] not in blocked:
            result.append(m)
            continue

        # Try alternative provider for same model
        canon = re.sub(r"(\d)-(\d)", r"\1.\2", m["model"])
        for alt in all_providers.get(canon, []):
            if alt["provider"] not in blocked:
                m2 = dict(m)
                m2["provider"] = alt["provider"]
                m2["model"] = alt["model"]
                result.append(m2)
                break
        # If no alternative, model is dropped

    return result


# ── Role-Based Picking ───────────────────────────────────────────────────────


def parse_k(s):
    m = re.match(r"([\d.]+)K", str(s))
    return float(m.group(1)) * 1000 if m else 0


def score_with_preferences(base_score, model, preferences, generation_rank=None):
    """Apply preference bonuses to a base score."""
    score = float(base_score)

    provider = (model.get("provider") or "").lower()
    company = (model.get("company") or "").lower()
    model_text = f"{model.get('model', '')} {model.get('short_name', '')} {provider}".lower()

    # Recency matters even without explicit preferences.
    score += recency_bonus(model, generation_rank)

    if not preferences:
        return score

    preferred_providers = set(preferences.get("providers", []))
    preferred_companies = set(preferences.get("companies", []))
    preferred_models = preferences.get("models", [])

    if provider in preferred_providers:
        score += 1000.0
    if company in preferred_companies:
        score += 600.0

    for needle in preferred_models:
        n = needle.lower().strip()
        if n and n in model_text:
            score += 250.0

    # Extra nudge: within OpenAI-family, prefer Codex variants when present.
    if company == "openai" and "codex" in model_text:
        score += 150.0

    return score


def pick_for_role(roster, role, exclude_companies=None, preferences=None, generation_rank=None):
    """Pick best model for a role, preferring companies not yet used."""
    candidates = roster
    if exclude_companies:
        narrowed = [m for m in candidates if m["company"] not in exclude_companies]
        if narrowed:
            candidates = narrowed

    if role == "coder":
        def score(m):
            coding = m.get("coding_score") or 0
            big_output = 1 if parse_k(m.get("max_output", "")) >= 128_000 else 0
            thinking = 0.5 if m.get("thinking") else 0
            return score_with_preferences(coding + big_output + thinking, m, preferences, generation_rank)
        return max(candidates, key=score)

    elif role == "reviewer":
        def score(m):
            intel = m.get("intelligence_score") or 0
            agentic = m.get("agentic_score") or 0
            thinking = 1 if m.get("thinking") else 0
            return score_with_preferences(intel + agentic * 0.5 + thinking, m, preferences, generation_rank)
        return max(candidates, key=score)

    elif role == "scout":
        # Scout is cost-first. Preferences are a tie-break/filter, not a cost override.
        candidates_sorted = sorted(candidates, key=lambda m: m.get("price_per_m_input", 999))

        if not preferences:
            return candidates_sorted[0]

        preferred_providers = set(preferences.get("providers", []))
        preferred_companies = set(preferences.get("companies", []))
        preferred_models = [m.lower().strip() for m in preferences.get("models", []) if m]

        def is_preferred(m):
            provider = (m.get("provider") or "").lower()
            company = (m.get("company") or "").lower()
            model_text = f"{m.get('model', '')} {m.get('short_name', '')} {provider}".lower()
            if provider in preferred_providers:
                return True
            if company in preferred_companies:
                return True
            return any(needle and needle in model_text for needle in preferred_models)

        preferred_candidates = [m for m in candidates_sorted if is_preferred(m)]
        if preferred_candidates:
            return preferred_candidates[0]

        return candidates_sorted[0]

    elif role == "planner":
        def score(m):
            intel = m.get("intelligence_score") or 0
            agentic = m.get("agentic_score") or 0
            return score_with_preferences(intel + agentic, m, preferences, generation_rank)
        return max(candidates, key=score)

    else:
        return max(candidates, key=lambda m: score_with_preferences(m.get("coding_score") or 0, m, preferences, generation_rank))


def normalize_pref_list(values):
    """Normalize repeated or comma-separated preference values."""
    out = []
    for value in values:
        for token in str(value).split(","):
            token = token.strip().lower()
            if token:
                out.append(token)
    return out


# ── Output ───────────────────────────────────────────────────────────────────


def model_id(m):
    return f"{m['provider']}/{m['model']}"


def fmt(val):
    if val is None:
        return "—"
    if isinstance(val, float):
        return f"{val:.1f}" if val != int(val) else str(int(val))
    return str(val)


def render_markdown(picks, blocked, total, filtered_count, preferences):
    lines = []

    if blocked:
        parts = [f"`{p}` ({info['remaining_min']}m left)" for p, info in blocked.items()]
        lines.append(f"**429'd providers (excluded):** {', '.join(parts)}")
        lines.append("")

    if preferences and any(preferences.get(k) for k in ("providers", "companies", "models", "avoid_providers")):
        lines.append("**Preference hints:**")
        if preferences.get("providers"):
            lines.append(f"- providers: {', '.join(preferences['providers'])}")
        if preferences.get("companies"):
            lines.append(f"- companies: {', '.join(preferences['companies'])}")
        if preferences.get("models"):
            lines.append(f"- model hints: {', '.join(preferences['models'])}")
        if preferences.get("avoid_providers"):
            lines.append(f"- avoid providers: {', '.join(preferences['avoid_providers'])}")
        lines.append("")

    lines.append(
        f"{total} models → {filtered_count} after filtering "
        f"(latest {LATEST_N_GENS} generations, 429-aware)"
    )
    lines.append("")
    lines.append("| Role | Model ID | Company | Coding | Intel | Agentic | $/M in | tok/s |")
    lines.append("|------|----------|---------|-------:|------:|--------:|-------:|------:|")

    for role, m in picks.items():
        lines.append(
            f"| {role} "
            f"| `{model_id(m)}` "
            f"| {m['company']} "
            f"| {fmt(m.get('coding_score'))} "
            f"| {fmt(m.get('intelligence_score'))} "
            f"| {fmt(m.get('agentic_score'))} "
            f"| ${m.get('price_per_m_input', 0):.2f} "
            f"| {fmt(m.get('tok_per_sec'))} |"
        )

    return "\n".join(lines)


def render_json(picks):
    return json.dumps(
        {role: {"model_id": model_id(m), **m} for role, m in picks.items()},
        indent=2,
    )


# ── Main ─────────────────────────────────────────────────────────────────────


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Pick dispatch models from live roster")
    parser.add_argument("--role", action="append", choices=["planner", "coder", "reviewer", "scout", "any"], help="Role to pick for")
    parser.add_argument("--json", action="store_true", help="Emit JSON output")
    parser.add_argument("--mark-429", dest="mark_provider", help="Mark provider as 429-blocked")
    parser.add_argument("--clear-429", action="store_true", help="Clear all 429 blocks")
    parser.add_argument("--status", action="store_true", help="Show blocked providers")

    parser.add_argument(
        "--prefer-provider",
        action="append",
        default=[],
        help="Preferred provider (can repeat or use commas)",
    )
    parser.add_argument(
        "--prefer-company",
        action="append",
        default=[],
        help="Preferred company (can repeat or use commas)",
    )
    parser.add_argument(
        "--prefer-model",
        action="append",
        default=[],
        help="Preferred model substring/hint (can repeat or use commas)",
    )
    parser.add_argument(
        "--avoid-provider",
        action="append",
        default=[],
        help="Avoid provider unless no alternative remains (can repeat or use commas)",
    )

    return parser.parse_args(argv)


def main():
    args = parse_args(sys.argv[1:])

    if args.mark_provider:
        mark_429(args.mark_provider)
        return

    if args.clear_429:
        clear_429()
        return

    if args.status:
        blocked = get_blocked_providers()
        if not blocked:
            print("No providers are currently rate-limited.")
        else:
            for p, info in blocked.items():
                print(f"  {p}: since {info['since']} ({info['remaining_min']}m remaining)")
        return

    # Parse --role flags
    roles = [r for r in (args.role or []) if r and r != "any"]
    if not roles:
        roles = ["coder", "reviewer", "scout"]

    preferences = {
        "providers": normalize_pref_list(args.prefer_provider),
        "companies": normalize_pref_list(args.prefer_company),
        "models": normalize_pref_list(args.prefer_model),
        "avoid_providers": normalize_pref_list(args.avoid_provider),
    }

    # Fetch data
    roster = fetch_roster()
    total = len(roster)
    all_providers = get_all_pi_providers()
    blocked = get_blocked_providers()

    # Filter
    filtered = filter_latest_generations(roster)
    filtered = filter_blocked_providers(filtered, blocked, all_providers)
    filtered = filter_avoided_providers(filtered, preferences.get("avoid_providers"))

    if not filtered:
        print("No models after filtering — falling back to generation filter only.", file=sys.stderr)
        filtered = filter_latest_generations(roster)

    generation_rank = build_generation_rank(filtered)

    # Pick with provider diversity
    picks = {}
    used_companies = set()
    for role in roles:
        pick = pick_for_role(
            filtered,
            role,
            exclude_companies=used_companies,
            preferences=preferences,
            generation_rank=generation_rank,
        )
        picks[role] = pick
        used_companies.add(pick["company"])

    if args.json:
        print(render_json(picks))
    else:
        print(render_markdown(picks, blocked, total, len(filtered), preferences))


if __name__ == "__main__":
    main()
