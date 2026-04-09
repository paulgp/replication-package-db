#!/usr/bin/env python3
"""Fetch available LLM models with OpenRouter pricing, throughput, and benchmarks.

Outputs a markdown table suitable for an LLM tech lead to pick models from.
Uses only Python stdlib.

Usage:
    python3 fetch-models.py                    # all models from target companies
    python3 fetch-models.py --pi               # only models available in pi
    python3 fetch-models.py --json             # raw JSON output
    python3 fetch-models.py --pi --json        # pi models as JSON
"""

import json
import re
import subprocess
import sys
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

OPENROUTER_PUBLIC = "https://openrouter.ai/api/v1/models"
OPENROUTER_FRONTEND = "https://openrouter.ai/api/frontend/models"
OPENROUTER_THROUGHPUT = "https://openrouter.ai/api/frontend/stats/throughput-comparison"
OPENROUTER_BENCHMARKS = "https://openrouter.ai/api/internal/v1/artificial-analysis-benchmarks"

COMPANIES = {"anthropic", "openai", "google", "xai"}

# Higher number = more preferred when two providers map to the same canonical model.
PROVIDER_PRIORITY = {
    "openai-codex": 100,
    "openai": 95,
    "anthropic": 90,
    "google": 80,
    "xai": 70,
    "z.ai": 60,
    "github-copilot": 10,
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fetch_json(url, timeout=10):
    req = urllib.request.Request(url, headers={"User-Agent": "forge-models/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def parse_k(s):
    m = re.match(r"([\d.]+)K", s)
    return float(m.group(1)) * 1000 if m else 0


def first_sentence(text):
    if not text:
        return ""
    # Split on ". " but not on URLs or abbreviations
    m = re.match(r"(.+?\.)\s", text.replace("\n", " "))
    return m.group(1) if m else text[:120]


def provider_priority(provider):
    return PROVIDER_PRIORITY.get((provider or "").lower(), 0)


def should_replace_existing(existing, candidate):
    """Decide which provider row wins for the same canonical model."""
    if candidate["in_price"] < existing["in_price"]:
        return True
    if candidate["in_price"] > existing["in_price"]:
        return False

    # Price tie: prefer direct providers over wrappers (e.g., openai-codex > github-copilot).
    cand_priority = provider_priority(candidate.get("pi_provider"))
    existing_priority = provider_priority(existing.get("pi_provider"))
    if cand_priority != existing_priority:
        return cand_priority > existing_priority

    return False


# ---------------------------------------------------------------------------
# 1. Parse pi --list-models (optional)
# ---------------------------------------------------------------------------

def get_pi_models():
    """Returns list of pi models, or None if pi is not available."""
    try:
        result = subprocess.run(["pi", "--list-models"], capture_output=True, text=True, timeout=15)
    except FileNotFoundError:
        return None
    if result.returncode != 0:
        return None
    models = []
    for line in result.stdout.strip().split("\n"):
        if line.startswith("provider") or not line.strip():
            continue
        parts = line.split()
        if len(parts) < 6:
            continue
        # Skip date-stamped and -latest duplicates
        if re.search(r"-\d{8}$", parts[1]) or parts[1].endswith("-latest"):
            continue
        models.append({
            "pi_provider": parts[0],
            "pi_model": parts[1],
            "context": parts[2],
            "max_output": parts[3],
            "thinking": parts[4] == "yes",
        })
    return models


# ---------------------------------------------------------------------------
# 2. Fetch OpenRouter data
# ---------------------------------------------------------------------------

def get_openrouter_data():
    public = fetch_json(OPENROUTER_PUBLIC)["data"]
    frontend = fetch_json(OPENROUTER_FRONTEND)["data"]

    pricing = {}
    for m in public:
        pricing[m["id"]] = m
        parts = m["id"].split("/", 1)
        if len(parts) == 2:
            pricing[parts[1]] = m

    meta = {}
    for m in frontend:
        meta[m["slug"]] = {
            "permaslug": m.get("permaslug"),
            "description": m.get("description", ""),
            "group": m.get("group", ""),
            "author": m.get("author", ""),
            "short_name": m.get("short_name", ""),
        }

    return pricing, meta


def match_or_slug(pi_provider, pi_model, pricing):
    """Find the canonical OpenRouter slug (provider/model) for a pi model."""
    provider_map = {"anthropic": "anthropic", "openai-codex": "openai", "github-copilot": None}
    or_provider = provider_map.get(pi_provider, pi_provider)
    normalized = re.sub(r"-\d{8}$", "", pi_model)
    normalized = re.sub(r"-latest$", "", normalized)
    dotted = re.sub(r"(\d)-(\d)", r"\1.\2", normalized)

    candidates = []
    if or_provider:
        candidates += [f"{or_provider}/{normalized}", f"{or_provider}/{dotted}"]
    candidates += [normalized, dotted]
    if pi_provider == "github-copilot":
        for p in ["anthropic", "openai", "google", "xai"]:
            candidates += [f"{p}/{normalized}", f"{p}/{dotted}"]

    for c in candidates:
        if c in pricing:
            # Always return the canonical id (provider/model), not the bare key
            return pricing[c].get("id", c)
    return None


def infer_company(pi_provider, pi_model):
    """Infer the real company from the model name."""
    if pi_provider in COMPANIES:
        return pi_provider
    name = pi_model.lower()
    if "claude" in name or "haiku" in name or "sonnet" in name or "opus" in name:
        return "anthropic"
    if "gemini" in name or "gemma" in name:
        return "google"
    if "grok" in name:
        return "xai"
    return "openai"


# ---------------------------------------------------------------------------
# 3. Fetch throughput + benchmarks in parallel
# ---------------------------------------------------------------------------

def fetch_throughput(permaslug):
    url = f"{OPENROUTER_THROUGHPUT}?permaslug={urllib.parse.quote(permaslug)}"
    try:
        data = fetch_json(url)
        vals = [v for pt in data.get("data", []) for v in pt.get("y", {}).values() if v and v > 0]
        return int(sum(vals) / len(vals)) if vals else None
    except Exception:
        return None


def fetch_benchmarks(slug):
    url = f"{OPENROUTER_BENCHMARKS}?slug={urllib.parse.quote(slug)}"
    try:
        data = fetch_json(url)
        entries = data.get("data", [])
        if not entries:
            return None
        # Take the first entry (usually the adaptive/best config)
        e = entries[0]
        evals = e.get("benchmark_data", {}).get("evaluations", {})
        return {
            "coding": evals.get("artificial_analysis_coding_index"),
            "intelligence": evals.get("artificial_analysis_intelligence_index"),
            "agentic": evals.get("artificial_analysis_agentic_index"),
        }
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def build_roster_from_openrouter(pricing, meta):
    """Build roster from all OpenRouter models for target companies."""
    seen = {}
    for slug, or_data in pricing.items():
        if "/" not in slug:
            continue  # skip bare-name duplicates
        author = slug.split("/")[0]
        if author not in COMPANIES:
            continue

        pr = or_data.get("pricing", {})
        in_price = float(pr.get("prompt", 0)) * 1_000_000
        out_price = float(pr.get("completion", 0)) * 1_000_000

        m = meta.get(slug, {})
        top = or_data.get("top_provider", {})
        ctx = or_data.get("context_length", 0)
        max_out = top.get("max_completion_tokens") if top else None

        # Format context/output like pi does
        def fmt_k(n):
            if not n: return "—"
            if n >= 1000: return f"{n/1000:.0f}K"
            return str(n)

        # Skip image-only, audio, embedding, old deprecated models
        modality = or_data.get("architecture", {}).get("modality", "")
        if "text" not in modality:
            continue
        # Skip variants we don't want
        if any(x in slug for x in [":thinking", ":free", ":extended", "-online",
                                     "audio", "image-gen", "embedding", "safeguard",
                                     "oss-", "gemma-", "chat"]):
            continue

        short_name = m.get("short_name") or slug.split("/")[-1]
        permaslug = m.get("permaslug")

        # Dedup by short_name
        if short_name in seen and in_price >= seen[short_name]["in_price"]:
            continue

        seen[short_name] = {
            "pi_provider": author,
            "pi_model": slug.split("/")[-1],
            "or_slug": slug,
            "permaslug": permaslug,
            "company": author,
            "family": m.get("group", ""),
            "short_name": short_name,
            "description": first_sentence(m.get("description") or or_data.get("description", "")),
            "context": fmt_k(ctx),
            "max_output": fmt_k(max_out),
            "thinking": or_data.get("supported_parameters") and "reasoning" in or_data.get("supported_parameters", []),
            "in_price": in_price,
            "out_price": out_price,
            "tok_s": None,
            "benchmarks": None,
        }

    return seen


def build_roster_from_pi(pi_models, pricing, meta):
    """Build roster from pi models, enriched with OpenRouter data."""
    seen = {}
    for pm in pi_models:
        or_slug = match_or_slug(pm["pi_provider"], pm["pi_model"], pricing)
        if not or_slug:
            continue

        or_data = pricing.get(or_slug, {})
        pr = or_data.get("pricing", {})
        in_price = float(pr.get("prompt", 0)) * 1_000_000
        out_price = float(pr.get("completion", 0)) * 1_000_000

        m = meta.get(or_slug, {})
        company = infer_company(pm["pi_provider"], pm["pi_model"])

        # For throughput, map copilot models to their real provider's permaslug
        real_slug = None
        if pm["pi_provider"] == "github-copilot":
            dotted = re.sub(r"(\d)-(\d)", r"\1.\2", pm["pi_model"])
            for p in COMPANIES:
                candidate = f"{p}/{dotted}"
                if candidate in meta:
                    real_slug = candidate
                    break
        permaslug = meta.get(real_slug or or_slug, {}).get("permaslug") or m.get("permaslug")

        canon = re.sub(r"(\d)-(\d)", r"\1.\2", pm["pi_model"])
        candidate = {
            "pi_provider": pm["pi_provider"],
            "pi_model": pm["pi_model"],
            "or_slug": or_slug,
            "permaslug": permaslug,
            "company": company,
            "family": m.get("group", ""),
            "short_name": m.get("short_name") or pm["pi_model"],
            "description": first_sentence(m.get("description") or or_data.get("description", "")),
            "context": pm["context"],
            "max_output": pm["max_output"],
            "thinking": pm["thinking"],
            "in_price": in_price,
            "out_price": out_price,
            "tok_s": None,
            "benchmarks": None,
        }

        if canon in seen and not should_replace_existing(seen[canon], candidate):
            continue

        seen[canon] = candidate

    return seen


def enrich_with_stats(seen):
    """Fetch throughput and benchmarks in parallel."""
    slugs_throughput = {v["permaslug"] for v in seen.values() if v["permaslug"]}
    slugs_bench = {v["or_slug"] for v in seen.values() if v["or_slug"]}

    results = {}
    with ThreadPoolExecutor(max_workers=12) as pool:
        for slug in slugs_throughput:
            results[("tp", slug)] = pool.submit(fetch_throughput, slug)
        for slug in slugs_bench:
            results[("bm", slug)] = pool.submit(fetch_benchmarks, slug)

    tp_cache = {slug: results[("tp", slug)].result() for slug in slugs_throughput if ("tp", slug) in results}
    bm_cache = {slug: results[("bm", slug)].result() for slug in slugs_bench if ("bm", slug) in results}

    for v in seen.values():
        v["tok_s"] = tp_cache.get(v["permaslug"])
        v["benchmarks"] = bm_cache.get(v["or_slug"])


def build_roster(use_pi=False):
    pricing, meta = get_openrouter_data()

    if use_pi:
        pi_models = get_pi_models()
        if pi_models is None:
            print("Warning: pi not found, showing all OpenRouter models", file=sys.stderr)
            seen = build_roster_from_openrouter(pricing, meta)
        else:
            seen = build_roster_from_pi(pi_models, pricing, meta)
    else:
        seen = build_roster_from_openrouter(pricing, meta)

    enrich_with_stats(seen)
    return list(seen.values())


def fmt(val, fallback="—"):
    if val is None:
        return fallback
    if isinstance(val, float):
        return f"{val:.1f}"
    return str(val)


def render_markdown(roster):
    # Sort by coding score descending, then by price
    def sort_key(m):
        coding = (m.get("benchmarks") or {}).get("coding")
        return (-(coding or 0), m["in_price"])

    roster.sort(key=sort_key)

    companies = sorted({m["company"] for m in roster})
    lines = [
        f"{len(roster)} models from {len(companies)} companies ({', '.join(companies)}). "
        f"Sorted by coding benchmark.",
        "",
        "| Model | Company | Coding | Intel | Agentic | $/M in | tok/s | Max Output | Best for |",
        "|-------|---------|-------:|------:|--------:|-------:|------:|-----------:|----------|",
    ]

    for m in roster:
        bm = m.get("benchmarks") or {}
        think = " 💭" if m["thinking"] else ""
        lines.append(
            f"| {m['short_name']}{think} "
            f"| {m['company']} "
            f"| {fmt(bm.get('coding'))} "
            f"| {fmt(bm.get('intelligence'))} "
            f"| {fmt(bm.get('agentic'))} "
            f"| ${m['in_price']:.2f} "
            f"| {fmt(m['tok_s'])} "
            f"| {m['max_output']} "
            f"| {m['description']} |"
        )

    return "\n".join(lines)


def render_json(roster):
    # Strip internal fields
    clean = []
    for m in roster:
        bm = m.get("benchmarks") or {}
        clean.append({
            "model": m["pi_model"],
            "provider": m["pi_provider"],
            "company": m["company"],
            "family": m["family"],
            "short_name": m["short_name"],
            "description": m["description"],
            "context": m["context"],
            "max_output": m["max_output"],
            "thinking": m["thinking"],
            "price_per_m_input": m["in_price"],
            "price_per_m_output": m["out_price"],
            "tok_per_sec": m["tok_s"],
            "coding_score": bm.get("coding"),
            "intelligence_score": bm.get("intelligence"),
            "agentic_score": bm.get("agentic"),
        })
    return json.dumps(clean, indent=2)


def print_help():
    print("""fetch-models.py — Live LLM model roster with benchmarks, pricing, and throughput.

Usage:
    python3 fetch-models.py              All models from anthropic/openai/google/xai
    python3 fetch-models.py --pi         Only models configured in pi
    python3 fetch-models.py --json       JSON output instead of markdown
    python3 fetch-models.py --pi --json  Pi models as JSON

Data sources (fetched live, ~3s):
    OpenRouter /api/v1/models            Pricing, descriptions, context/output limits
    OpenRouter /api/frontend/models      Company, family, permaslugs
    OpenRouter /api/frontend/stats       Throughput in tok/s
    OpenRouter /api/internal/v1          Artificial Analysis coding/intel/agentic benchmarks
    pi --list-models                     Available models (with --pi only)""")


if __name__ == "__main__":
    if "--help" in sys.argv or "-h" in sys.argv:
        print_help()
        sys.exit(0)
    use_pi = "--pi" in sys.argv
    roster = build_roster(use_pi=use_pi)
    if "--json" in sys.argv:
        print(render_json(roster))
    else:
        print(render_markdown(roster))
