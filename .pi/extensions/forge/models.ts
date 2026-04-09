/**
 * models tool — Discover available models for dispatch staffing.
 *
 * Primary: runs the model-selection skill's pick-models.py to return
 * role-specific picks that are:
 *   - limited to the 2 latest generations per company
 *   - aware of provider 429 blocks (with fallback providers where possible)
 *   - diversity-aware across roles/workflows
 *
 * Fallback: if the skill script is unavailable, falls back to `pi --list-models`
 * with heuristic scoring.
 */

import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import { Type } from "@sinclair/typebox";
import { Text } from "@mariozechner/pi-tui";
import { accessSync, constants } from "node:fs";
import { logOperation } from "./log.js";

/** Inline replacement for pi-ai's StringEnum to avoid transitive dep issues. */
function StringEnum<T extends readonly string[]>(values: T, options?: Record<string, unknown>) {
	return Type.Unsafe<T[number]>({ type: "string", enum: [...values], ...options });
}

interface ParsedModel {
	provider: string;
	model: string;
	context: string;
	maxOutput: string;
	thinking: boolean;
	images: boolean;
	company: string;
	generation: number;
}

interface Recommendation {
	provider: string;
	model: string;
	context: string;
	maxOutput: string;
	thinking: boolean;
	why: string;
}

interface PickedRoleModel {
	model_id: string;
	model: string;
	provider: string;
	company: string;
	short_name: string;
	context: string;
	max_output: string;
	thinking: boolean;
	price_per_m_input: number;
	tok_per_sec: number;
	coding_score: number | null;
	intelligence_score: number | null;
	agentic_score: number | null;
}

type PickPayload = Record<string, PickedRoleModel>;

interface ModelSelectionPreferences {
	preferredProviders?: string[];
	preferredCompanies?: string[];
	preferredModels?: string[];
	avoidProviders?: string[];
}

const DEFAULT_PREFERENCE_PROFILE: ModelSelectionPreferences = {
	preferredProviders: ["openai", "openai-codex", "anthropic"],
	preferredCompanies: ["openai", "anthropic"],
	preferredModels: ["codex"],
	avoidProviders: ["github-copilot"],
};

function parseContextSize(s: string): number {
	const match = s.match(/([\d.]+)K/);
	return match ? parseFloat(match[1]) * 1000 : 0;
}

const LATEST_FALLBACK_GENERATIONS = 2;

/** Providers we care about. Filters out noise/unconfigured vendors. */
const PROVIDERS = new Set(["anthropic", "openai", "openai-codex", "google", "z.ai", "github-copilot"]);

function inferCompany(provider: string, model: string): string {
	if (provider === "openai" || provider === "openai-codex") return "openai";
	if (provider === "anthropic") return "anthropic";
	if (provider === "google") return "google";
	if (provider === "z.ai") return "z.ai";

	const lower = model.toLowerCase();
	if (lower.startsWith("gpt") || lower.startsWith("o")) return "openai";
	if (lower.startsWith("claude")) return "anthropic";
	if (lower.startsWith("gemini")) return "google";
	if (lower.startsWith("grok")) return "xai";
	return provider;
}

function extractGeneration(model: string): number {
	const normalized = model.toLowerCase().replace(/(\d)-(\d)/g, "$1.$2");
	const tokens = normalized.match(/\d+(?:\.\d+)?/g) ?? [];

	for (const token of tokens) {
		// Ignore likely date stamps like 20251001.
		if (!token.includes(".") && token.length >= 6) continue;
		const n = Number.parseFloat(token);
		if (Number.isFinite(n)) return n;
	}

	return 0;
}

function parseModelsTable(stdout: string): ParsedModel[] {
	const lines = stdout.trim().split("\n");
	const models: ParsedModel[] = [];

	for (const line of lines) {
		if (line.includes("provider") && line.includes("model")) continue;
		if (!line.trim()) continue;

		const parts = line.trim().split(/\s{2,}/);
		if (parts.length < 6) continue;

		const provider = parts[0];
		if (!PROVIDERS.has(provider)) continue;
		const model = parts[1];

		models.push({
			provider,
			model,
			context: parts[2],
			maxOutput: parts[3],
			thinking: parts[4] === "yes",
			images: parts[5] === "yes",
			company: inferCompany(provider, model),
			generation: extractGeneration(model),
		});
	}
	return models;
}

function filterLatestGenerations(models: ParsedModel[], maxGenerations: number = LATEST_FALLBACK_GENERATIONS): ParsedModel[] {
	const generationsByCompany = new Map<string, Set<number>>();
	for (const m of models) {
		if (!m.generation || m.generation <= 0) continue;
		const company = m.company || m.provider;
		const current = generationsByCompany.get(company) ?? new Set<number>();
		current.add(m.generation);
		generationsByCompany.set(company, current);
	}

	const allowedByCompany = new Map<string, Set<number>>();
	for (const [company, generations] of generationsByCompany.entries()) {
		const top = [...generations].sort((a, b) => b - a).slice(0, maxGenerations);
		allowedByCompany.set(company, new Set(top));
	}

	return models.filter((m) => {
		if (!m.generation || m.generation <= 0) return true;
		const company = m.company || m.provider;
		return allowedByCompany.get(company)?.has(m.generation) ?? true;
	});
}

function filterAvoidedProviders(models: ParsedModel[], avoidProviders?: string[]): ParsedModel[] {
	if (!avoidProviders || avoidProviders.length === 0) return models;
	const avoided = new Set(avoidProviders.map((p) => p.toLowerCase()));
	const keep = models.filter((m) => !avoided.has(m.provider));
	return keep.length > 0 ? keep : models;
}

function buildGenerationRank(models: ParsedModel[]): Map<string, number> {
	const generationsByCompany = new Map<string, Set<number>>();
	for (const m of models) {
		if (!m.generation || m.generation <= 0) continue;
		const company = m.company || m.provider;
		const current = generationsByCompany.get(company) ?? new Set<number>();
		current.add(m.generation);
		generationsByCompany.set(company, current);
	}

	const rank = new Map<string, number>();
	for (const [company, generations] of generationsByCompany.entries()) {
		[...generations].sort((a, b) => b - a).forEach((generation, idx) => {
			rank.set(`${company}:${generation}`, idx);
		});
	}
	return rank;
}

function preferenceBonus(model: ParsedModel, preferences?: ModelSelectionPreferences): number {
	if (!preferences) return 0;

	let bonus = 0;
	const provider = model.provider.toLowerCase();
	const company = (model.company || model.provider).toLowerCase();
	const text = `${model.model} ${provider}`.toLowerCase();

	const preferredProviders = new Set((preferences.preferredProviders ?? []).map((p) => p.toLowerCase()));
	const preferredCompanies = new Set((preferences.preferredCompanies ?? []).map((c) => c.toLowerCase()));
	const preferredModels = (preferences.preferredModels ?? []).map((m) => m.toLowerCase()).filter(Boolean);

	if (preferredProviders.has(provider)) bonus += 25;
	if (preferredCompanies.has(company)) bonus += 18;
	for (const needle of preferredModels) {
		if (text.includes(needle)) bonus += 8;
	}
	if (company === "openai" && text.includes("codex")) bonus += 6;

	return bonus;
}

function recencyBonus(model: ParsedModel, generationRank: Map<string, number>): number {
	if (!model.generation || model.generation <= 0) return 0;
	const company = model.company || model.provider;
	const rank = generationRank.get(`${company}:${model.generation}`);
	if (rank === 0) return 12;
	if (rank === 1) return 4;
	return -4;
}

/**
 * Score a model for a given role. Higher is better.
 */
function scoreForRole(
	model: ParsedModel,
	role: string,
	preferences?: ModelSelectionPreferences,
	generationRank: Map<string, number> = new Map(),
): number {
	const ctx = parseContextSize(model.context);
	const out = parseContextSize(model.maxOutput);
	const modelName = model.model.toLowerCase();
	let score = 0;

	switch (role) {
		case "scout":
			score = model.thinking ? 10 : 0;
			score -= ctx / 50000;
			if (modelName.includes("haiku") || modelName.includes("spark") || modelName.includes("mini")) {
				score += 20;
			}
			break;

		case "planner":
		case "reviewer":
			score = model.thinking ? 30 : 0;
			score += ctx / 10000;
			score += out / 10000;
			if (modelName.includes("opus") || model.generation >= 5) score += 20;
			break;

		case "coder":
			score = model.thinking ? 20 : 0;
			score += out / 5000;
			score += ctx / 20000;
			if (modelName.includes("opus") || modelName.includes("codex") || model.generation >= 5) {
				score += 15;
			}
			break;

		default:
			score = model.thinking ? 30 : 0;
			score += ctx / 10000;
			score += out / 10000;
			if (modelName.includes("opus") || model.generation >= 5) score += 20;
			break;
	}

	score += recencyBonus(model, generationRank);
	score += preferenceBonus(model, preferences);
	return score;
}

/**
 * Pick top recommendations with provider diversity.
 */
function recommend(
	models: ParsedModel[],
	role: string,
	count: number = 3,
	preferences?: ModelSelectionPreferences,
): Recommendation[] {
	const viable = models.filter((m) => {
		if (role === "scout") return true;
		return m.thinking;
	});

	const recent = filterLatestGenerations(viable, LATEST_FALLBACK_GENERATIONS);
	const preferredPool = filterAvoidedProviders(recent, preferences?.avoidProviders);
	const generationRank = buildGenerationRank(preferredPool);

	const scored = preferredPool.map((m) => ({
		model: m,
		score: scoreForRole(m, role, preferences, generationRank),
	}));
	scored.sort((a, b) => b.score - a.score);

	const recs: Recommendation[] = [];
	const usedProviders = new Set<string>();

	for (const { model: m } of scored) {
		if (recs.length >= count) break;

		if (recs.length > 0 && usedProviders.has(m.provider)) {
			const allProviders = new Set(scored.map((s) => s.model.provider));
			if (usedProviders.size < allProviders.size) continue;
		}

		const freshness = recencyBonus(m, generationRank) >= 12 ? ", latest generation" : "";
		const why =
			role === "scout"
				? m.model.includes("haiku") || m.model.includes("spark") || m.model.includes("mini")
					? "fast/cheap, good for recon"
					: "capable scout"
				: `strong reasoning, ${m.context} context, ${m.maxOutput} output${m.thinking ? ", thinking" : ""}${freshness}`;

		recs.push({
			provider: m.provider,
			model: m.model,
			context: m.context,
			maxOutput: m.maxOutput,
			thinking: m.thinking,
			why,
		});
		usedProviders.add(m.provider);
	}

	return recs;
}

// --- Model selection via skill script ---

/** Role-specific guidance for the LLM to interpret the model table. */
function roleGuidance(role: string): string {
	switch (role) {
		case "scout":
			return "**Goal: fast/cheap model.** Pick the lowest $/M in with decent tok/s. Thinking support optional.";
		case "coder":
			return "**Goal: strongest coder.** Pick the highest Coding score with large Max Output (128K+) and thinking support. Speed (tok/s) is secondary.";
		case "planner":
			return "**Goal: strong reasoner.** Pick the highest Intel/Agentic scores with thinking support and large context.";
		case "reviewer":
			return "**Goal: thorough reviewer.** Pick the highest Intel score with thinking support. Different company than the coder for diverse perspective.";
		default:
			return "**Goal: strongest overall.** Pick the highest benchmark scores with thinking support.";
	}
}

/** Candidate paths for model picker script across common install locations. */
const PICK_MODEL_SCRIPT_CANDIDATES = [
	".agents/skills/model-selection/pick-models.py",
	"$HOME/.agents/skills/model-selection/pick-models.py",
	"skills/model-selection/pick-models.py",
];

function resolveCandidate(path: string): string {
	if (path.startsWith("$HOME")) {
		return path.replace("$HOME", process.env.HOME || "");
	}
	return path;
}

function fileExists(path: string): boolean {
	try {
		accessSync(path, constants.F_OK);
		return true;
	} catch {
		return false;
	}
}

function appendPreferenceArgs(args: string[], values: string[] | undefined, flag: string): string[] {
	if (!values || values.length === 0) return args;
	for (const value of values) {
		if (!value) continue;
		for (const token of value.split(",")) {
			const v = token.trim();
			if (v) args.push(flag, v);
		}
	}
	return args;
}

function hasExplicitPreferences(preferences: {
	preferredProviders?: string[];
	preferredCompanies?: string[];
	preferredModels?: string[];
	avoidProviders?: string[];
}): boolean {
	return !!(
		(preferences.preferredProviders && preferences.preferredProviders.length > 0) ||
		(preferences.preferredCompanies && preferences.preferredCompanies.length > 0) ||
		(preferences.preferredModels && preferences.preferredModels.length > 0) ||
		(preferences.avoidProviders && preferences.avoidProviders.length > 0)
	);
}

function buildPreferencesFromParams(params: {
	preferredProviders?: string[];
	preferredCompanies?: string[];
	preferredModels?: string[];
	avoidProviders?: string[];
}): { preferences: ModelSelectionPreferences; usedDefaults: boolean } {
	const userPreferences: ModelSelectionPreferences = {
		preferredProviders: params.preferredProviders,
		preferredCompanies: params.preferredCompanies,
		preferredModels: params.preferredModels,
		avoidProviders: params.avoidProviders,
	};

	if (hasExplicitPreferences(userPreferences)) {
		return { preferences: userPreferences, usedDefaults: false };
	}

	return { preferences: DEFAULT_PREFERENCE_PROFILE, usedDefaults: true };
}

async function tryPickModelsScript(
	pi: ExtensionAPI,
	role: string,
	preferences?: ModelSelectionPreferences,
	signal?: AbortSignal,
): Promise<{ role: string; pick: PickedRoleModel } | null> {
	const candidates = PICK_MODEL_SCRIPT_CANDIDATES.map(resolveCandidate);
	const argsBase = ["--json"];

	if (role !== "any") {
		argsBase.push("--role", role);
	}

	appendPreferenceArgs(argsBase, preferences?.preferredProviders, "--prefer-provider");
	appendPreferenceArgs(argsBase, preferences?.preferredCompanies, "--prefer-company");
	appendPreferenceArgs(argsBase, preferences?.preferredModels, "--prefer-model");
	appendPreferenceArgs(argsBase, preferences?.avoidProviders, "--avoid-provider");

	for (const script of candidates) {
		if (!fileExists(script)) continue;

		try {
			const result = await pi.exec("python3", [script, ...argsBase], {
				timeout: 30000,
				signal,
			});
			if (result.code !== 0 || !result.stdout.trim()) continue;

			const payload = JSON.parse(result.stdout) as PickPayload;
			const pickRole = role === "any" ? "coder" : role;
			const pick = payload[pickRole] || payload[Object.keys(payload)[0]];
			if (!pick) continue;

			return { role: pickRole, pick };
		} catch {
			continue;
		}
	}

	return null;
}

export function registerModels(pi: ExtensionAPI) {
	pi.registerTool({
		name: "models",
		label: "Models",
		description: "Discover available models with live benchmarks, pricing, and throughput. Always call this before creating agents to ensure you pick the right models from diverse providers.",
		promptSnippet: "Discover available AI models with benchmarks, pricing, and role-based guidance (scout, planner, coder, reviewer)",
		promptGuidelines: [
			"Call models() before dispatch or debate to pick role-appropriate models.",
			"The model picker is recency-aware (2 latest generations per company) and tracks 429-downed providers.",
			"Default preference policy: prefer OpenAI/Anthropic providers, favor OpenAI Codex variants, avoid github-copilot unless needed.",
			"If a provider starts returning 429, re-run models() and it will shift to alternatives where possible.",
			"Pass explicit user preferences through preferredProviders / preferredModels when known.",
			"Prefer skipping github-copilot when alternatives are available unless user explicitly asks for it.",
			"Always use different providers/companies in debates for diverse perspectives.",
			"Never hardcode model names — they change frequently.",
			"When users have fixed preferences, consider running models() once and reusing the selected ids for the same workflow."
		],
		parameters: Type.Object({
			role: Type.Optional(
				StringEnum(["planner", "coder", "reviewer", "scout", "any"] as const, {
					description: 'Role to optimize for. Default: "any"',
					default: "any",
				}),
			),
			preferredProviders: Type.Optional(Type.Array(Type.String(), {
				description: "Preferred providers, e.g. ['openai', 'openai-codex', 'anthropic']",
			})),
			preferredCompanies: Type.Optional(Type.Array(Type.String(), {
				description: "Preferred model companies for selection, e.g. ['anthropic', 'openai']",
			})),
			preferredModels: Type.Optional(Type.Array(Type.String(), {
				description: "Preferred model substrings/hints, e.g. ['codex']",
			})),
			avoidProviders: Type.Optional(Type.Array(Type.String(), {
				description: "Providers to avoid unless no alternatives remain, e.g. ['github-copilot']",
			})),
		}),

		async execute(_toolCallId, params, signal, _onUpdate, _ctx) {
			const role = params.role || "any";
			const { preferences, usedDefaults } = buildPreferencesFromParams({
				preferredProviders: params.preferredProviders,
				preferredCompanies: params.preferredCompanies,
				preferredModels: params.preferredModels,
				avoidProviders: params.avoidProviders,
			});

			// Primary: role-aware recommendations from model-selection skill (recency + 429-aware)
			const pick = await tryPickModelsScript(pi, role, preferences, signal);

			if (pick) {
				const p = pick.pick;
				const preferenceLines: string[] = [];
				if (usedDefaults) {
					preferenceLines.push("  - preference profile: default (openai/anthropic preferred, codex favored, github-copilot avoided when possible)");
				} else {
					for (const [name, values] of Object.entries(preferences) as [string, string[] | undefined][]) {
						if (values && values.length > 0) {
							preferenceLines.push(`  - preference ${name}: ${values.join(", ")}`);
						}
					}
				}

				const text = [
					`## Model pick for role: ${pick.role}`,
					"",
					roleGuidance(pick.role),
					...(preferenceLines.length ? ["", "Preference hints:", ...preferenceLines] : []),
					"",
					`- **${p.model_id}** (${p.company})`,
					`  - coding: ${p.coding_score ?? "—"}  intel: ${p.intelligence_score ?? "—"}  agentic: ${p.agentic_score ?? "—"}`,
					`  - $/M in: $${p.price_per_m_input.toFixed(2)}  tok/s: ${p.tok_per_sec}  context: ${p.context}  output: ${p.max_output}`,
					`  - think: ${p.thinking ? "yes" : "no"}`,
				].join("\n");

				logOperation(process.env.FORGE_WORKDIR, {
					tool: "models",
					timestamp: new Date().toISOString(),
					params: { role, roleSelected: pick.role, model: p.model_id, preferences, usedDefaults },
					result: `recency-aware pick (${p.model_id}) via model-selection skill`,
				});

				return {
					content: [{ type: "text", text }],
					details: { role, source: "skill" as const, model: p.model_id },
				};
			}

			// Fallback: basic pi --list-models with heuristic scoring
			const result = await pi.exec("pi", ["--list-models"], { signal, timeout: 10000 });

			if (result.code !== 0) {
				throw new Error(`Failed to list models: ${result.stderr}`);
			}

			const allModels = parseModelsTable(result.stdout);
			if (allModels.length === 0) {
				throw new Error("No models found. Check pi configuration.");
			}

			const recommended = recommend(allModels, role, 3, preferences);

			logOperation(process.env.FORGE_WORKDIR, {
				tool: "models",
				timestamp: new Date().toISOString(),
				params: { role, preferences, usedDefaults },
				result: `${recommended.length} recommended from ${new Set(allModels.map(m => m.provider)).size} providers (${allModels.length} total, fallback recency-aware)`,
			});

			const output = {
				role,
				recommended,
				all: allModels,
			};

			const text = [
				`## Models for role: ${role}`,
				"",
				"_Fallback mode: model-selection skill unavailable; using recency- and preference-aware heuristics from `pi --list-models`._",
				"",
				"### Recommended (diverse providers)",
				"",
			];

			for (const r of recommended) {
				text.push(`- **${r.provider}/${r.model}** — ${r.why}`);
			}

			text.push("", `### All available: ${allModels.length} models across ${new Set(allModels.map((m) => m.provider)).size} providers`);

			return {
				content: [{ type: "text", text: text.join("\n") }],
				details: output,
			};
		},

		renderCall(args, theme) {
			const role = args.role || "any";
			return new Text(
				theme.fg("toolTitle", theme.bold("models ")) + theme.fg("accent", role),
				0,
				0,
			);
		},

		renderResult(result, _options, theme) {
			const details = result.details as { role: string; source?: string; recommended?: Recommendation[] } | undefined;
			if (!details) {
				const text = result.content[0];
				return new Text(text?.type === "text" ? text.text : "(no output)", 0, 0);
			}

			if (details.source === "skill") {
				return new Text(
					theme.fg("success", "✓ ") +
					theme.fg("toolTitle", theme.bold("models")) +
					theme.fg("muted", ` (${details.role})`) +
					theme.fg("dim", " — recency/429-aware pick"),
					0, 0,
				);
			}

			let text =
				theme.fg("success", "✓ ") +
				theme.fg("toolTitle", theme.bold("models")) +
				theme.fg("muted", ` (${details.role})`) +
				theme.fg("dim", " — fallback");
			for (const r of details.recommended ?? []) {
				text += `\n  ${theme.fg("accent", `${r.provider}/${r.model}`)} ${theme.fg("dim", `— ${r.why}`)}`;
			}
			return new Text(text, 0, 0);
		},
	});
}
