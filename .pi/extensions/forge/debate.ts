/**
 * debate tool — Multi-round debate between agents until convergence.
 *
 * Dispatches agents in parallel each round, writes all input/output to workDir,
 * checks for explicit convergence votes. Pure plumbing — no judgment or bail logic.
 */

import * as fs from "node:fs";
import * as path from "node:path";
import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
/** Inline replacement for pi-ai's StringEnum to avoid transitive dep issues. */
function StringEnum<T extends readonly string[]>(values: T, options?: Record<string, unknown>) {
	return Type.Unsafe<T[number]>({ type: "string", enum: [...values], ...options });
}
import { Type } from "@sinclair/typebox";
import { Text } from "@mariozechner/pi-tui";
import { runAgent, getFinalOutput, type UsageStats, type DispatchResult } from "./dispatch.js";
import { logOperation } from "./log.js";

// ── Types ──────────────────────────────────────────────────────────

interface AgentSpec {
	persona: string;
	model?: string;
	tools?: string[];
	thinking?: string;
	name?: string;
}

interface RoundResult {
	round: number;
	agentOutputs: Array<{
		name: string;
		file: string;
		output: string;
		usage: UsageStats;
		model?: string;
	}>;
}

interface DebateResult {
	converged: boolean;
	rounds: number;
	reason: string;
	convergence: string;
	workDir: string;
	roundFiles: string[];
	consensusFile?: string;
	totalUsage: UsageStats;
}

interface DebateDetails {
	result: DebateResult;
	roundResults: RoundResult[];
}

// ── Helpers ────────────────────────────────────────────────────────

const MAX_CONCURRENCY = 4;

function agentName(spec: AgentSpec, index: number): string {
	if (spec.name) return spec.name;
	if (spec.model) {
		// Extract short name from model ID: "anthropic/claude-opus-4-6" -> "opus"
		const parts = spec.model.split("/");
		const modelName = parts[parts.length - 1];
		// Try to find a distinctive short name
		for (const keyword of ["opus", "sonnet", "haiku", "codex", "gpt", "gemini", "spark"]) {
			if (modelName.includes(keyword)) return keyword;
		}
		return modelName.slice(0, 12);
	}
	return `agent-${index + 1}`;
}

function addUsage(total: UsageStats, add: UsageStats): void {
	total.input += add.input;
	total.output += add.output;
	total.cacheRead += add.cacheRead;
	total.cacheWrite += add.cacheWrite;
	total.cost += add.cost;
	total.turns += add.turns;
}

function emptyUsage(): UsageStats {
	return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0, contextTokens: 0, turns: 0 };
}

function escapeRegex(s: string): string {
	return s.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

type ConvergenceVote = "true" | "false" | "missing";

/**
 * Parse structured convergence vote from output.
 * Preferred format (standalone line): "LGTM: True" / "LGTM: False".
 * Also accepts custom convergence key: "<KEY>: True|False".
 */
function parseConvergenceVote(output: string, convergence: string): ConvergenceVote {
	const c = escapeRegex(convergence.trim());
	if (!c) return "missing";

	const structured = new RegExp(`^\\s*(?:[-*]\\s*)?${c}\\s*:\\s*(true|false)\\s*(?:[.!])?\\s*$`, "im");
	const match = structured.exec(output);
	if (match) {
		return match[1].toLowerCase() === "true" ? "true" : "false";
	}

	// Backward compatibility: legacy standalone keyword is treated as True.
	const legacy = new RegExp(`^\\s*(?:[-*]\\s*)?${c}\\s*(?:[.!])?\\s*$`, "im");
	if (legacy.test(output)) return "true";

	return "missing";
}

async function mapWithConcurrencyLimit<TIn, TOut>(
	items: TIn[],
	concurrency: number,
	fn: (item: TIn, index: number) => Promise<TOut>,
): Promise<TOut[]> {
	if (items.length === 0) return [];
	const limit = Math.max(1, Math.min(concurrency, items.length));
	const results: TOut[] = new Array(items.length);
	let nextIndex = 0;
	const workers = new Array(limit).fill(null).map(async () => {
		while (true) {
			const current = nextIndex++;
			if (current >= items.length) return;
			results[current] = await fn(items[current], current);
		}
	});
	await Promise.all(workers);
	return results;
}

// ── Core: runDebate ────────────────────────────────────────────────

async function runDebate(
	params: {
		topic: string;
		contextFiles?: string[];
		agents: AgentSpec[];
		topology: "round-table" | "adversarial";
		convergence: string;
		maxRounds: number;
		workDir: string;
		cwd: string;
		forgeDepth: number;
	},
	signal: AbortSignal | undefined,
	onUpdate?: (text: string, details: DebateDetails) => void,
): Promise<DebateDetails> {
	fs.mkdirSync(params.workDir, { recursive: true });

	const names = params.agents.map((a, i) => agentName(a, i));
	const totalUsage = emptyUsage();
	const roundResults: RoundResult[] = [];
	const allRoundFiles: string[] = [];

	for (let round = 1; round <= params.maxRounds; round++) {
		// Build a minimal prompt — agents can read files themselves
		let roundPrompt = `# Debate: ${params.topic}\n\n`;
		roundPrompt += `Round ${round}/${params.maxRounds}. `;
		roundPrompt += `Give your analysis concisely. `;
		roundPrompt += `End with exactly one standalone decision line: "${params.convergence}: True" if you fully agree, or "${params.convergence}: False" if you disagree. `;
		roundPrompt += `Do not emit bare "${params.convergence}" without a boolean.\n`;

		// Point to context files — don't paste them
		if (params.contextFiles && params.contextFiles.length > 0) {
			roundPrompt += `\n## Context files (read as needed)\n`;
			for (const f of params.contextFiles) {
				roundPrompt += `- ${f}\n`;
			}
		}

		// Point to prior round files — don't paste them
		if (round > 1) {
			roundPrompt += `\n## Prior rounds (read as needed)\n`;
			for (const prev of roundResults) {
				for (const ao of prev.agentOutputs) {
					roundPrompt += `- Round ${prev.round}, ${ao.name}: ${ao.file}\n`;
				}
			}
		}

		// Write the round prompt
		const promptFile = path.join(params.workDir, `round-${round}-prompt.md`);
		fs.writeFileSync(promptFile, roundPrompt, "utf-8");

		// Dispatch agents
		let agentOutputs: RoundResult["agentOutputs"];

		if (params.topology === "adversarial" && params.agents.length === 2 && round > 1) {
			// Adversarial: agents go sequentially, each seeing the other's latest
			agentOutputs = [];
			let adversarialPrompt = roundPrompt;

			for (let i = 0; i < 2; i++) {
				const spec = params.agents[i];
				const name = names[i];

				const dispatchResult = await runAgent({
					persona: spec.persona,
					task: adversarialPrompt,
					model: spec.model,
					tools: spec.tools,
					thinking: spec.thinking,
					cwd: params.cwd,
					forgeDepth: params.forgeDepth,
					signal,
				});

				const output = getFinalOutput(dispatchResult.messages);
				const outFile = path.join(params.workDir, `round-${round}-agent-${name}.md`);
				fs.writeFileSync(outFile, output, "utf-8");
				allRoundFiles.push(`round-${round}-agent-${name}.md`);

				agentOutputs.push({
					name,
					file: outFile,
					output,
					usage: dispatchResult.usage,
					model: dispatchResult.model,
				});
				addUsage(totalUsage, dispatchResult.usage);

				// Second agent gets a pointer to first agent's file
				if (i === 0) {
					adversarialPrompt += `\n\n${name} just responded — read ${outFile} before giving your take.\n`;
				}
			}
		} else {
			// Round-table: all agents in parallel
			const dispatches = await mapWithConcurrencyLimit(
				params.agents,
				MAX_CONCURRENCY,
				async (spec, i) => {
					const name = names[i];
					const dispatchResult = await runAgent({
						persona: spec.persona,
						task: roundPrompt,
						model: spec.model,
						tools: spec.tools,
						thinking: spec.thinking,
						cwd: params.cwd,
						forgeDepth: params.forgeDepth,
						signal,
					});

					const output = getFinalOutput(dispatchResult.messages);
					const outFile = path.join(params.workDir, `round-${round}-agent-${name}.md`);
					fs.writeFileSync(outFile, output, "utf-8");
					allRoundFiles.push(`round-${round}-agent-${name}.md`);

					return {
						name,
						file: outFile,
						output,
						usage: dispatchResult.usage,
						model: dispatchResult.model,
					};
				},
			);

			agentOutputs = dispatches;
			for (const d of dispatches) addUsage(totalUsage, d.usage);
		}

		const roundResult: RoundResult = { round, agentOutputs };
		roundResults.push(roundResult);

		// Check convergence: did ALL agents vote "<convergence>: True"?
		const votes = agentOutputs.map((ao) => parseConvergenceVote(ao.output, params.convergence));
		const allConverged = votes.every((vote) => vote === "true");

		const debateResult: DebateResult = {
			converged: allConverged,
			rounds: round,
			reason: allConverged ? `all agents voted ${params.convergence}: true` : round >= params.maxRounds ? "max_rounds" : "in_progress",
			convergence: params.convergence,
			workDir: params.workDir,
			roundFiles: allRoundFiles,
			totalUsage,
		};

		const details: DebateDetails = { result: debateResult, roundResults };

		const costStr = totalUsage.cost > 0 ? ` ($${totalUsage.cost.toFixed(4)} so far)` : "";
		onUpdate?.(
			allConverged
				? `✓ Converged after ${round} round${round > 1 ? "s" : ""}${costStr}`
				: `Round ${round}/${params.maxRounds} complete — not yet converged${costStr}`,
			details,
		);

		if (allConverged) {
			// Write consensus file
			const consensusFile = path.join(params.workDir, "consensus.md");
			let consensus = `# Consensus: ${params.topic}\n\nConverged after ${round} round${round > 1 ? "s" : ""}.\n\n`;
			for (const ao of agentOutputs) {
				consensus += `## ${ao.name}\n\n${ao.output}\n\n`;
			}
			fs.writeFileSync(consensusFile, consensus, "utf-8");
			debateResult.consensusFile = "consensus.md";

			return { result: debateResult, roundResults };
		}
	}

	// Max rounds reached without convergence
	return {
		result: {
			converged: false,
			rounds: params.maxRounds,
			reason: "max_rounds",
			convergence: params.convergence,
			workDir: params.workDir,
			roundFiles: allRoundFiles,
			totalUsage,
		},
		roundResults,
	};
}

// ── Tool Registration ──────────────────────────────────────────────

export function registerDebate(pi: ExtensionAPI, currentDepth: number) {
	pi.registerTool({
		name: "debate",
		label: "Debate",
		description:
			"Run a multi-round debate between agents until convergence. Agents are dispatched in parallel each round. All input/output is written to workDir for file-based handoff. The tool is mechanical — it runs rounds and checks for convergence. All bail decisions are yours.",
		promptSnippet: "Run a multi-round debate between dynamically created agents until they converge (LGTM)",
		promptGuidelines: [
			"Create agent personas that match the problem domain — don't use generic reviewers.",
			"Always use models from different providers in debates for diverse perspectives.",
			"Require explicit decision lines in outputs: '<convergence>: True|False' (e.g., 'LGTM: True').",
			"Read the round files in workDir to assess debate quality and decide whether to continue.",
			"YOU decide when to bail — if agents are stalling, oscillating, or the task is underspecified, stop and reassess.",
			"Use 'round-table' topology for design/planning, 'adversarial' for focused code review (2 agents).",
		],
		parameters: Type.Object({
			topic: Type.String({ description: "What to debate" }),
			contextFiles: Type.Optional(Type.Array(Type.String(), { description: "File paths agents should read for context" })),
			agents: Type.Array(
				Type.Object({
					persona: Type.String({ description: "System prompt for this agent" }),
					model: Type.Optional(Type.String({ description: "Model ID" })),
					tools: Type.Optional(Type.Array(Type.String())),
					thinking: Type.Optional(Type.String()),
					name: Type.Optional(Type.String({ description: "Short name for file naming, e.g. 'opus', 'codex'" })),
				}),
				{ description: "Agents to participate in the debate (2+)", minItems: 2 },
			),
			topology: Type.Optional(
				StringEnum(["round-table", "adversarial"] as const, {
					description: "Debate topology. round-table: all see all. adversarial: 1v1 back-and-forth (2 agents only).",
					default: "round-table",
				}),
			),
			convergence: Type.Optional(Type.String({ description: 'Decision key used as "<key>: True|False". Default: "LGTM"', default: "LGTM" })),
			maxRounds: Type.Optional(Type.Number({ description: "Maximum rounds before stopping. Default: 4", default: 4 })),
			workDir: Type.String({ description: "Directory where round files are written" }),
		}),

		async execute(_toolCallId, params, signal, onUpdate, ctx) {
			const startTime = Date.now();
			const topology = params.topology || "round-table";
			const convergence = params.convergence || "LGTM";
			const maxRounds = params.maxRounds || 4;

			if (topology === "adversarial" && params.agents.length !== 2) {
				throw new Error("Adversarial topology requires exactly 2 agents.");
			}

			const { result, roundResults } = await runDebate(
				{
					topic: params.topic,
					contextFiles: params.contextFiles,
					agents: params.agents,
					topology,
					convergence,
					maxRounds,
					workDir: params.workDir,
					cwd: ctx.cwd,
					forgeDepth: currentDepth,
				},
				signal,
				onUpdate
					? (text, details) => {
							onUpdate({
								content: [{ type: "text", text }],
								details,
							});
						}
					: undefined,
			);

			const duration = Date.now() - startTime;
			logOperation(params.workDir, {
				tool: "debate",
				timestamp: new Date().toISOString(),
				params: { topic: params.topic, topology, maxRounds, agents: params.agents, workDir: params.workDir },
				result: result.converged
					? `Converged after ${result.rounds} round(s)`
					: `Did NOT converge after ${result.rounds} rounds (${result.reason})`,
				cost: result.totalUsage.cost,
				duration,
			});

			const summary = result.converged
				? `Debate converged after ${result.rounds} round${result.rounds > 1 ? "s" : ""}. All agents voted ${convergence}: True.\n\nSee ${result.workDir}/consensus.md for the full consensus.`
				: `Debate did NOT converge after ${result.rounds} rounds (reason: ${result.reason}).\n\nReview the round files in ${result.workDir}/ to assess the situation and decide next steps.`;

			const costLine = result.totalUsage.cost > 0 ? `\nTotal cost: $${result.totalUsage.cost.toFixed(4)}` : "";

			return {
				content: [{ type: "text", text: summary + costLine }],
				details: { result, roundResults } as DebateDetails,
			};
		},

		renderCall(args, theme) {
			const topology = args.topology || "round-table";
			const agentCount = args.agents?.length || 0;
			const maxRounds = args.maxRounds || 4;

			let text = theme.fg("toolTitle", theme.bold("debate "));
			text += theme.fg("accent", `${topology} (${agentCount} agents, max ${maxRounds} rounds)`);
			const topic = args.topic?.length > 60 ? `${args.topic.slice(0, 60)}...` : (args.topic || "...");
			text += `\n  ${theme.fg("dim", topic)}`;

			if (args.agents) {
				for (const a of args.agents.slice(0, 3)) {
					const name = a.name || a.model || "agent";
					const persona = a.persona?.length > 40 ? `${a.persona.slice(0, 40)}...` : (a.persona || "");
					text += `\n  ${theme.fg("accent", name)} ${theme.fg("dim", persona)}`;
				}
				if (args.agents.length > 3) text += `\n  ${theme.fg("muted", `... +${args.agents.length - 3} more`)}`;
			}

			return new Text(text, 0, 0);
		},

		renderResult(result, _options, theme) {
			const details = result.details as DebateDetails | undefined;
			if (!details?.result) {
				const text = result.content[0];
				return new Text(text?.type === "text" ? text.text : "(no output)", 0, 0);
			}

			const r = details.result;
			const icon = r.converged ? theme.fg("success", "✓") : theme.fg("warning", "◐");
			const status = r.converged
				? `converged after ${r.rounds} round${r.rounds > 1 ? "s" : ""}`
				: `${r.rounds} rounds — ${r.reason}`;

			let text = `${icon} ${theme.fg("toolTitle", theme.bold("debate"))} ${theme.fg("accent", status)}`;

			// Show per-round summary
			for (const round of details.roundResults) {
				text += `\n  ${theme.fg("muted", `Round ${round.round}:`)}`;
				for (const ao of round.agentOutputs) {
					const vote = parseConvergenceVote(ao.output, r.convergence || "LGTM");
					const rIcon =
						vote === "true"
							? theme.fg("success", "✓")
							: vote === "false"
								? theme.fg("warning", "✗")
								: theme.fg("muted", "?");
					text += ` ${rIcon}${theme.fg("dim", ao.name)}`;
				}
			}

			if (r.totalUsage.cost > 0) {
				text += `\n  ${theme.fg("dim", `$${r.totalUsage.cost.toFixed(4)}`)}`;
			}

			return new Text(text, 0, 0);
		},
	});
}
