/**
 * dispatch tool — Run an ephemeral agent with an inline persona.
 *
 * Spawns a separate `pi` process with the persona as an appended system prompt.
 * The agent inherits project skills, extensions, and AGENTS.md.
 *
 * Adapted from the existing subagent extension's runSingleAgent().
 */

import { spawn } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
/** Inline replacement for pi-ai's StringEnum to avoid transitive dep issues. */
function StringEnum<T extends readonly string[]>(values: T, options?: Record<string, unknown>) {
	return Type.Unsafe<T[number]>({ type: "string", enum: [...values], ...options });
}

interface Message { role: string; content: string; }
import { Type } from "@sinclair/typebox";
import { Text } from "@mariozechner/pi-tui";
import { logOperation } from "./log.js";

// ── Types ──────────────────────────────────────────────────────────

export interface UsageStats {
	input: number;
	output: number;
	cacheRead: number;
	cacheWrite: number;
	cost: number;
	contextTokens: number;
	turns: number;
}

export interface DispatchResult {
	persona: string;
	task: string;
	exitCode: number;
	messages: Message[];
	stderr: string;
	usage: UsageStats;
	model?: string;
	stopReason?: string;
	errorMessage?: string;
}

interface DispatchDetails {
	result: DispatchResult;
}

// ── Helpers ────────────────────────────────────────────────────────

function emptyUsage(): UsageStats {
	return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, cost: 0, contextTokens: 0, turns: 0 };
}

export function getFinalOutput(messages: Message[]): string {
	for (let i = messages.length - 1; i >= 0; i--) {
		const msg = messages[i];
		if (msg.role === "assistant") {
			for (const part of msg.content) {
				if (part.type === "text") return part.text;
			}
		}
	}
	return "";
}

function writePromptToTempFile(name: string, prompt: string): { dir: string; filePath: string } {
	const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-forge-"));
	const safeName = name.replace(/[^\w.-]+/g, "_");
	const filePath = path.join(tmpDir, `persona-${safeName}.md`);
	fs.writeFileSync(filePath, prompt, { encoding: "utf-8", mode: 0o600 });
	return { dir: tmpDir, filePath };
}

function formatTokens(count: number): string {
	if (count < 1000) return count.toString();
	if (count < 10000) return `${(count / 1000).toFixed(1)}k`;
	if (count < 1000000) return `${Math.round(count / 1000)}k`;
	return `${(count / 1000000).toFixed(1)}M`;
}

function formatUsage(usage: UsageStats, model?: string): string {
	const parts: string[] = [];
	if (usage.turns) parts.push(`${usage.turns} turn${usage.turns > 1 ? "s" : ""}`);
	if (usage.input) parts.push(`↑${formatTokens(usage.input)}`);
	if (usage.output) parts.push(`↓${formatTokens(usage.output)}`);
	if (usage.cost) parts.push(`$${usage.cost.toFixed(4)}`);
	if (model) parts.push(model);
	return parts.join(" ");
}

// ── Core: runAgent ─────────────────────────────────────────────────

export interface RunAgentOptions {
	persona: string;
	task: string;
	model?: string;
	tools?: string[];
	thinking?: string;
	workDir?: string;
	cwd: string;
	forgeDepth: number;
	signal?: AbortSignal;
	onUpdate?: (text: string, usage: UsageStats) => void;
}

export async function runAgent(opts: RunAgentOptions): Promise<DispatchResult> {
	const args: string[] = ["--mode", "json", "-p", "--no-session"];

	if (opts.model) args.push("--model", opts.model);
	if (opts.tools && opts.tools.length > 0) args.push("--tools", opts.tools.join(","));
	if (opts.thinking) args.push("--thinking", opts.thinking);

	let tmpDir: string | null = null;
	let tmpPath: string | null = null;

	const result: DispatchResult = {
		persona: opts.persona,
		task: opts.task,
		exitCode: 0,
		messages: [],
		stderr: "",
		usage: emptyUsage(),
		model: opts.model,
	};

	try {
		// Write persona to temp file for --append-system-prompt
		if (opts.persona.trim()) {
			const tmp = writePromptToTempFile("agent", opts.persona);
			tmpDir = tmp.dir;
			tmpPath = tmp.filePath;
			args.push("--append-system-prompt", tmpPath);
		}

		// If workDir specified, prepend instruction to the task
		let taskText = opts.task;
		if (opts.workDir) {
			fs.mkdirSync(opts.workDir, { recursive: true });
			taskText = `[Write substantial output to files in ${opts.workDir}. Respond with a brief summary only.]\n\n${opts.task}`;
		}

		args.push(taskText);

		let wasAborted = false;

		const exitCode = await new Promise<number>((resolve) => {
			const env = {
				...process.env,
				FORGE_DEPTH: String(opts.forgeDepth + 1),
				...(opts.workDir ? { FORGE_WORKDIR: opts.workDir } : {}),
			};
			const proc = spawn("pi", args, {
				cwd: opts.cwd,
				env,
				shell: false,
				stdio: ["ignore", "pipe", "pipe"],
			});

			let buffer = "";

			const processLine = (line: string) => {
				if (!line.trim()) return;
				let event: any;
				try {
					event = JSON.parse(line);
				} catch {
					return;
				}

				if (event.type === "message_end" && event.message) {
					const msg = event.message as Message;
					result.messages.push(msg);

					if (msg.role === "assistant") {
						result.usage.turns++;
						const usage = msg.usage;
						if (usage) {
							result.usage.input += usage.input || 0;
							result.usage.output += usage.output || 0;
							result.usage.cacheRead += usage.cacheRead || 0;
							result.usage.cacheWrite += usage.cacheWrite || 0;
							result.usage.cost += usage.cost?.total || 0;
							result.usage.contextTokens = usage.totalTokens || 0;
						}
						if (!result.model && msg.model) result.model = msg.model;
						if (msg.stopReason) result.stopReason = msg.stopReason;
						if (msg.errorMessage) result.errorMessage = msg.errorMessage;
					}

					opts.onUpdate?.(getFinalOutput(result.messages) || "(running...)", result.usage);
				}

				if (event.type === "tool_result_end" && event.message) {
					result.messages.push(event.message as Message);
					opts.onUpdate?.(getFinalOutput(result.messages) || "(running...)", result.usage);
				}
			};

			proc.stdout.on("data", (data: Buffer) => {
				buffer += data.toString();
				const lines = buffer.split("\n");
				buffer = lines.pop() || "";
				for (const line of lines) processLine(line);
			});

			proc.stderr.on("data", (data: Buffer) => {
				result.stderr += data.toString();
			});

			proc.on("close", (code: number | null) => {
				if (buffer.trim()) processLine(buffer);
				resolve(code ?? 0);
			});

			proc.on("error", () => resolve(1));

			if (opts.signal) {
				const killProc = () => {
					wasAborted = true;
					proc.kill("SIGTERM");
					setTimeout(() => {
						if (!proc.killed) proc.kill("SIGKILL");
					}, 5000);
				};
				if (opts.signal.aborted) killProc();
				else opts.signal.addEventListener("abort", killProc, { once: true });
			}
		});

		result.exitCode = exitCode;
		if (wasAborted) throw new Error("Agent was aborted");
		return result;
	} finally {
		if (tmpPath) try { fs.unlinkSync(tmpPath); } catch { /* ignore */ }
		if (tmpDir) try { fs.rmdirSync(tmpDir); } catch { /* ignore */ }
	}
}

// ── Tool Registration ──────────────────────────────────────────────

export function registerDispatch(pi: ExtensionAPI, currentDepth: number) {
	pi.registerTool({
		name: "dispatch",
		label: "Dispatch",
		description:
			"Run an ephemeral agent with an inline persona. The agent inherits project skills, extensions, and AGENTS.md. Use for scouting, coding, reviewing, or any single-agent task.",
		promptSnippet: "Run an ephemeral agent with a custom persona, model, and tools",
		promptGuidelines: [
			"ALWAYS set workDir so agent output is persisted to disk as a paper trail.",
			"Create a specific persona for each task — don't reuse generic agents.",
			"Include skill file paths in the persona so the agent reads them (e.g., 'Read .pi/skills/coding-in-go/SKILL.md first').",
			"Pick the model via the models() tool first; for scouting/dispatch staffing use recency-aware selections from model-selection (avoid stale generations). If the user has a known provider/model preference, pass it through models() preferences."
		],
		parameters: Type.Object({
			persona: Type.String({ description: "System prompt / persona for the agent" }),
			task: Type.String({ description: "The task to perform" }),
			model: Type.Optional(Type.String({ description: "Model ID, e.g. 'anthropic/claude-opus-4-6'" })),
			tools: Type.Optional(Type.Array(Type.String(), { description: "Tools to enable, e.g. ['read', 'edit', 'bash']" })),
			thinking: Type.Optional(
				StringEnum(["off", "minimal", "low", "medium", "high", "xhigh"] as const, {
					description: "Thinking level for the agent",
				}),
			),
			workDir: Type.Optional(Type.String({ description: "Directory for file-based output" })),
			cwd: Type.Optional(Type.String({ description: "Working directory (defaults to current)" })),
		}),

		async execute(_toolCallId, params, signal, onUpdate, ctx) {
			const startTime = Date.now();
			const dispatchResult = await runAgent({
				persona: params.persona,
				task: params.task,
				model: params.model,
				tools: params.tools,
				thinking: params.thinking,
				workDir: params.workDir,
				cwd: params.cwd ?? ctx.cwd,
				forgeDepth: currentDepth,
				signal: signal,
				onUpdate: onUpdate
					? (text, usage) => {
							onUpdate({
								content: [{ type: "text", text }],
								details: { result: { persona: params.persona, task: params.task, exitCode: 0, messages: [], stderr: "", usage, model: params.model } },
							});
						}
					: undefined,
			});

			const duration = Date.now() - startTime;
			const finalOutput = getFinalOutput(dispatchResult.messages);
			const isError =
				dispatchResult.exitCode !== 0 ||
				dispatchResult.stopReason === "error" ||
				dispatchResult.stopReason === "aborted";

			// Log the operation — find the nearest workDir for the log
			const logDir = params.workDir || (process.env.FORGE_WORKDIR);
			logOperation(logDir, {
				tool: "dispatch",
				timestamp: new Date().toISOString(),
				params: { model: params.model, tools: params.tools, persona: params.persona, task: params.task, workDir: params.workDir, thinking: params.thinking },
				result: isError
					? `FAILED (exit ${dispatchResult.exitCode}): ${dispatchResult.errorMessage || dispatchResult.stderr || "(no details)"}`
					: `OK — ${formatUsage(dispatchResult.usage, dispatchResult.model)}`,
				cost: dispatchResult.usage.cost,
				duration,
			});

			if (isError) {
				const errorMsg = dispatchResult.errorMessage || dispatchResult.stderr || finalOutput || "(no output)";
				throw new Error(`Agent failed: ${errorMsg}`);
			}

			const warnings: string[] = [];
			if (!params.workDir) {
				warnings.push("⚠ No workDir set — agent output not persisted to disk.");
			}

			const outputParts = warnings.length > 0
				? [warnings.join("\n"), "", finalOutput || "(no output)"].join("\n")
				: finalOutput || "(no output)";

			return {
				content: [{ type: "text", text: outputParts }],
				details: { result: dispatchResult } as DispatchDetails,
			};
		},

		renderCall(args, theme) {
			const model = args.model ? theme.fg("muted", ` [${args.model}]`) : "";
			const persona = args.persona?.length > 50 ? `${args.persona.slice(0, 50)}...` : (args.persona || "...");
			const task = args.task?.length > 60 ? `${args.task.slice(0, 60)}...` : (args.task || "...");
			let text = theme.fg("toolTitle", theme.bold("dispatch")) + model;
			text += `\n  ${theme.fg("accent", "persona:")} ${theme.fg("dim", persona)}`;
			text += `\n  ${theme.fg("accent", "task:")} ${theme.fg("dim", task)}`;
			return new Text(text, 0, 0);
		},

		renderResult(result, _options, theme) {
			const details = result.details as DispatchDetails | undefined;
			if (!details?.result) {
				const text = result.content[0];
				return new Text(text?.type === "text" ? text.text : "(no output)", 0, 0);
			}

			const r = details.result;
			const isError = r.exitCode !== 0 || r.stopReason === "error";
			const icon = isError ? theme.fg("error", "✗") : theme.fg("success", "✓");
			const usageStr = formatUsage(r.usage, r.model);

			const finalOutput = getFinalOutput(r.messages);
			const preview = finalOutput.length > 200 ? `${finalOutput.slice(0, 200)}...` : finalOutput;

			let text = `${icon} ${theme.fg("toolTitle", theme.bold("dispatch"))}`;
			if (usageStr) text += ` ${theme.fg("dim", usageStr)}`;
			if (preview) text += `\n${theme.fg("toolOutput", preview)}`;

			return new Text(text, 0, 0);
		},
	});
}
