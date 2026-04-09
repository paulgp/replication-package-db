/**
 * ship command — Ship one or more Forgejo issues.
 *
 * Single issue:   /ship 5
 * Multiple:       /ship 1,4,6    /ship 1-9    /ship 1,3,5-8,12
 * Local task:     /ship file:./path/to/task.md    /ship path:./path/to/task.md
 *
 * For multi-issue runs, the extension drives the loop: sends one issue
 * at a time, waits for agent_end, compacts between issues, then sends
 * the next. This prevents the agent from stopping mid-sequence.
 *
 * NOTE: We use an agent_end awaiter instead of ctx.waitForIdle() because
 * pi.sendUserMessage() is fire-and-forget — runningPrompt isn't set until
 * 3+ awaits later inside the prompt chain, so waitForIdle() resolves
 * immediately. Every reference extension (Omega, theredbeard, tallow,
 * proactive-compact) uses agent_end events for this reason.
 */

import { existsSync } from "node:fs";
import type { ExtensionAPI, ExtensionCommandContext } from "@mariozechner/pi-coding-agent";

// ── Issue spec parser ──────────────────────────────────────────────

/**
 * Parse an issue spec like "1,3,5-8,12" into sorted unique issue numbers.
 * Returns empty array on any invalid input.
 */
function parseIssueSpec(spec: string): number[] {
	const trimmed = spec.trim();
	if (!trimmed) return [];

	const issues = new Set<number>();

	for (const token of trimmed.split(",")) {
		const part = token.trim();
		if (!part) return [];

		if (/^\d+$/.test(part)) {
			const n = Number.parseInt(part, 10);
			if (!Number.isSafeInteger(n) || n <= 0) return [];
			issues.add(n);
			continue;
		}

		const rangeMatch = part.match(/^(\d+)\s*-\s*(\d+)$/);
		if (rangeMatch) {
			const start = Number.parseInt(rangeMatch[1], 10);
			const end = Number.parseInt(rangeMatch[2], 10);
			if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start <= 0 || end <= 0) return [];

			const lo = Math.min(start, end);
			const hi = Math.max(start, end);
			if (hi - lo > 200) return []; // sanity cap
			for (let i = lo; i <= hi; i++) issues.add(i);
			continue;
		}

		return [];
	}

	return [...issues].sort((a, b) => a - b);
}

/**
 * Local mode requires explicit `file:` or `path:` prefix.
 */
type ShipTarget =
	| { kind: "issues"; issues: number[] }
	| { kind: "local"; source: string };

function parseShipSpec(spec: string): ShipTarget | null {
	const trimmed = spec.trim();
	if (!trimmed) return null;

	const localMatch = trimmed.match(/^\s*(?:file|path)\s*:\s*(.+)\s*$/i);
	if (localMatch) {
		const source = localMatch[1].trim();
		if (!source) return null;
		return { kind: "local", source };
	}

	const issues = parseIssueSpec(trimmed);
	if (issues.length === 0) return null;
	return { kind: "issues", issues };
}

/** Build a deterministic local workdir from path/slug. */
function localTaskWorkdir(source: string): string {
	const file = source.split(/[\\/]/).filter(Boolean).pop() || "task";
	const stem = file.replace(/\.[^.]+$/, "") || file;
	const slug = stem
		.toLowerCase()
		.replace(/[^a-z0-9]+/g, "-")
		.replace(/-+/g, "-")
		.replace(/^-|-$/g, "") || "task";
	return `/tmp/forge/task-${slug}`;
}

// ── Active run state ───────────────────────────────────────────────

type ShipRun = { id: number; cancelled: boolean };

let activeRun: ShipRun | null = null;
let nextRunId = 0;

const REQUIRED_CONSENSUS_FILES = ["plan-review/consensus.md", "code-review/consensus.md"] as const;

// ── Agent completion awaiter ───────────────────────────────────────
// Replaces ctx.waitForIdle() which races with sendUserMessage().
// Promise created BEFORE sending so agent_end is never missed.
// If sendUserMessage's async chain fails silently (e.g. getApiKey()
// throws inside the fire-and-forget .catch()), agent_end never fires
// and the waiter hangs until a lifecycle event. Acceptable: the error
// surfaces via runner.emitError(), and lifecycle events provide escape.

let resolveAgentDone: (() => void) | null = null;

/** Called from the single agent_end listener. */
function signalAgentEnd(): void {
	if (resolveAgentDone) {
		const r = resolveAgentDone;
		resolveAgentDone = null;
		r();
	}
}

/** Discard a pending waiter without resolving. For cleanup only. */
function clearAgentEndWaiter(): void {
	resolveAgentDone = null;
}

/** Arm a promise that resolves on next agent_end. Call BEFORE sendUserMessage(). */
function nextAgentEnd(): Promise<void> {
	// Fail-open: resolve stale waiter if one exists
	if (resolveAgentDone) {
		resolveAgentDone();
		resolveAgentDone = null;
	}
	return new Promise<void>((resolve) => {
		resolveAgentDone = resolve;
	});
}

// ── Compaction helper ──────────────────────────────────────────────

/** Promise wrapper around ctx.compact() — fail-open on error. */
function compactAndWait(ctx: ExtensionCommandContext, instructions: string): Promise<void> {
	return new Promise((resolve, reject) => {
		ctx.compact({
			customInstructions: instructions,
			onComplete: () => resolve(),
			onError: (err) => reject(err),
		});
	});
}

/** Build compaction instructions summarizing progress. */
function compactionInstructions(completed: number[], remaining: number[]): string {
	return [
		`Shipped issues: ${completed.map((n) => `#${n}`).join(", ")}.`,
		`Remaining: ${remaining.map((n) => `#${n}`).join(", ")}.`,
		"Keep: project structure, patterns used, test commands.",
		"Discard: scout details, debate rounds, specific code changes from completed issues.",
	].join("\n");
}

// ── Prompt builders ────────────────────────────────────────────────

/** Single-issue prompt (mirrors pi/prompts/ship.md). */
function singleIssuePrompt(issue: number): string {
	return [
		`Ship issue #${issue}.`,
		"",
		"First: run `git status --porcelain` — if dirty, stop and tell me.",
		`Then: \`rm -rf /tmp/forge/issue-${issue}/ && mkdir -p /tmp/forge/issue-${issue}/\``,
		`Then: follow the forge /ship workflow strictly. Every dispatch and debate MUST set workDir under \`/tmp/forge/issue-${issue}/\`.`,
		"Required gates: mandatory scout; clarify with user if ambiguous; plan debate to LGTM; code; review debate to LGTM; if review is not consensus LGTM, return to coding.",
		"Do not ship until both files exist: `plan-review/consensus.md` and `code-review/consensus.md` in the issue workDir.",
	].join("\n");
}

/** Prompt for one issue within a multi-issue sequence. */
function sequenceIssuePrompt(issue: number, index: number, total: number, remaining: number[]): string {
	const lines = [
		`Ship issue #${issue} (${index}/${total}).`,
		"",
		"1. Run `git status --porcelain` — if dirty, stop and tell me.",
		`2. \`rm -rf /tmp/forge/issue-${issue}/ && mkdir -p /tmp/forge/issue-${issue}/\``,
		`3. Follow the forge /ship workflow strictly. Every dispatch and debate MUST set workDir under \`/tmp/forge/issue-${issue}/\`.`,
		"4. Required gates: mandatory scout; clarify with user if ambiguous; plan debate to LGTM; code; review debate to LGTM; if review is not consensus LGTM, return to coding.",
		"5. Do not ship until both files exist: `plan-review/consensus.md` and `code-review/consensus.md` in the issue workDir.",
		"",
		"If this issue fails (blocked, tests won't pass, unresolved ambiguity, review loops): hard fail and stop the /ship run.",
	];

	if (remaining.length > 0) {
		lines.push("");
		lines.push(`After this issue, ${remaining.length} more will follow automatically: ${remaining.map((n) => `#${n}`).join(", ")}.`);
	}

	return lines.join("\n");
}

/** Prompt for explicit local-task shipping. */
function localTaskPrompt(source: string): string {
	const workDir = localTaskWorkdir(source);
	return [
		`Ship local task: ${source}.`,
		"",
		"First: run `git status --porcelain` — if dirty, stop and tell me.",
		`Then: read ${source} from disk and treat it as the acceptance/context source.`,
		`Then: \`rm -rf ${workDir} && mkdir -p ${workDir}\``,
		`Then: follow the forge /ship workflow strictly. Every dispatch and debate MUST set workDir under \`${workDir}\`.`,
		"Required gates: mandatory scout; clarify with user if ambiguous; plan debate to LGTM; code; review debate to LGTM; if review is not consensus LGTM, return to coding.",
		"Do not ship until both files exist: `plan-review/consensus.md` and `code-review/consensus.md` in this workDir.",
		"Do not run Forgejo/GitHub issue operations (`fj issue*` / `gh issue*`) in local mode.",
		"If the source is ambiguous, stop and ask one clarifying question before coding.",
	].join("\n");
}

function sendShipPrompt(pi: ExtensionAPI, ctx: ExtensionCommandContext, prompt: string): void {
	if (ctx.isIdle()) {
		pi.sendUserMessage(prompt);
	} else {
		pi.sendUserMessage(prompt, { deliverAs: "followUp" });
	}
}

function missingConsensusFiles(workDir: string): string[] {
	return REQUIRED_CONSENSUS_FILES.filter((relPath) => !existsSync(`${workDir}/${relPath}`));
}

async function enforceConsensusGates(
	pi: ExtensionAPI,
	run: ShipRun,
	workDir: string,
	label: string,
	maxNudges: number = 2,
): Promise<boolean> {
	for (let attempt = 1; attempt <= maxNudges; attempt++) {
		if (run.cancelled) return false;

		const missing = missingConsensusFiles(workDir);
		if (missing.length === 0) return true;

		const prompt = [
			`Quality gate for ${label} not satisfied yet.`,
			"",
			"Do NOT ship yet.",
			`Missing consensus files in ${workDir}: ${missing.map((m) => `\`${m}\``).join(", ")}`,
			"",
			"Continue the strict workflow:",
			"1) if ambiguity remains, ask the user",
			"2) finish plan debate to LGTM",
			"3) code",
			"4) finish review debate to LGTM",
			"5) only then commit/push",
		].join("\n");

		const done = nextAgentEnd();
		pi.sendUserMessage(prompt, { deliverAs: "followUp" });
		await done;
	}

	return missingConsensusFiles(workDir).length === 0;
}

// ── Command registration ───────────────────────────────────────────

export function registerShip(pi: ExtensionAPI) {
	// Event listeners — registered once at load, not per-command.
	pi.on("agent_end", () => signalAgentEnd());

	pi.on("session_shutdown", () => {
		if (activeRun) activeRun.cancelled = true;
		signalAgentEnd(); // unblock teardown — session is dying
	});
	pi.on("session_switch", () => {
		if (activeRun) activeRun.cancelled = true;
		signalAgentEnd(); // unblock teardown — session is switching
	});

	pi.registerCommand("ship", {
		description: "Ship one or more issues, or a local file task (e.g. /ship 5, /ship 1,3-5, file:./task.md)",
		handler: async (args, ctx) => {
			const spec = args.trim();

			// Handle /ship stop
			if (spec === "stop") {
				if (activeRun) {
					activeRun.cancelled = true;
					if (ctx.hasUI) ctx.ui.notify("Ship run cancelled — will stop after current issue.", "warning");
				} else {
					if (ctx.hasUI) ctx.ui.notify("No active ship run to stop.", "info");
				}
				return;
			}

			if (!spec) {
				if (ctx.hasUI)
					ctx.ui.notify(
						"Usage: /ship <issues> (e.g. 5, 1-9, 1,3,5-8,12) or /ship file:./task.md",
						"warning",
					);
				return;
			}

			const target = parseShipSpec(spec);
			if (!target) {
				if (ctx.hasUI)
					ctx.ui.notify(
						`Invalid /ship spec: "${spec}". Use issue numbers/ranges or file: / path: local path.`,
						"error",
					);
				return;
			}

			if (target.kind === "local") {
				if (activeRun && !activeRun.cancelled) {
					if (ctx.hasUI) ctx.ui.notify("A ship run is already active. Use /ship stop first.", "error");
					return;
				}

				const workDir = localTaskWorkdir(target.source);
				const run: ShipRun = { id: nextRunId++, cancelled: false };
				activeRun = run;

				try {
					const done = nextAgentEnd();
					sendShipPrompt(pi, ctx, localTaskPrompt(target.source));
					await done;
					const gatesSatisfied = await enforceConsensusGates(pi, run, workDir, `local task ${target.source}`);
					if (!gatesSatisfied) {
						if (ctx.hasUI) ctx.ui.notify("Consensus gates not satisfied for local task. Hard fail.", "error");
						return;
					}
				} finally {
					clearAgentEndWaiter();
					if (activeRun?.id === run.id) activeRun = null;
				}
				return;
			}

			const issues = target.issues;

			// Reject if a run is already active
			if (activeRun && !activeRun.cancelled) {
				if (ctx.hasUI) ctx.ui.notify("A ship run is already active. Use /ship stop first.", "error");
				return;
			}

			if (issues.length === 1) {
				const issue = issues[0];
				const run: ShipRun = { id: nextRunId++, cancelled: false };
				activeRun = run;

				try {
					const done = nextAgentEnd();
					sendShipPrompt(pi, ctx, singleIssuePrompt(issue));
					await done;
					const gatesSatisfied = await enforceConsensusGates(pi, run, `/tmp/forge/issue-${issue}`, `issue #${issue}`);
					if (!gatesSatisfied) {
						if (ctx.hasUI) ctx.ui.notify(`Consensus gates not satisfied for issue #${issue}. Hard fail.`, "error");
						return;
					}
				} finally {
					clearAgentEndWaiter();
					if (activeRun?.id === run.id) activeRun = null;
				}
				return;
			}

			// Multi-issue: extension drives the loop.
			const total = issues.length;
			const run: ShipRun = { id: nextRunId++, cancelled: false };
			activeRun = run;

			try {
				for (let i = 0; i < issues.length; i++) {
					if (run.cancelled) break;

					const issue = issues[i];
					const prompt = sequenceIssuePrompt(issue, i + 1, total, issues.slice(i + 1));
					const done = nextAgentEnd(); // arm BEFORE send — avoids waitForIdle race
					pi.sendUserMessage(prompt);
					await done; // resolves on agent_end or lifecycle signal

					if (run.cancelled) break;

					const gatesSatisfied = await enforceConsensusGates(pi, run, `/tmp/forge/issue-${issue}`, `issue #${issue}`);
					if (!gatesSatisfied) {
						if (ctx.hasUI) ctx.ui.notify(`Consensus gates not satisfied for issue #${issue}. Hard fail.`, "error");
						return;
					}

					if (run.cancelled || i === issues.length - 1) continue;

					// Compact between issues — fail-open
					try {
						await compactAndWait(
							ctx,
							compactionInstructions(issues.slice(0, i + 1), issues.slice(i + 1)),
						);
					} catch (e) {
						if (ctx.hasUI) ctx.ui.notify(`Compaction failed, continuing: ${e}`, "warning");
					}
				}
			} finally {
				clearAgentEndWaiter(); // prevent hung waiter on unexpected exit
				if (activeRun?.id === run.id) activeRun = null;
			}
		},
	});
}
