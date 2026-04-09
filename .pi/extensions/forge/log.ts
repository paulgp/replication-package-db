/**
 * Forge operation log — writes a structured log of every tool call to workDir.
 *
 * Each entry records: timestamp, tool, params summary, result summary, cost.
 * The log file is append-only markdown at `workDir/forge.log.md`.
 */

import * as fs from "node:fs";
import * as path from "node:path";

export interface LogEntry {
	tool: string;
	timestamp: string;
	params: Record<string, unknown>;
	result: string;
	cost?: number;
	duration?: number;
}

/**
 * Append a log entry to the forge log file.
 * If no workDir is provided, logs to stderr as a fallback.
 */
export function logOperation(workDir: string | undefined, entry: LogEntry): void {
	const line = formatEntry(entry);

	if (workDir) {
		const logFile = path.join(workDir, "forge.log.md");
		const header = !fs.existsSync(logFile)
			? `# Forge Operation Log\n\n`
			: "";
		fs.appendFileSync(logFile, header + line + "\n", "utf-8");
	}

	// Always log to stderr for real-time visibility
	process.stderr.write(`[forge] ${entry.tool}: ${entry.result}\n`);
}

function formatEntry(e: LogEntry): string {
	const time = e.timestamp;
	const cost = e.cost != null ? ` | $${e.cost.toFixed(4)}` : "";
	const duration = e.duration != null ? ` | ${(e.duration / 1000).toFixed(1)}s` : "";

	let md = `## ${time} — ${e.tool}${cost}${duration}\n\n`;

	// Summarize params based on tool type
	if (e.tool === "dispatch") {
		const p = e.params;
		md += `- **model**: ${p.model || "(default)"}\n`;
		md += `- **tools**: ${Array.isArray(p.tools) ? (p.tools as string[]).join(", ") : "(default)"}\n`;
		md += `- **persona**: ${truncate(String(p.persona || ""), 200)}\n`;
		md += `- **task**: ${truncate(String(p.task || ""), 200)}\n`;
		if (p.workDir) md += `- **workDir**: ${p.workDir}\n`;
		if (p.thinking) md += `- **thinking**: ${p.thinking}\n`;
	} else if (e.tool === "debate") {
		const p = e.params;
		md += `- **topic**: ${truncate(String(p.topic || ""), 200)}\n`;
		md += `- **topology**: ${p.topology || "round-table"}\n`;
		md += `- **maxRounds**: ${p.maxRounds || 4}\n`;
		if (Array.isArray(p.agents)) {
			for (const a of p.agents as Array<Record<string, unknown>>) {
				md += `- **agent**: ${a.name || a.model || "unnamed"} — ${truncate(String(a.persona || ""), 100)}\n`;
			}
		}
		if (p.workDir) md += `- **workDir**: ${p.workDir}\n`;
	} else if (e.tool === "models") {
		md += `- **role**: ${e.params.role || "any"}\n`;
	}

	md += `\n**Result**: ${e.result}\n\n---\n`;
	return md;
}

function truncate(s: string, max: number): string {
	if (s.length <= max) return s;
	return s.slice(0, max) + "...";
}
