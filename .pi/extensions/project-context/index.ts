// project-context — Read Justfile and README at session start, inject into system prompt.
//
// Pi equivalent of the Claude session-start.sh hook.
// No dependencies — uses only Node built-ins (works with both Node and Bun).

import * as fs from "node:fs";
import * as path from "node:path";
import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";

const CONTEXT_FILES = ["Justfile", "README.md"];

export default function (pi: ExtensionAPI) {
  let context = "";

  pi.on("session_start", async (_event, ctx) => {
    const parts: string[] = [];
    for (const file of CONTEXT_FILES) {
      const p = path.join(ctx.cwd, file);
      if (fs.existsSync(p)) {
        parts.push(`## ${file}\n\n${fs.readFileSync(p, "utf-8")}`);
      }
    }
    context = parts.join("\n\n");
  });

  pi.on("before_agent_start", async (event) => {
    if (!context) return;
    return {
      systemPrompt:
        event.systemPrompt +
        "\n\nREQUIRED: Before doing anything else, review the following project files. " +
        "The Justfile is the authoritative reference for all project commands.\n\n" +
        context,
    };
  });
}
