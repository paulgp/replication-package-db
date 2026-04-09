/**
 * Forge — Dynamic multi-agent coordination for Pi
 *
 * Three tools:
 *   - models: Discover available models and get role-based recommendations
 *   - dispatch: Run an ephemeral agent with an inline persona
 *   - debate: Run a multi-round debate between agents until convergence
 */

import type { ExtensionAPI } from "@mariozechner/pi-coding-agent";
import { registerModels } from "./models.js";
import { registerDispatch } from "./dispatch.js";
import { registerDebate } from "./debate.js";
import { registerShip } from "./ship.js";

const MAX_FORGE_DEPTH = 2;

export default function (pi: ExtensionAPI) {
	const currentDepth = parseInt(process.env.FORGE_DEPTH || "0", 10);

	if (currentDepth >= MAX_FORGE_DEPTH) {
		// Recursion guard — register tools that return errors
		pi.on("session_start", async (_event, ctx) => {
			if (ctx.hasUI) {
				ctx.ui.notify("Forge: max recursion depth reached, tools disabled", "warning");
			}
		});
		return;
	}

	registerModels(pi);
	registerDispatch(pi, currentDepth);
	registerDebate(pi, currentDepth);
	registerShip(pi);

	pi.on("session_start", async (_event, ctx) => {
		if (ctx.hasUI) {
			ctx.ui.setStatus("forge", `forge (depth ${currentDepth})`);
		}
	});
}
