/**
 * Empty / unknown / cursor-without-key becomes none so the API still boots and
 * cloud-agent tools stay uninjected.
 */
export function resolveCloudAgentProvider(source: NodeJS.ProcessEnv = process.env): string {
  const configured = source.CLOUD_AGENT_PROVIDER;
  if (configured !== undefined && !configured.trim()) return "none";
  const requested = (configured?.trim() || "none").toLowerCase();
  if (requested === "none" || requested === "") return "none";
  if (requested === "emulator" || requested === "fake") return "emulator";
  if (requested === "cursor") {
    return optional(source.CURSOR_API_KEY) && optional(source.CLOUD_AGENT_SPACE_ID)
      ? "cursor"
      : "none";
  }
  if (requested === "opencode") {
    return opencodeConfigured(source) ? "opencode" : "none";
  }
  // Soft-fall: do not throw at API boot for typos / future vendor names.
  return "none";
}

/** URL plus HTTP Basic password. A credential stuffed into the URL does not count. */
function opencodeConfigured(source: NodeJS.ProcessEnv) {
  const password = optional(source.OPENCODE_PASSWORD) ?? optional(source.OPENCODE_SERVER_PASSWORD);
  return Boolean(password && opencodeHttpUrl(source.OPENCODE_SERVER_URL));
}

function opencodeHttpUrl(value: string | undefined) {
  const trimmed = optional(value);
  if (!trimmed) return undefined;
  try {
    const url = new URL(trimmed);
    if (url.username || url.password) return undefined;
    if (url.protocol !== "http:" && url.protocol !== "https:") return undefined;
    return trimmed;
  } catch {
    return undefined;
  }
}

function optional(value: string | undefined): string | undefined {
  const trimmed = value?.trim();
  return trimmed || undefined;
}
