/** Worker env forwarded into computer `shell` commands when no saved credential owns the name. */
export const OPENCODE_SHELL_VARIABLES = [
  "OPENCODE_SERVER_URL",
  "OPENCODE_PUBLIC_URL",
  "OPENCODE_PASSWORD",
] as const;

export function opencodeShellEnvironment(
  source: NodeJS.ProcessEnv = process.env,
): Record<string, string> {
  const environment: Record<string, string> = {};
  for (const name of OPENCODE_SHELL_VARIABLES) {
    const value = source[name]?.trim();
    if (value) environment[name] = value;
  }
  return environment;
}

/**
 * Copy worker OpenCode variables into the space environment used for shell.
 * Names already set by an AgentSecret or a bot command variable are left alone.
 * Returns every worker value so callers can redact them even when a saved
 * credential shadowed the export.
 */
export function applyOpencodeShellEnvironment(
  agentEnvironment: Record<string, string>,
  botEnvironment: Readonly<Record<string, string>>,
  source: NodeJS.ProcessEnv = process.env,
): string[] {
  const worker = opencodeShellEnvironment(source);
  for (const [name, value] of Object.entries(worker)) {
    if (agentEnvironment[name] === undefined && botEnvironment[name] === undefined) {
      agentEnvironment[name] = value;
    }
  }
  return Object.values(worker);
}
