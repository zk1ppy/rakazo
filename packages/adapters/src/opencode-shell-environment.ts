/** Worker env forwarded into computer `shell` commands when no saved credential owns the name. */
export const OPENCODE_SHELL_VARIABLES = [
  "OPENCODE_SERVER_URL",
  "OPENCODE_PUBLIC_URL",
  "OPENCODE_PASSWORD",
] as const;

const OPENCODE_SHELL_URLS = ["OPENCODE_SERVER_URL", "OPENCODE_PUBLIC_URL"] as const;
const OPENCODE_SHELL_SECRETS = ["OPENCODE_PASSWORD", "OPENCODE_SERVER_PASSWORD"] as const;

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
 * Copy the worker password into the space environment when no saved credential
 * owns the name. Returns the worker password values, including
 * `OPENCODE_SERVER_PASSWORD`, so callers can redact them. URLs are not copied
 * here and are not returned: they must be applied after the secret snapshot.
 */
export function applyOpencodeShellEnvironment(
  agentEnvironment: Record<string, string>,
  botEnvironment: Readonly<Record<string, string>>,
  source: NodeJS.ProcessEnv = process.env,
): string[] {
  const password = source.OPENCODE_PASSWORD?.trim();
  if (
    password &&
    agentEnvironment.OPENCODE_PASSWORD === undefined &&
    botEnvironment.OPENCODE_PASSWORD === undefined
  ) {
    agentEnvironment.OPENCODE_PASSWORD = password;
  }
  const redactions: string[] = [];
  for (const name of OPENCODE_SHELL_SECRETS) {
    const value = source[name]?.trim();
    if (value && !redactions.includes(value)) redactions.push(value);
  }
  return redactions;
}

/**
 * Copy worker OpenCode URLs into the space environment. Call this after
 * `runSecrets` has snapshotted the environment so the URLs stay visible in
 * command output. A saved credential with the same name wins.
 */
export function applyOpencodeShellUrls(
  agentEnvironment: Record<string, string>,
  botEnvironment: Readonly<Record<string, string>>,
  source: NodeJS.ProcessEnv = process.env,
): void {
  for (const name of OPENCODE_SHELL_URLS) {
    const value = source[name]?.trim();
    if (!value) continue;
    if (agentEnvironment[name] === undefined && botEnvironment[name] === undefined) {
      agentEnvironment[name] = value;
    }
  }
}
