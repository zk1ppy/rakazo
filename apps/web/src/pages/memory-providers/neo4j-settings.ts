/** Example for the empty URI field. Not a connection default. */
export const NEO4J_URI_PLACEHOLDER = "bolt://localhost:7687";

export type Neo4jSettingsFieldError = "required";

/**
 * The URI must be configured. URL validity stays on the backend (`parseNeo4jUri`);
 * the settings overlay shows that error after connect.
 */
export function neo4jUriError(uri: string): Neo4jSettingsFieldError | null {
  if (!uri.trim()) return "required";
  return null;
}

export function neo4jConnectionDraft(input: {
  uri: string;
  database: string;
  username: string;
  password: string;
}):
  | {
      ok: true;
      draft: { settings: Record<string, string>; credentials: Record<string, string> };
    }
  | { ok: false; error: Neo4jSettingsFieldError } {
  const error = neo4jUriError(input.uri);
  if (error) return { ok: false, error };
  const database = input.database.trim();
  const username = input.username.trim();
  return {
    ok: true,
    draft: {
      settings: {
        uri: input.uri.trim(),
        username,
        ...(database ? { database } : {}),
      },
      credentials: { password: input.password },
    },
  };
}

export function neo4jConnectedHost(uri: string): string {
  const trimmed = uri.trim();
  if (!trimmed) return "Neo4j";
  try {
    return new URL(trimmed).host || trimmed;
  } catch {
    return trimmed;
  }
}
