import { createHash } from "node:crypto";
import type { CloudAgentProvider } from "@rakazo/adapter-kit";
import { EmulatorCloudAgentProvider } from "./cloud-agent-emulator.js";
import { resolveCloudAgentProvider } from "./cloud-agent-provider-env.js";
import { CursorCloudAgentProvider } from "./cursor-cloud-agent.js";
import { OpenCodeCloudAgentProvider } from "./opencode-cloud-agent.js";

/**
 * The composition root binds a credential to one Space, or to every Space when
 * `allSpaces` is set (self-hosted OpenCode with no Space id, or "*").
 */
export interface CloudAgentConnection {
  provider: CloudAgentProvider;
  /** Opaque credential binding. Rotation fails closed for existing agents. */
  key: string;
  /** Only the offline emulator may be available in every Space without `allSpaces`. */
  spaceId?: string;
  allSpaces?: boolean;
}

export function cloudAgentsEnabled(
  connection: CloudAgentConnection | null | undefined,
  spaceId: string,
) {
  return Boolean(
    connection &&
      (connection.allSpaces ||
        connection.spaceId === spaceId ||
        (!connection.spaceId && connection.provider.describe().capabilities.offline)),
  );
}

export function createCloudAgentConnection(
  source: NodeJS.ProcessEnv = process.env,
): CloudAgentConnection | null {
  const kind = resolveCloudAgentProvider(source);
  if (kind === "emulator") {
    return { provider: new EmulatorCloudAgentProvider(), key: "emulator" };
  }
  if (kind === "opencode") return opencodeConnection(source);
  if (kind !== "cursor") return null;
  const apiKey = source.CURSOR_API_KEY!.trim();
  const spaceId = source.CLOUD_AGENT_SPACE_ID!.trim();
  return {
    provider: new CursorCloudAgentProvider({ apiKey }),
    key: `cursor:${createHash("sha256").update(apiKey).digest("hex")}`,
    spaceId,
  };
}

function opencodeConnection(source: NodeJS.ProcessEnv): CloudAgentConnection | null {
  const serverUrl = source.OPENCODE_SERVER_URL?.trim();
  const password = source.OPENCODE_PASSWORD?.trim() || source.OPENCODE_SERVER_PASSWORD?.trim();
  if (!serverUrl || !password) return null;
  const username = source.OPENCODE_USERNAME?.trim() || "opencode";
  const spaceId = source.CLOUD_AGENT_SPACE_ID?.trim();
  const allSpaces = !spaceId || spaceId === "*";
  return {
    provider: new OpenCodeCloudAgentProvider({
      serverUrl,
      publicUrl: source.OPENCODE_PUBLIC_URL,
      username: source.OPENCODE_USERNAME,
      password,
      workspaceRoot: source.OPENCODE_WORKSPACE_ROOT,
      defaultModel: source.OPENCODE_DEFAULT_MODEL,
    }),
    key: `opencode:${createHash("sha256").update(`${serverUrl}\0${username}\0${password}`).digest("hex")}`,
    ...(allSpaces ? { allSpaces: true } : { spaceId }),
  };
}
