import type { SpaceMemoryConfig } from "@rakazo/contracts";
import type { ComponentType } from "react";
import { Neo4jExtractionSettings, Neo4jSettingsForm } from "./Neo4jSettingsForm";
import { neo4jConnectedHost } from "./neo4j-settings";
import { SerenitySettingsForm } from "./SerenitySettingsForm";
import { SupermemorySettingsForm } from "./SupermemorySettingsForm";

export interface MemoryProviderConnectionDraft {
  settings: Record<string, string>;
  credentials: Record<string, string>;
}

export interface MemoryProviderSettingsFormProps {
  busy: boolean;
  onConnect: (draft: MemoryProviderConnectionDraft) => Promise<boolean>;
}

export interface Neo4jExtractionUpdate {
  extract: boolean;
  maxChunkChars?: string;
  extractModelProvider?: string;
  extractModelId?: string;
  consolidateThinkingLevel?: string;
  consolidateMaxOutputTokens?: string;
  dream?: boolean;
}

export interface MemoryProviderConnectedSettingsProps {
  config: SpaceMemoryConfig;
  busy: boolean;
  onUpdateExtraction: (input: Neo4jExtractionUpdate) => Promise<boolean>;
}

export interface MemoryProviderSettingsRegistration {
  id: string;
  name: string;
  description: string;
  SettingsForm: ComponentType<MemoryProviderSettingsFormProps>;
  ConnectedSettings?: ComponentType<MemoryProviderConnectedSettingsProps>;
  connectedLabel: (config: SpaceMemoryConfig) => string;
}

export const MEMORY_PROVIDER_SETTINGS: readonly MemoryProviderSettingsRegistration[] = [
  {
    id: "supermemory",
    name: "Supermemory",
    description: "Add semantic recall alongside native MEMORY.md memory.",
    SettingsForm: SupermemorySettingsForm,
    connectedLabel: (config) =>
      config.settings.mode === "cloud" ? "Supermemory Cloud" : `Local · ${config.settings.baseUrl}`,
  },
  {
    id: "serenity",
    name: "Serenity",
    description: "Hosted or self-hosted Serenity brain for durable memory.",
    SettingsForm: SerenitySettingsForm,
    connectedLabel: (config) => {
      const label = config.settings.brainLabel?.trim();
      const endpoint = config.settings.endpoint ?? "Serenity";
      const mode = config.settings.allowWrites === "true" ? "read/write" : "recall only";
      return label ? `Serenity · ${label} · ${mode}` : `Serenity · ${endpoint} · ${mode}`;
    },
  },
  {
    id: "neo4j",
    name: "Neo4j",
    description: "Graph memory stored in Neo4j.",
    SettingsForm: Neo4jSettingsForm,
    ConnectedSettings: Neo4jExtractionSettings,
    connectedLabel: (config) => {
      const database = config.settings.database?.trim() || "neo4j";
      return `Neo4j · ${database} · ${neo4jConnectedHost(config.settings.uri ?? "")}`;
    },
  },
];

export function memoryProviderSettings(provider: string) {
  return MEMORY_PROVIDER_SETTINGS.find((entry) => entry.id === provider) ?? null;
}

export function defaultMemoryProviderSettings(): MemoryProviderSettingsRegistration {
  const registration = MEMORY_PROVIDER_SETTINGS[0];
  if (!registration) throw new Error("No memory provider settings are registered.");
  return registration;
}
