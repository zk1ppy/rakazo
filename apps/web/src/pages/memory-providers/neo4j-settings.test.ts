import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
import {
  NEO4J_URI_PLACEHOLDER,
  neo4jConnectedHost,
  neo4jConnectionDraft,
  neo4jDreamEnabled,
  neo4jExtractEnabled,
  neo4jUriError,
} from "./neo4j-settings";

describe("Neo4j memory provider settings", () => {
  it("registers Neo4j without a default URI", () => {
    const registry = readFileSync(fileURLToPath(new URL("./registry.ts", import.meta.url)), "utf8");
    expect(registry).toContain('id: "neo4j"');
    expect(registry).toContain("Graph memory stored in Neo4j.");
    expect(registry).not.toContain("useState(NEO4J_URI_PLACEHOLDER)");

    const form = readFileSync(
      fileURLToPath(new URL("./Neo4jSettingsForm.tsx", import.meta.url)),
      "utf8",
    );
    expect(form).toContain('const [uri, setUri] = useState("");');
    expect(form.match(/<Trans>Dream mode<\/Trans>/g)).toEqual(["<Trans>Dream mode</Trans>"]);
    expect(form.indexOf("export function Neo4jExtractionSettings")).toBeLessThan(
      form.indexOf("<Trans>Dream mode</Trans>"),
    );
    expect(form).toContain("placeholder={NEO4J_URI_PLACEHOLDER}");
    expect(form).toContain("required");
    expect(NEO4J_URI_PLACEHOLDER).toBe("bolt://localhost:7687");
  });

  it("requires an explicit URI and leaves URL validity to the backend", () => {
    expect(neo4jUriError("")).toBe("required");
    expect(neo4jUriError("   ")).toBe("required");
    expect(neo4jUriError("not a url")).toBeNull();
    expect(neo4jUriError(NEO4J_URI_PLACEHOLDER)).toBeNull();

    expect(
      neo4jConnectionDraft({ uri: "", database: "", username: "neo4j", password: "secret" }),
    ).toEqual({ ok: false, error: "required" });

    expect(
      neo4jConnectionDraft({
        uri: " bolt://localhost:7687 ",
        database: " ",
        username: " neo4j ",
        password: "secret",
      }),
    ).toEqual({
      ok: true,
      draft: {
        settings: { uri: "bolt://localhost:7687", username: "neo4j" },
        credentials: { password: "secret" },
      },
    });

    expect(
      neo4jConnectionDraft({
        uri: "neo4j+s://example.databases.neo4j.io",
        database: " graph ",
        username: "reader",
        password: " secret ",
      }),
    ).toEqual({
      ok: true,
      draft: {
        settings: {
          uri: "neo4j+s://example.databases.neo4j.io",
          username: "reader",
          database: "graph",
        },
        credentials: { password: " secret " },
      },
    });
  });

  it("shows the host without credentials in the connected label", () => {
    expect(neo4jConnectedHost("")).toBe("Neo4j");
    expect(neo4jConnectedHost("bolt://localhost:7687")).toBe("localhost:7687");
    expect(neo4jConnectedHost("not a url")).toBe("not a url");
  });

  it("persists extraction only when the form sets it", () => {
    expect(neo4jExtractEnabled({})).toBe(true);
    expect(neo4jDreamEnabled({})).toBe(false);
    expect(neo4jDreamEnabled({ dream: "true" })).toBe(true);
    expect(neo4jDreamEnabled({ dream: "false" })).toBe(false);
    expect(neo4jExtractEnabled({ extract: "false" })).toBe(false);
    expect(neo4jExtractEnabled({ extract: "off" })).toBe(false);

    expect(
      neo4jConnectionDraft({
        uri: "bolt://localhost:7687",
        database: "",
        username: "neo4j",
        password: "secret",
        extract: false,
        maxChunkChars: " 500 ",
      }),
    ).toEqual({
      ok: true,
      draft: {
        settings: {
          uri: "bolt://localhost:7687",
          username: "neo4j",
          extract: "false",
          maxChunkChars: "500",
        },
        credentials: { password: "secret" },
      },
    });

    const enabled = neo4jConnectionDraft({
      uri: "bolt://localhost:7687",
      database: "",
      username: "neo4j",
      password: "secret",
      extract: true,
      maxChunkChars: "   ",
    });
    expect(enabled).toEqual({
      ok: true,
      draft: {
        settings: {
          uri: "bolt://localhost:7687",
          username: "neo4j",
          extract: "true",
        },
        credentials: { password: "secret" },
      },
    });
  });

  it("includes an extraction model only when both sides are set", () => {
    expect(
      neo4jConnectionDraft({
        uri: "bolt://localhost:7687",
        database: "",
        username: "neo4j",
        password: "secret",
        extract: true,
        extractModelProvider: " synthetic ",
        extractModelId: " model-a ",
      }),
    ).toEqual({
      ok: true,
      draft: {
        settings: {
          uri: "bolt://localhost:7687",
          username: "neo4j",
          extract: "true",
          extractModelProvider: "synthetic",
          extractModelId: "model-a",
        },
        credentials: { password: "secret" },
      },
    });

    const partial = neo4jConnectionDraft({
      uri: "bolt://localhost:7687",
      database: "",
      username: "neo4j",
      password: "secret",
      extractModelProvider: "synthetic",
    });
    expect(partial.ok && partial.draft.settings.extractModelProvider).toBeUndefined();
  });
});
